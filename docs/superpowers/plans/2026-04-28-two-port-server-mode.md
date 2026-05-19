# Two-Port Server Mode: Logical-DB-Aware Session Routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every server-mode housegate a valid agent entry point by resolving the logical database to its host proxy at handshake time and on each `USE`, transparently rebinding the session's upstream to the correct peer's internal-port. Collapse the historic forwarding-only mode into server mode.

**Architecture:** Server mode grows a second listener (`internal_listen_addr`) for peer-only traffic; connections there are pre-flagged `IsPeerTrusted=true` and Stripper hard-rejects any `__route__` envelope as an invariant violation. A new `forward` plugin sits before `rewrite` on the external-port chain, looks up `RetrieveDatabaseInfo(hello.Database)` at OnHello and the same on `USE` detection at OnQuery, and rebinds the session to the resolved peer's internal-port via an extended `Session.RebindUpstream`. A new `ForwardAware` marker (opt-out, mirrors `PeerTrustAware`) skips `rewrite`, `commitgate`, `dbrewriter` on forwarded sessions while keeping `auth`, `metrics`, `usage`, `concurrency` running on the entry proxy. Agent drops its fixed `agent_upstream` and reads NetworkState to pick a random permissioned peer (with a bootstrap fallback to any bound peer for new accounts).

**Tech Stack:** Go 1.22+, Bazel 8.5.1 + Bzlmod, ClickHouse native TCP protocol via `ch-go`, secp256k1 JWS, Redis NetworkState (statemirror), stdlib `testing`.

**Spec:** [docs/superpowers/specs/2026-04-28-two-port-server-mode.md](../specs/2026-04-28-two-port-server-mode.md)

---

## Branch & PR Strategy

User preference: never push to `main`; every change is feature-branch + PR. This plan is split into five phases, one PR each, merged sequentially. Each phase begins with `git switch main && git pull && git switch -c <branch>` and ends with `gh pr create`.

| Phase | Branch | Depends on |
|---|---|---|
| 1 | `feat/internal-port-listener` | main |
| 2 | `feat/forward-decision-hello` | Phase 1 merged |
| 3 | `feat/forward-decision-use` | Phase 2 merged |
| 4 | `feat/agent-auto-discover` | Phase 3 merged (Phase 2 also OK) |
| 5 | `chore/remove-forwarding-only-mode` | Phase 4 merged |

Within a phase, every task ends with a commit. Tests must be green at every commit (Bazel: `bazel test //pkg/proxy:proxy_test //pkg/plugins/...:all` minimum).

---

## File Structure

**New files:**
- `pkg/plugins/forward/forward.go` — `Plugin` struct + OnHello/OnQuery hooks
- `pkg/plugins/forward/use_regex.go` — USE statement regex matcher (small, isolated for unit testing)
- `pkg/plugins/forward/forward_test.go` — unit tests for hello + USE flows
- `pkg/plugins/forward/BUILD.bazel`
- `pkg/peer/handshake.go` — `SignPeerHello` helper extracted from rewriter
- `pkg/peer/handshake_test.go`
- `pkg/plugin/forward_aware.go` — `ForwardAware` marker (kept separate from `chain.go` for clarity)

**Modified:**
- `pkg/config/config.go` — add `InternalListen`; remove `ModeForwardingOnly` (Phase 5)
- `pkg/proxy/server.go` — add `PreflagSession` hook on `Server`
- `pkg/chsession/session.go`, `state.go` — add peer-rebind mode to `RebindUpstream`
- `pkg/plugin/chain.go` — `ForwardAware` filter alongside existing `RouteAware`/`PeerTrustAware`
- `build.go` — two listener wiring; insert forward plugin; (Phase 5) delete `buildForwarding`
- `pkg/rewriter/sentio.go` — replace `resolveRemoteCredentials` body with call to `peer.SignPeerHello`
- `pkg/plugins/route/stripper.go` — hard-reject `__route__` when session is on internal-port
- `pkg/plugins/rewrite/`, `commitgate/`, `dbrewriter/` — implement `ForwardAware` opt-out
- `pkg/plugins/agent/` + `build.go::buildAgent` — NetworkState-driven upstream selection
- `CLAUDE.md` — updated mode set + topology diagram (Phase 5)

**Deleted (Phase 5):**
- `Config.Mode() == ModeForwardingOnly` branch
- `buildForwarding`, `pickRandomBoundProxy` in `build.go`

---

# Phase 1 — Internal-port listener

**Goal of phase:** server mode listens on two ports. Internal-port pre-flags `IsPeerTrusted=true` and rejects `__route__`. No routing-decision behavior yet — peer-trust traffic that previously hit external-port now also works on internal-port.

**Branch:** `feat/internal-port-listener`

- [ ] **Phase 1 setup:** create branch

```bash
git switch main && git pull --ff-only && git switch -c feat/internal-port-listener
```

---

## Task 1.1: Add `InternalListen` config field

**Files:**
- Modify: `pkg/config/config.go`
- Test: `pkg/config/config_test.go`

- [ ] **Step 1: Write the failing test for the new field**

In `pkg/config/config_test.go` add:

```go
func TestConfig_Validate_ServerMode_InternalListenOptional(t *testing.T) {
    cfg := minimalServerConfig(t)  // helper that returns a valid server-mode cfg
    cfg.InternalListen = ""
    if err := cfg.Validate(); err != nil {
        t.Fatalf("internal_listen empty must be valid (back-compat): %v", err)
    }

    cfg.InternalListen = "0.0.0.0:9001"
    if err := cfg.Validate(); err != nil {
        t.Fatalf("internal_listen set must be valid: %v", err)
    }

    cfg.InternalListen = "not-a-host-port"
    if err := cfg.Validate(); err == nil {
        t.Fatalf("internal_listen with malformed addr must error")
    }
}
```

If `minimalServerConfig` doesn't exist, add it next to existing test helpers.

- [ ] **Step 2: Run test to verify it fails**

```bash
bazel test //pkg/config:config_test --test_filter=TestConfig_Validate_ServerMode_InternalListenOptional --test_arg=-test.v
```

Expected: FAIL — `cfg.InternalListen` undefined.

- [ ] **Step 3: Add the field + validation**

In `pkg/config/config.go`, in the `Config` struct, alongside `Listen` (line 48):

```go
// InternalListen is the bind address for peer-only traffic (cross-housegate
// __peer__-authenticated handshakes from other server-mode proxies). Empty =
// disabled (back-compat); set to a host:port to enable.
InternalListen string `yaml:"internal_listen" json:"internal_listen"`
```

In `Config.Validate()` (lines 206–246), after the existing Server-mode checks, add:

```go
if c.InternalListen != "" {
    if _, _, err := net.SplitHostPort(c.InternalListen); err != nil {
        errs = append(errs, fmt.Errorf("internal_listen: invalid host:port %q: %w", c.InternalListen, err))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
bazel test //pkg/config:config_test --test_filter=TestConfig_Validate_ServerMode_InternalListenOptional --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Run full config tests**

```bash
bazel test //pkg/config:config_test
```

Expected: PASS (no regression).

- [ ] **Step 6: Commit**

```bash
git add pkg/config/config.go pkg/config/config_test.go
git commit -m "config: add optional internal_listen for peer-only listener

Empty value preserves current behavior (single external listener). Set
to a host:port to enable the second listener wired in a follow-up
commit."
```

---

## Task 1.2: Add `PreflagSession` hook on `Server`

**Files:**
- Modify: `pkg/proxy/server.go`
- Test: `pkg/proxy/server_test.go`

This adds a per-`Server`-instance callback that mutates `SessionState` immediately after session creation, before `OnConnect`/`OnHello` fire. The internal-port `Server` will use this to set `IsPeerTrusted=true` and a new `IsInternalPort=true` flag.

- [ ] **Step 1: Add `IsInternalPort` to SessionState**

In `pkg/chsession/state.go`, in the `SessionState` struct (around line 79 next to `IsPeerTrusted`), add:

```go
// IsInternalPort is true when the connection arrived on the server's
// internal-only listener. Used by Stripper to hard-reject __route__
// envelopes that have no business arriving from a peer.
IsInternalPort bool
```

- [ ] **Step 2: Write the failing test for the preflag hook**

In `pkg/proxy/server_test.go` add:

```go
func TestServer_PreflagSession_RunsBeforeOnHello(t *testing.T) {
    var preflagCalled, helloCalled bool
    var preflagOrder, helloOrder int
    counter := 0

    s := &Server{
        Hooks: testHooks{
            onHello: func(ctx context.Context, sess chsession.Session, _ *chproto.ClientHello) error {
                counter++
                helloOrder = counter
                helloCalled = true
                if !sess.State().IsInternalPort {
                    t.Errorf("preflag did not set IsInternalPort before OnHello")
                }
                return nil
            },
        },
        UpstreamDialer: nopDialer,
        PreflagSession: func(st *chsession.SessionState) {
            counter++
            preflagOrder = counter
            preflagCalled = true
            st.IsInternalPort = true
        },
    }

    runOneClientHello(t, s)  // helper: dial s, send ClientHello, expect ServerHello

    if !preflagCalled || !helloCalled {
        t.Fatalf("preflag=%v hello=%v", preflagCalled, helloCalled)
    }
    if preflagOrder >= helloOrder {
        t.Fatalf("preflag must run before OnHello (preflag=%d hello=%d)", preflagOrder, helloOrder)
    }
}
```

If `testHooks`, `nopDialer`, `runOneClientHello` don't exist, add them in `server_test.go` matching existing test helper style (use `net.Pipe` for the connection, similar to `TestServer_HandshakeRoundTrip`).

- [ ] **Step 3: Run test to verify it fails**

```bash
bazel test //pkg/proxy:proxy_test --test_filter=TestServer_PreflagSession_RunsBeforeOnHello --test_arg=-test.v
```

Expected: FAIL — `Server.PreflagSession` undefined.

- [ ] **Step 4: Add the field + invocation**

In `pkg/proxy/server.go`, in the `Server` struct (lines 24–42), add:

```go
// PreflagSession, if non-nil, is invoked on the freshly-created
// SessionState immediately after the connection is accepted, before
// OnConnect/OnHello run. Intended for listener-level state injection
// (e.g. internal-port pre-flagging IsPeerTrusted + IsInternalPort).
PreflagSession func(*chsession.SessionState)
```

In `handle` (lines 124–154), after `sess := chsession.New(id, c)` (line 126) and before `s.Hooks.OnConnect`, add:

```go
if s.PreflagSession != nil {
    s.PreflagSession(sess.State())
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
bazel test //pkg/proxy:proxy_test --test_filter=TestServer_PreflagSession_RunsBeforeOnHello --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 6: Run full proxy tests**

```bash
bazel test //pkg/proxy:proxy_test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/proxy/server.go pkg/proxy/server_test.go pkg/chsession/state.go
git commit -m "proxy: add PreflagSession hook on Server + IsInternalPort flag

PreflagSession runs once per accepted connection before OnConnect/
OnHello. Internal-port wiring (next commit) uses it to pre-flag
IsPeerTrusted=true and IsInternalPort=true."
```

---

## Task 1.3: Wire two listeners in `buildServer`

**Files:**
- Modify: `build.go`
- Modify: `pkg/proxy/server.go` (or wherever `builtServer` is defined — confirm before editing)
- Test: integration test in `proxy_test.go` if existing patterns allow, otherwise unit test in `build_test.go`

- [ ] **Step 1: Inspect `builtServer` definition**

```bash
grep -n "type builtServer" build.go pkg/proxy/*.go
```

Note the file + line. The struct currently holds `srv *Server`, `preServe func() error`, `teardown func() error`. We need a slice of `(Server, listenAddr)` pairs.

- [ ] **Step 2: Refactor `builtServer` to support multiple servers**

In whichever file owns `builtServer`, change:

```go
type builtServer struct {
    srv      *Server
    preServe func() error
    teardown func() error
}
```

to:

```go
type builtServer struct {
    listeners []serverListener
    preServe  func() error
    teardown  func() error
}

type serverListener struct {
    Server     *proxy.Server
    ListenAddr string
    Label      string  // "external" | "internal", used in metrics + logs
}
```

Update every existing reference (caller iterates `listeners`).

- [ ] **Step 3: Run tests to confirm no regression after refactor**

```bash
bazel test //... 2>&1 | tail -40
```

Expected: PASS (refactor only, behavior unchanged — Server-mode build still produces one listener).

- [ ] **Step 4: Commit refactor**

```bash
git add build.go pkg/proxy/server.go  # plus any caller updates
git commit -m "build: refactor builtServer to support multiple listeners

Pure refactor; current code still builds one listener per mode. Sets
up for the internal-port listener added in the next commit."
```

- [ ] **Step 5: Write the failing test for two-listener wiring**

In `build_test.go` (create if missing):

```go
func TestBuildServer_TwoListenersWhenInternalListenSet(t *testing.T) {
    cfg := minimalServerConfig(t)
    cfg.Listen = "127.0.0.1:0"
    cfg.InternalListen = "127.0.0.1:0"

    bs, err := buildServer(Options{Config: cfg}, newTestRedisFactory(t))
    if err != nil {
        t.Fatalf("buildServer: %v", err)
    }
    defer bs.teardown()

    if got, want := len(bs.listeners), 2; got != want {
        t.Fatalf("listeners: got %d want %d", got, want)
    }

    var external, internal *serverListener
    for i := range bs.listeners {
        switch bs.listeners[i].Label {
        case "external":
            external = &bs.listeners[i]
        case "internal":
            internal = &bs.listeners[i]
        }
    }
    if external == nil || internal == nil {
        t.Fatalf("missing labelled listener: ext=%v int=%v", external, internal)
    }

    if external.Server.PreflagSession != nil {
        t.Errorf("external listener must not preflag")
    }
    if internal.Server.PreflagSession == nil {
        t.Fatalf("internal listener must preflag IsPeerTrusted+IsInternalPort")
    }
    var st chsession.SessionState
    internal.Server.PreflagSession(&st)
    if !st.IsPeerTrusted || !st.IsInternalPort {
        t.Errorf("internal preflag missed flags: peer=%v internal=%v",
            st.IsPeerTrusted, st.IsInternalPort)
    }
}

func TestBuildServer_OneListenerWhenInternalListenEmpty(t *testing.T) {
    cfg := minimalServerConfig(t)
    cfg.Listen = "127.0.0.1:0"
    cfg.InternalListen = ""

    bs, err := buildServer(Options{Config: cfg}, newTestRedisFactory(t))
    if err != nil {
        t.Fatalf("buildServer: %v", err)
    }
    defer bs.teardown()

    if got, want := len(bs.listeners), 1; got != want {
        t.Fatalf("listeners: got %d want %d", got, want)
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

```bash
bazel test //:build_test --test_arg=-test.v 2>&1 | tail -20
```

Expected: FAIL — only one listener built.

- [ ] **Step 7: Implement two-listener wiring in `buildServer`**

In `build.go`, in `buildServer`, where the single `*Server` is currently constructed and added to the result, replace with:

```go
external := &proxy.Server{
    Hooks:           hooks,
    UpstreamDialer:  externalDialer,
    Observer:        obs,
    ShutdownTimeout: cfg.ShutdownTimeout,
}
bs.listeners = append(bs.listeners, serverListener{
    Server:     external,
    ListenAddr: cfg.Listen,
    Label:      "external",
})

if cfg.InternalListen != "" {
    internal := &proxy.Server{
        Hooks:           hooks,  // same chain as external; ForwardAware filter handles the differences
        UpstreamDialer:  externalDialer,  // local CH dialer; internal-port serves locally
        Observer:        obs,
        ShutdownTimeout: cfg.ShutdownTimeout,
        PreflagSession: func(st *chsession.SessionState) {
            st.IsPeerTrusted = true
            st.IsInternalPort = true
        },
    }
    bs.listeners = append(bs.listeners, serverListener{
        Server:     internal,
        ListenAddr: cfg.InternalListen,
        Label:      "internal",
    })
}
```

- [ ] **Step 8: Run test to verify it passes**

```bash
bazel test //:build_test --test_filter='TestBuildServer_(Two|One)Listener' --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 9: Wire both listeners in the caller (`Run` / `RunWith`)**

Find the caller that does `srv.Serve(ctx, ln)`. Replace with a loop over `bs.listeners` that starts each in its own goroutine and aggregates errors via `errgroup`. Use this pattern:

```go
g, gctx := errgroup.WithContext(ctx)
for _, sl := range bs.listeners {
    sl := sl
    ln, err := net.Listen("tcp", sl.ListenAddr)
    if err != nil {
        return fmt.Errorf("listen %s (%s): %w", sl.Label, sl.ListenAddr, err)
    }
    log.Infow("listener up", "label", sl.Label, "addr", ln.Addr().String())
    g.Go(func() error { return sl.Server.Serve(gctx, ln) })
}
return g.Wait()
```

- [ ] **Step 10: Run full proxy tests**

```bash
bazel test //pkg/proxy:proxy_test //:build_test
```

Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add build.go proxy.go cmd/main.go build_test.go
git commit -m "build: wire optional internal-port listener in server mode

When internal_listen is set, buildServer constructs a second Server
with PreflagSession that flags IsPeerTrusted=true + IsInternalPort=true.
Both listeners share the same plugin chain; ForwardAware/PeerTrustAware
markers handle per-listener behavior differences."
```

---

## Task 1.4: Stripper hard-rejects `__route__` on internal-port

**Files:**
- Modify: `pkg/plugins/route/stripper.go`
- Test: `pkg/plugins/route/stripper_test.go`

- [ ] **Step 1: Write the failing test**

In `pkg/plugins/route/stripper_test.go` add:

```go
func TestStripper_OnHello_RejectsRouteOnInternalPort(t *testing.T) {
    s := &Stripper{}
    sess := newTestSession(t)
    sess.State().IsInternalPort = true

    hello := &chproto.ClientHello{User: route.FormatRouteUser("peer:9001", "alice")}
    err := s.OnHello(context.Background(), sess, hello)
    if err == nil {
        t.Fatalf("expected error on __route__ over internal-port, got nil")
    }
    if !strings.Contains(err.Error(), "route envelope on internal-port") {
        t.Fatalf("error message should explain the rejection: %v", err)
    }
}

func TestStripper_OnHello_AllowsNonRouteOnInternalPort(t *testing.T) {
    s := &Stripper{}
    sess := newTestSession(t)
    sess.State().IsInternalPort = true

    hello := &chproto.ClientHello{User: peer.FormatUser("peer:9001")}
    if err := s.OnHello(context.Background(), sess, hello); err != nil {
        t.Fatalf("__peer__ on internal-port must pass through: %v", err)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
bazel test //pkg/plugins/route:route_test --test_filter='TestStripper_OnHello_(Rejects|Allows)' --test_arg=-test.v
```

Expected: FAIL — Stripper accepts `__route__` regardless of port.

- [ ] **Step 3: Implement the rejection**

In `pkg/plugins/route/stripper.go`, in `OnHello` (lines 28–54), add at the top after parsing:

```go
target, realUser, isRoute := route.ParseRouteFromUser(hello.User)
if isRoute && sess.State().IsInternalPort {
    return fmt.Errorf("route envelope on internal-port (target=%q user=%q): "+
        "internal-port must never receive __route__; check peer config or firewall",
        target, realUser)
}
if !isRoute {
    return nil
}
// existing strip logic continues unchanged below ...
```

- [ ] **Step 4: Run test to verify it passes**

```bash
bazel test //pkg/plugins/route:route_test --test_filter='TestStripper_OnHello_(Rejects|Allows)' --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Run full route tests**

```bash
bazel test //pkg/plugins/route:route_test
```

Expected: PASS (existing tests unaffected since they default `IsInternalPort=false`).

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/route/stripper.go pkg/plugins/route/stripper_test.go
git commit -m "route: hard-reject __route__ envelope on internal-port

Internal-port receives only peer-trusted handshakes; a __route__
envelope arriving there means a peer is asking us to forward further,
which would break the 'CH only talks to its own proxy' invariant and
risk loops. Fail closed."
```

---

## Task 1.5: Phase 1 integration test — peer-trust traffic on internal-port

**Files:**
- Test: `pkg/proxy/internal_port_integration_test.go` (new)

This task confirms a peer-style handshake (`__peer__|<addr>` envelope + relay JWS) succeeds on the internal-port. Reuses the existing peer-trust test fixtures.

- [ ] **Step 1: Write the integration test**

```go
func TestInternalPort_AcceptsPeerTrustedHandshake(t *testing.T) {
    cfg := testutil.MinimalServerConfigWithRelayKey(t)  // has RelayPrivateKeyHex
    cfg.Listen = "127.0.0.1:0"
    cfg.InternalListen = "127.0.0.1:0"

    bs, err := buildServer(Options{Config: cfg}, newTestRedisFactory(t))
    if err != nil {
        t.Fatalf("buildServer: %v", err)
    }
    defer bs.teardown()

    var internal *serverListener
    for i := range bs.listeners {
        if bs.listeners[i].Label == "internal" {
            internal = &bs.listeners[i]
        }
    }
    require.NotNil(t, internal)

    ln := startListener(t, internal)
    defer ln.Close()

    // Construct a peer hello: user = __peer__|<self>, password = relay JWS
    peerSigner := authtest.NewRelaySigner(t, cfg.RelayPrivateKeyHex)
    audience := strconv.FormatUint(cfg.IndexerID, 10)
    token, err := peerSigner.SignPeerLogin(audience, 30*time.Second)
    require.NoError(t, err)

    conn, err := net.Dial("tcp", ln.Addr().String())
    require.NoError(t, err)
    defer conn.Close()

    sendHello(t, conn, peer.FormatUser(peerSigner.Address()), token, "default")
    expectServerHello(t, conn)
}
```

- [ ] **Step 2: Run the test**

```bash
bazel test //pkg/proxy:proxy_test --test_filter=TestInternalPort_AcceptsPeerTrustedHandshake --test_arg=-test.v
```

Expected: PASS (Phase 1 wiring already supports this).

If this fails, the failure tells us peer-trust handshake doesn't compose with the internal-port pre-flag — debug before proceeding.

- [ ] **Step 3: Commit**

```bash
git add pkg/proxy/internal_port_integration_test.go
git commit -m "proxy: integration test for peer-trust handshake on internal-port"
```

---

## Phase 1 — Wrap-up

- [ ] **Run full test suite**

```bash
bazel test //...
```

Expected: PASS (or matching baseline; CLAUDE.md notes some external-service tests are pre-existing skips).

- [ ] **Push and open PR**

```bash
git push -u origin feat/internal-port-listener
gh pr create --title "feat: internal-port listener for peer-only traffic" --body "$(cat <<'EOF'
## Summary
- Add optional \`internal_listen\` config field; when set, server mode runs a second listener
- Internal listener pre-flags sessions \`IsPeerTrusted=true\` + \`IsInternalPort=true\`
- Stripper hard-rejects \`__route__\` envelope on internal-port (invariant: CH only talks to local proxy)

This is Phase 1 of the two-port server-mode design — sets up infrastructure with no behavior change for existing deployments. Subsequent phases add the forward-decision plugin (Phase 2), USE rebind (Phase 3), and agent auto-discovery (Phase 4).

Spec: docs/superpowers/specs/2026-04-28-two-port-server-mode.md

## Test plan
- [x] config Validate accepts both empty and host:port for internal_listen
- [x] buildServer constructs two listeners only when internal_listen is set
- [x] internal listener pre-flags IsPeerTrusted + IsInternalPort
- [x] Stripper rejects __route__ on internal-port
- [x] Peer-trust handshake works on internal-port
- [x] \`bazel test //...\` green

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for review + merge before starting Phase 2.

---

# Phase 2 — Forward-decision plugin (OnHello) + RebindUpstream-to-peer

**Goal of phase:** agent can connect to any server proxy and have its session pivoted at handshake time to whichever peer hosts `hello.Database`. Cross-DB SQL still falls back to the rewriter's `remote()` path. USE rebind comes in Phase 3.

**Branch:** `feat/forward-decision-hello`

- [ ] **Phase 2 setup:**

```bash
git switch main && git pull --ff-only && git switch -c feat/forward-decision-hello
```

---

## Task 2.1: Extract `SignPeerHello` helper into `pkg/peer`

**Files:**
- Create: `pkg/peer/handshake.go`
- Create: `pkg/peer/handshake_test.go`
- Modify: `pkg/rewriter/sentio.go` (replace body of `resolveRemoteCredentials`)
- Modify: `pkg/peer/BUILD.bazel`

- [ ] **Step 1: Write the failing test**

`pkg/peer/handshake_test.go`:

```go
package peer

import (
    "testing"
    "time"

    "github.com/sentioxyz/housegate/pkg/auth"
    "github.com/sentioxyz/housegate/pkg/network"
)

func TestSignPeerHello_WithSigner_BuildsPeerEnvelope(t *testing.T) {
    signer := authtest.NewRelaySigner(t)
    target := network.IndexerInfo{IndexerId: 42, IndexerUrl: "x", ClickhouseProxyPort: 9001}

    user, password, err := SignPeerHello(signer, 30*time.Second, target, nil)
    if err != nil {
        t.Fatalf("SignPeerHello: %v", err)
    }
    if want := FormatUser(signer.Address()); user != want {
        t.Errorf("user = %q want %q", user, want)
    }

    validator := authtest.NewEthValidator(t, signer.Address())
    if _, err := validator.ValidatePeerLogin(password, "42"); err != nil {
        t.Errorf("validator rejected token meant for indexer 42: %v", err)
    }
}

func TestSignPeerHello_NoSigner_FallsBackToStaticCreds(t *testing.T) {
    target := network.IndexerInfo{IndexerId: 42}
    fallback := credentialstest.NewStaticProvider(t, "u-42", "p-42")

    user, password, err := SignPeerHello(nil, 0, target, fallback)
    if err != nil {
        t.Fatalf("SignPeerHello: %v", err)
    }
    if user != "u-42" || password != "p-42" {
        t.Errorf("expected static creds, got user=%q password=%q", user, password)
    }
}

func TestSignPeerHello_NoSignerNoFallback_Errors(t *testing.T) {
    target := network.IndexerInfo{IndexerId: 42}
    _, _, err := SignPeerHello(nil, 0, target, nil)
    if err == nil {
        t.Fatalf("expected error when both signer and fallback are nil")
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
bazel test //pkg/peer:peer_test --test_arg=-test.v 2>&1 | tail -15
```

Expected: FAIL — `SignPeerHello` undefined.

- [ ] **Step 3: Add the helper**

`pkg/peer/handshake.go`:

```go
package peer

import (
    "fmt"
    "strconv"
    "time"

    "github.com/sentioxyz/housegate/pkg/auth"
    "github.com/sentioxyz/housegate/pkg/credentials"
    "github.com/sentioxyz/housegate/pkg/network"
)

// SignPeerHello builds the (user, password) pair for a TCP handshake from one
// housegate to another's internal-port. Prefer signer-based peer-relay JWS;
// fall back to static credentials when signer is nil.
func SignPeerHello(
    signer auth.PeerSigner,
    ttl time.Duration,
    target network.IndexerInfo,
    fallback credentials.CredentialProvider,
) (user, password string, err error) {
    if signer != nil {
        audience := strconv.FormatUint(uint64(target.IndexerId), 10)
        token, signErr := signer.SignPeerLogin(audience, ttl)
        if signErr != nil {
            return "", "", fmt.Errorf("sign peer login (audience=%s): %w", audience, signErr)
        }
        return FormatUser(signer.Address()), token, nil
    }
    if fallback == nil {
        return "", "", fmt.Errorf("no peer signer and no static credential provider")
    }
    cred, ok := fallback.GetCredentialForIndexer(target)
    if !ok {
        return "", "", fmt.Errorf("no static credential for indexer %d", target.IndexerId)
    }
    return cred.User, cred.Password, nil
}
```

- [ ] **Step 4: Update `BUILD.bazel`**

```bash
bazel run //:gazelle
```

- [ ] **Step 5: Run test to verify it passes**

```bash
bazel test //pkg/peer:peer_test --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 6: Replace `resolveRemoteCredentials` body to call the helper**

In `pkg/rewriter/sentio.go` lines 482–517, replace the function body:

```go
func (f *SentioNetworkFactory) resolveRemoteCredentials(peerIndexerInfo network.IndexerInfo) (user, password string) {
    ttl := f.peerTokenTTL
    if ttl == 0 {
        ttl = peerLoginTokenTTL
    }
    user, password, err := peer.SignPeerHello(f.peerSigner, ttl, peerIndexerInfo, f.credProvider)
    if err != nil {
        log.Warnw("resolve remote credentials failed; remote() will likely fail downstream",
            "indexer", peerIndexerInfo.IndexerId, "err", err)
        return "", ""
    }
    return user, password
}
```

- [ ] **Step 7: Run rewriter tests**

```bash
bazel test //pkg/rewriter:rewriter_test
```

Expected: PASS (behavior unchanged).

- [ ] **Step 8: Commit**

```bash
git add pkg/peer/handshake.go pkg/peer/handshake_test.go pkg/peer/BUILD.bazel pkg/rewriter/sentio.go
git commit -m "peer: extract SignPeerHello from rewriter for shared use

Pulls the static-vs-JWS credential-resolution logic out of
pkg/rewriter/sentio.go's resolveRemoteCredentials into a public
helper so the upcoming forward plugin can call it during peer
rebind. Behavior unchanged."
```

---

## Task 2.2: Add `ForwardAware` marker + chain filter

**Files:**
- Create: `pkg/plugin/forward_aware.go`
- Modify: `pkg/plugin/chain.go`
- Modify: `pkg/chsession/state.go`
- Test: `pkg/plugin/chain_test.go`

- [ ] **Step 1: Add `IsForwarding` flag on `SessionState`**

In `pkg/chsession/state.go`, in `SessionState` struct (around line 79), add:

```go
// IsForwarding is set by the forward plugin when the entire session is
// being transparently forwarded to a peer's internal-port. ForwardAware
// plugins use this to skip themselves; opt-in plugins (auth, metrics,
// usage, concurrency) keep running on the entry proxy.
IsForwarding bool
```

- [ ] **Step 2: Add the marker interface**

`pkg/plugin/forward_aware.go`:

```go
package plugin

// ForwardAware lets a plugin opt out of running on a session that the
// forward-decision plugin has marked as a transparent forward to a peer's
// internal-port. Default (no implementation, or RunOnForward()==true) =
// the plugin runs.
//
// Mirror of PeerTrustAware. Used for plugins like rewrite / commitgate /
// dbrewriter whose work belongs on the host proxy, not the entry proxy.
type ForwardAware interface {
    RunOnForward() bool
}
```

- [ ] **Step 3: Write the failing test for the chain filter**

In `pkg/plugin/chain_test.go` add:

```go
func TestPluginChain_SkipsForwardOptOutWhenIsForwarding(t *testing.T) {
    var ranForwardAware, ranForwardOptOut, ranNeutral bool

    chain := PluginChain{
        QueryPlugins: []QueryPlugin{
            forwardAwareTrue{onQuery: func() { ranForwardAware = true }},     // RunOnForward=true → runs
            forwardAwareFalse{onQuery: func() { ranForwardOptOut = true }},   // RunOnForward=false → skipped
            neutralPlugin{onQuery: func() { ranNeutral = true }},             // no marker → runs (default)
        },
    }

    sess := newTestSession(t)
    sess.State().IsForwarding = true

    if err := chain.OnQuery(context.Background(), &QueryContext{Session: sess}); err != nil {
        t.Fatalf("OnQuery: %v", err)
    }

    if !ranForwardAware {
        t.Errorf("plugin with RunOnForward()=true should run")
    }
    if ranForwardOptOut {
        t.Errorf("plugin with RunOnForward()=false must be skipped on IsForwarding session")
    }
    if !ranNeutral {
        t.Errorf("plugin without ForwardAware should run by default")
    }
}

type forwardAwareTrue  struct{ onQuery func() }
type forwardAwareFalse struct{ onQuery func() }
type neutralPlugin     struct{ onQuery func() }
func (p forwardAwareTrue) Name() string  { return "fa-true" }
func (p forwardAwareTrue) RunOnForward() bool { return true }
func (p forwardAwareTrue) OnQuery(_ context.Context, _ *QueryContext) error { p.onQuery(); return nil }
func (p forwardAwareFalse) Name() string { return "fa-false" }
func (p forwardAwareFalse) RunOnForward() bool { return false }
func (p forwardAwareFalse) OnQuery(_ context.Context, _ *QueryContext) error { p.onQuery(); return nil }
func (p neutralPlugin) Name() string { return "neutral" }
func (p neutralPlugin) OnQuery(_ context.Context, _ *QueryContext) error { p.onQuery(); return nil }
```

- [ ] **Step 4: Run test to verify it fails**

```bash
bazel test //pkg/plugin:plugin_test --test_filter=TestPluginChain_SkipsForwardOptOut --test_arg=-test.v
```

Expected: FAIL — chain doesn't filter on `IsForwarding`.

- [ ] **Step 5: Add the filter helper next to existing `runsOnRouted`/`runsOnPeerTrust`**

In `pkg/plugin/chain.go` (lines 49–66), add:

```go
// runsOnForward returns whether p should run when SessionState.IsForwarding
// is true. Default (no marker) = true. Implementing ForwardAware lets a
// plugin opt out by returning false.
func runsOnForward(p any) bool {
    fa, ok := p.(ForwardAware)
    if !ok {
        return true
    }
    return fa.RunOnForward()
}
```

In every `chain.OnXxx` method that loops plugins and applies the existing route/peer-trust filters (lines 92, 108, 123, 141), add a `&& !(sess.State().IsForwarding && !runsOnForward(p))` guard. Alternatively factor a single `shouldRun(p, st *SessionState)` helper:

```go
func shouldRun(p any, st *chsession.SessionState) bool {
    if st.IsRouted() && !runsOnRouted(p) {
        return false
    }
    if st.IsPeerTrusted && !runsOnPeerTrust(p) {
        return false
    }
    if st.IsForwarding && !runsOnForward(p) {
        return false
    }
    return true
}
```

and call `if !shouldRun(p, sess.State()) { continue }` inside each loop.

- [ ] **Step 6: Run test to verify it passes**

```bash
bazel test //pkg/plugin:plugin_test --test_filter=TestPluginChain_SkipsForwardOptOut --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 7: Run full plugin tests**

```bash
bazel test //pkg/plugin:plugin_test //pkg/plugins/...
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/plugin/forward_aware.go pkg/plugin/chain.go pkg/plugin/chain_test.go pkg/chsession/state.go
git commit -m "plugin: add ForwardAware marker + IsForwarding session flag

Mirrors PeerTrustAware (opt-out): plugins that should not run on a
transparently forwarded session implement RunOnForward()==false. Default
(no marker) = run."
```

---

## Task 2.3: Mark `rewrite`, `commitgate`, `dbrewriter` as `ForwardAware` opt-out

**Files:**
- Modify: `pkg/plugins/rewrite/rewrite.go` (or wherever `Plugin` lives)
- Modify: `pkg/plugins/commitgate/commitgate.go`
- Modify: `pkg/plugins/dbrewriter/dbrewriter.go`
- Tests: matching `*_test.go` for each

For each of the three plugins, repeat the same pattern. Showing it once for `rewrite`:

- [ ] **Step 1: Write the failing test (rewrite)**

In `pkg/plugins/rewrite/rewrite_test.go` add:

```go
func TestRewritePlugin_RunOnForward_False(t *testing.T) {
    var p plugin.ForwardAware = (*Plugin)(nil)
    if p.RunOnForward() {
        t.Errorf("rewrite must opt out of forwarded sessions")
    }
}
```

- [ ] **Step 2: Run test to verify it fails (or doesn't compile)**

```bash
bazel test //pkg/plugins/rewrite:rewrite_test --test_filter=TestRewritePlugin_RunOnForward --test_arg=-test.v
```

Expected: FAIL — `*Plugin` doesn't implement `ForwardAware`.

- [ ] **Step 3: Implement the marker**

Add to `pkg/plugins/rewrite/rewrite.go`:

```go
// RunOnForward implements plugin.ForwardAware. Returns false — when the
// session is transparently forwarded to a peer, the rewrite belongs to
// the host proxy that actually runs the SQL.
func (*Plugin) RunOnForward() bool { return false }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
bazel test //pkg/plugins/rewrite:rewrite_test --test_filter=TestRewritePlugin_RunOnForward --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Repeat the same 3 steps for `commitgate`**

In `pkg/plugins/commitgate/commitgate_test.go`:

```go
func TestCommitgatePlugin_RunOnForward_False(t *testing.T) {
    var p plugin.ForwardAware = (*Plugin)(nil)
    if p.RunOnForward() {
        t.Errorf("commitgate must opt out of forwarded sessions; DDL gating belongs on the host proxy")
    }
}
```

Run, verify fail, add `func (*Plugin) RunOnForward() bool { return false }` to `commitgate.go`, run, verify pass.

- [ ] **Step 6: Repeat for `dbrewriter`**

Same pattern in `pkg/plugins/dbrewriter/dbrewriter_test.go` and add the method to `dbrewriter.go`.

- [ ] **Step 7: Run all three plugin tests**

```bash
bazel test //pkg/plugins/rewrite:rewrite_test //pkg/plugins/commitgate:commitgate_test //pkg/plugins/dbrewriter:dbrewriter_test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/plugins/rewrite/ pkg/plugins/commitgate/ pkg/plugins/dbrewriter/
git commit -m "plugins: mark rewrite/commitgate/dbrewriter as ForwardAware opt-out

When the forward plugin marks a session as IsForwarding=true, these
three plugins skip themselves — their work belongs on the host proxy,
not the entry proxy."
```

---

## Task 2.4: Add peer-rebind mode to `Session.RebindUpstream`

**Files:**
- Modify: `pkg/chsession/session.go`
- Modify: `pkg/chsession/state.go` (Replay should still work)
- Test: `pkg/chsession/session_test.go`

The current `RebindUpstream(ctx, newUp *chproto.Codec, replayState bool) error` accepts an already-constructed upstream codec. We extend the contract so the caller can ask for a peer-mode handshake on the new connection. Since the dial itself is the caller's responsibility, "peer mode" really means "after binding, send a peer hello on the new codec, then replay." The concrete change is a new method `RebindToPeer(ctx, newUp, peerHello)` that bundles the peer-hello-injection + Replay.

- [ ] **Step 1: Write the failing test**

`pkg/chsession/session_test.go`:

```go
func TestSession_RebindToPeer_SendsPeerHelloThenReplaysState(t *testing.T) {
    sess, oldUp, _ := newTestSessionWithUpstream(t)

    // populate state to be replayed
    sess.State().Database = "tenant1"
    sess.State().Settings = map[string]chproto.Setting{"max_block_size": stringSetting("1000")}

    newUpServer, newUpCodec := newPipedCodec(t)

    var receivedHello *chproto.ClientHello
    var replayCalls []string
    go func() {
        receivedHello = readClientHello(newUpServer)
        respondServerHello(newUpServer)
        for {
            pkt, err := newUpServer.ReadPacket()
            if err != nil { return }
            if q, ok := pkt.Body.(*chproto.ClientQuery); ok {
                replayCalls = append(replayCalls, q.Body)
                respondEndOfStream(newUpServer)
            }
        }
    }()

    err := sess.RebindToPeer(context.Background(), newUpCodec, &chproto.ClientHello{
        User:     "__peer__|self:9000",
        Password: "fake-jws",
        Database: "default",
    })
    if err != nil {
        t.Fatalf("RebindToPeer: %v", err)
    }

    // Old upstream still alive in struct but no longer the bound upstream
    if sess.Upstream() != newUpCodec {
        t.Errorf("session did not swap upstream")
    }
    if receivedHello == nil || receivedHello.User != "__peer__|self:9000" {
        t.Errorf("peer hello not sent on new upstream: %+v", receivedHello)
    }
    found := false
    for _, q := range replayCalls {
        if strings.Contains(q, "USE tenant1") { found = true }
    }
    if !found {
        t.Errorf("replay did not re-issue USE tenant1; got %v", replayCalls)
    }

    _ = oldUp  // silence unused
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
bazel test //pkg/chsession:chsession_test --test_filter=TestSession_RebindToPeer --test_arg=-test.v
```

Expected: FAIL — `RebindToPeer` undefined.

- [ ] **Step 3: Implement `RebindToPeer`**

In `pkg/chsession/session.go`, add to the `Session` interface (lines 20–30):

```go
// RebindToPeer atomically swaps the bound upstream to newUp, sends
// peerHello on the new codec, awaits the upstream's ServerHello,
// and replays Database+Settings via SessionState.Replay. Used by the
// forward plugin to pivot a session onto a peer's internal-port.
RebindToPeer(ctx context.Context, newUp *chproto.Codec, peerHello *chproto.ClientHello) error
```

In `*sessionImpl`, implement:

```go
func (s *sessionImpl) RebindToPeer(ctx context.Context, newUp *chproto.Codec, peerHello *chproto.ClientHello) error {
    if err := newUp.WriteClientHello(ctx, peerHello); err != nil {
        return fmt.Errorf("write peer hello: %w", err)
    }
    serverHello, err := newUp.ReadServerHello(ctx)
    if err != nil {
        return fmt.Errorf("read peer server-hello: %w", err)
    }
    _ = serverHello  // trust upstream; revision/timezone irrelevant for replay
    if err := newUp.SendAddendum(ctx, &chproto.Addendum{ProposedRecv: "notchunked", ProposedSend: "notchunked"}); err != nil {
        return fmt.Errorf("send peer addendum: %w", err)
    }
    s.up.Store(newUp)
    return s.state.Replay(ctx, newUp)
}
```

(Confirm Codec method names by grepping `pkg/chproto/codec.go`; adjust if `WriteClientHello`/`ReadServerHello`/`SendAddendum` are spelled differently.)

- [ ] **Step 4: Run test to verify it passes**

```bash
bazel test //pkg/chsession:chsession_test --test_filter=TestSession_RebindToPeer --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Run full chsession tests**

```bash
bazel test //pkg/chsession:chsession_test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/chsession/session.go pkg/chsession/session_test.go
git commit -m "chsession: add RebindToPeer to pivot session onto peer internal-port

Bundles: write peer ClientHello on new codec, read its ServerHello,
swap upstream pointer, replay USE+SET state. Used by the forward
plugin (next commit) to transparently re-route an already-handshaken
session to a different server proxy."
```

---

## Task 2.5: Create `forward` plugin with OnHello logic

**Files:**
- Create: `pkg/plugins/forward/forward.go`
- Create: `pkg/plugins/forward/use_regex.go`
- Create: `pkg/plugins/forward/forward_test.go`
- Create: `pkg/plugins/forward/BUILD.bazel`

We add `use_regex.go` as a stub now (Phase 3 fills it in) so the BUILD.bazel doesn't need touching mid-Phase-3.

- [ ] **Step 1: Write the failing test for OnHello (local DB)**

`pkg/plugins/forward/forward_test.go`:

```go
package forward

import (
    "context"
    "net"
    "testing"
    "time"

    "github.com/sentioxyz/housegate/pkg/auth/authtest"
    "github.com/sentioxyz/housegate/pkg/chproto"
    "github.com/sentioxyz/housegate/pkg/chsession"
    "github.com/sentioxyz/housegate/pkg/network"
)

func TestForwardPlugin_OnHello_LocalDatabase_NoForward(t *testing.T) {
    p := newTestForwardPlugin(t, fakeNetworkState{
        databases: map[string]network.DatabaseInfo{
            "tenant1": {IndexerId: 100},  // local indexer
        },
        indexers: map[uint64]network.IndexerInfo{
            100: {IndexerId: 100, IndexerUrl: "self", ClickhouseProxyPort: 9000},
        },
        selfIndexerID: 100,
    })

    sess := newTestSession(t)
    hello := &chproto.ClientHello{Database: "tenant1"}

    if err := p.OnHello(context.Background(), sess, hello); err != nil {
        t.Fatalf("OnHello: %v", err)
    }
    if sess.State().IsForwarding {
        t.Errorf("local DB must not set IsForwarding")
    }
    if sess.State().RouteTarget != "" {
        t.Errorf("local DB must leave RouteTarget empty, got %q", sess.State().RouteTarget)
    }
}
```

- [ ] **Step 2: Write the failing test for OnHello (remote DB)**

```go
func TestForwardPlugin_OnHello_RemoteDatabase_TriggersRebind(t *testing.T) {
    var rebindCalled bool
    var rebindHello *chproto.ClientHello

    p := newTestForwardPlugin(t, fakeNetworkState{
        databases: map[string]network.DatabaseInfo{
            "tenant2": {IndexerId: 200},
        },
        indexers: map[uint64]network.IndexerInfo{
            200: {IndexerId: 200, IndexerUrl: "peer.internal", ClickhouseProxyPort: 9001},
        },
        selfIndexerID: 100,
    })
    p.dialPeer = func(ctx context.Context, addr string) (*chproto.Codec, error) {
        if addr != "peer.internal:9001" {
            t.Fatalf("dial addr = %q want peer.internal:9001", addr)
        }
        _, codec := newPipedCodec(t)
        return codec, nil
    }
    p.rebindFn = func(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error {
        rebindCalled = true
        rebindHello = hello
        return nil
    }

    sess := newTestSession(t)
    hello := &chproto.ClientHello{Database: "tenant2", User: "alice"}

    if err := p.OnHello(context.Background(), sess, hello); err != nil {
        t.Fatalf("OnHello: %v", err)
    }
    if !sess.State().IsForwarding {
        t.Errorf("remote DB must set IsForwarding")
    }
    if sess.State().RouteTarget != "peer.internal:9001" {
        t.Errorf("RouteTarget = %q want peer.internal:9001", sess.State().RouteTarget)
    }
    if !rebindCalled {
        t.Fatalf("rebind not triggered")
    }
    if rebindHello.User == "" || rebindHello.User == "alice" {
        t.Errorf("rebind hello.User must be __peer__|...; got %q", rebindHello.User)
    }
}
```

- [ ] **Step 3: Write the failing test for unknown DB**

```go
func TestForwardPlugin_OnHello_UnknownDatabase_Errors(t *testing.T) {
    p := newTestForwardPlugin(t, fakeNetworkState{
        databases: map[string]network.DatabaseInfo{},
        indexers:  map[uint64]network.IndexerInfo{},
    })
    sess := newTestSession(t)
    hello := &chproto.ClientHello{Database: "nope"}

    err := p.OnHello(context.Background(), sess, hello)
    if err == nil || !strings.Contains(err.Error(), "doesn't exist") {
        t.Fatalf("expected db-doesn't-exist error, got %v", err)
    }
}
```

- [ ] **Step 4: Write the failing test for empty DB (defer decision)**

```go
func TestForwardPlugin_OnHello_EmptyDatabase_DefersToLocal(t *testing.T) {
    p := newTestForwardPlugin(t, fakeNetworkState{})
    sess := newTestSession(t)
    hello := &chproto.ClientHello{Database: ""}

    if err := p.OnHello(context.Background(), sess, hello); err != nil {
        t.Fatalf("OnHello empty DB must defer, not error: %v", err)
    }
    if sess.State().IsForwarding {
        t.Errorf("empty DB must not set IsForwarding")
    }
}
```

- [ ] **Step 5: Run the four tests to verify they fail**

```bash
bazel test //pkg/plugins/forward:forward_test --test_arg=-test.v 2>&1 | tail -30
```

Expected: FAIL — package doesn't exist.

- [ ] **Step 6: Implement the plugin**

`pkg/plugins/forward/forward.go`:

```go
package forward

import (
    "context"
    "fmt"
    "time"

    "github.com/sentioxyz/housegate/pkg/auth"
    "github.com/sentioxyz/housegate/pkg/chproto"
    "github.com/sentioxyz/housegate/pkg/chsession"
    "github.com/sentioxyz/housegate/pkg/credentials"
    "github.com/sentioxyz/housegate/pkg/network"
    "github.com/sentioxyz/housegate/pkg/peer"
    "github.com/sentioxyz/housegate/pkg/plugin"
    "github.com/sentioxyz/sentio-core/common/log"
)

type Plugin struct {
    NetworkState  network.State
    SelfIndexerID uint64
    PeerSigner    auth.PeerSigner
    PeerTokenTTL  time.Duration
    Fallback      credentials.CredentialProvider

    dialPeer func(ctx context.Context, addr string) (*chproto.Codec, error)
    rebindFn func(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error
}

func (p *Plugin) Name() string { return "forward" }

// OnHello looks up hello.Database in NetworkState. Local DB → no-op
// (chain proceeds locally). Remote DB → dial peer's internal-port,
// rebind the session, set IsForwarding=true. Unknown DB → return a
// CH-shaped "doesn't exist" error so the client sees a familiar
// failure mode.
func (p *Plugin) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
    if hello.Database == "" {
        return nil  // defer to first USE
    }
    info, ok := p.NetworkState.RetrieveDatabaseInfo(network.Database(hello.Database))
    if !ok {
        return fmt.Errorf("Code: 81. DB::Exception: Database %s doesn't exist", hello.Database)
    }
    if info.IndexerId == p.SelfIndexerID {
        return nil  // serve locally
    }
    return p.pivotToPeer(ctx, sess, hello, info.IndexerId)
}

func (p *Plugin) pivotToPeer(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello, peerIndexer uint64) error {
    peerInfo, ok := p.NetworkState.RetrieveIndexerInfo(network.IndexId(peerIndexer))
    if !ok {
        return fmt.Errorf("indexer %d not found in NetworkState", peerIndexer)
    }
    addr := fmt.Sprintf("%s:%d", peerInfo.IndexerUrl, peerInfo.ClickhouseProxyPort)

    up, err := p.dialPeer(ctx, addr)
    if err != nil {
        return fmt.Errorf("dial peer %s: %w", addr, err)
    }

    user, password, err := peer.SignPeerHello(p.PeerSigner, p.PeerTokenTTL, peerInfo, p.Fallback)
    if err != nil {
        return fmt.Errorf("sign peer hello for %s: %w", addr, err)
    }

    peerHello := &chproto.ClientHello{
        ClientName:   hello.ClientName,
        Major:        hello.Major,
        Minor:        hello.Minor,
        ProtoVersion: hello.ProtoVersion,
        Database:     hello.Database,
        User:         user,
        Password:     password,
    }
    if err := p.rebindFn(ctx, sess, up, peerHello); err != nil {
        return fmt.Errorf("rebind to peer %s: %w", addr, err)
    }

    sess.State().IsForwarding = true
    sess.State().RouteTarget = addr
    log.Infow("forward: pivoted session to peer at hello",
        "db", hello.Database, "peer", addr, "indexer", peerIndexer)
    return nil
}

// RunOnForward implements plugin.ForwardAware. The forward plugin must
// keep running on already-forwarded sessions so OnQuery can re-route on
// USE (Phase 3).
func (*Plugin) RunOnForward() bool { return true }

// Compile-time interface assertions
var (
    _ plugin.HelloPlugin    = (*Plugin)(nil)
    _ plugin.ForwardAware   = (*Plugin)(nil)
)
```

`pkg/plugins/forward/use_regex.go` (stub for Phase 3):

```go
package forward

// matchUse will return (newDB, true) if sql is a standalone USE statement,
// (zero, false) otherwise. Phase 3 fills this in.
func matchUse(sql string) (string, bool) { return "", false }
```

- [ ] **Step 7: Generate BUILD.bazel + run tests**

```bash
bazel run //:gazelle
bazel test //pkg/plugins/forward:forward_test --test_arg=-test.v
```

Expected: PASS for all four OnHello tests.

- [ ] **Step 8: Commit**

```bash
git add pkg/plugins/forward/
git commit -m "forward: new plugin pivots sessions to peer at hello time

OnHello looks up hello.Database in NetworkState. Local DB serves
normally; remote DB triggers a peer-internal dial, RebindToPeer with
__peer__-envelope hello, and sets IsForwarding=true. Unknown DB
returns a CH-shaped 'doesn't exist' error.

USE-time re-routing arrives in Phase 3."
```

---

## Task 2.6: Wire `forward` plugin into `buildServer` chain

**Files:**
- Modify: `build.go`
- Test: `build_test.go`

- [ ] **Step 1: Write the failing test**

In `build_test.go`:

```go
func TestBuildServer_ForwardPluginInsertedBeforeRewrite(t *testing.T) {
    cfg := minimalServerConfig(t)
    bs, err := buildServer(Options{Config: cfg}, newTestRedisFactory(t))
    if err != nil { t.Fatalf("buildServer: %v", err) }
    defer bs.teardown()

    chain := extractChain(t, bs.listeners[0].Server)  // unwraps PluginChain from Hooks

    var fwdIdx, rwIdx int = -1, -1
    for i, p := range chain.HelloPlugins {
        switch p.(type) {
        case *forward.Plugin:
            fwdIdx = i
        case *rewrite.Plugin:
            rwIdx = i
        }
    }
    if fwdIdx < 0 { t.Fatal("forward plugin not in chain") }
    if rwIdx < 0  { t.Fatal("rewrite plugin not in chain") }
    if fwdIdx >= rwIdx {
        t.Fatalf("forward plugin (idx=%d) must run before rewrite (idx=%d)", fwdIdx, rwIdx)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
bazel test //:build_test --test_filter=TestBuildServer_ForwardPluginInsertedBeforeRewrite --test_arg=-test.v
```

Expected: FAIL — forward plugin not wired.

- [ ] **Step 3: Wire the plugin**

In `build.go`, in `buildServer` (lines 444–465), insert before the rewrite plugin:

```go
forwardPlugin := &forward.Plugin{
    NetworkState:  ns,
    SelfIndexerID: cfg.IndexerID,
    PeerSigner:    relaySigner,    // already constructed above for sentio rewriter
    PeerTokenTTL:  peerLoginTokenTTL,
    Fallback:      credProvider,
}
forwardPlugin.SetDialer(buildPeerDialer(...))   // implement: dial via standard chproto.NewCodec + net.Dial
forwardPlugin.SetRebinder(func(ctx context.Context, sess chsession.Session, up *chproto.Codec, hello *chproto.ClientHello) error {
    return sess.RebindToPeer(ctx, up, hello)
})

helloPlugins := []plugin.HelloPlugin{
    routeStripper,
    credentialPlugin,
    forwardPlugin,    // ← inserted here, before sessionState + rewrite
    sessionStatePlugin,
    rewritePlugin,
}
```

Add `SetDialer` / `SetRebinder` setters on `*forward.Plugin` (so tests can inject fakes; production wires the real ones from build.go). Update the test in 2.5 to use these setters instead of internal fields.

- [ ] **Step 4: Run test to verify it passes**

```bash
bazel test //:build_test --test_filter=TestBuildServer_ForwardPluginInsertedBeforeRewrite --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Run full test suite**

```bash
bazel test //...
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add build.go pkg/plugins/forward/forward.go build_test.go
git commit -m "build: wire forward plugin into server-mode chain before rewrite

Forward decision runs before rewrite so the rewriter never sees a
session destined for a peer; cross-DB SQL still falls back to the
rewriter's remote() path on locally-served sessions."
```

---

## Task 2.7: Phase 2 integration test — agent → A → pivots to B at hello

**Files:**
- Test: `cmd/integration/two_proxy_pivot_test.go` (new) or extend existing integration test layout

- [ ] **Step 1: Write the integration test**

Spin up two `buildServer` instances (A and B) sharing an in-memory NetworkState that says `tenant2 → indexer-B`. Connect a fake agent (just a chproto client) to A, send `ClientHello{Database: "tenant2"}`, send a no-op `Query` (e.g., `SELECT 1`), assert: A's metrics show one forwarded session; B's metrics show one peer-trusted incoming session; the query response comes back to the client.

```go
func TestTwoProxyPivot_HelloDatabaseRoutesToPeer(t *testing.T) {
    ns := network.NewInMemory(network.SeedYAML(t, `
indexers:
  - id: 100
    url: 127.0.0.1
    clickhouse_proxy_port: $A_PORT
  - id: 200
    url: 127.0.0.1
    clickhouse_proxy_port: $B_PORT
databases:
  - name: tenant2
    indexer_id: 200
`))

    a := startServer(t, withIndexer(100), withNetworkState(ns), withInternalPort())
    b := startServer(t, withIndexer(200), withNetworkState(ns), withInternalPort(),
        withFakeCH(rowsFor("SELECT 1", []int{1})))

    conn := dialClient(t, a.ExternalAddr())
    sendHello(t, conn, "alice", "pw", "tenant2")
    expectServerHello(t, conn)
    sendQuery(t, conn, "SELECT 1")
    rows := readDataBlock(t, conn)

    require.Equal(t, [][]any{{1}}, rows)
    require.Equal(t, int64(1), a.metrics.SessionsForwarded.Get())
    require.Equal(t, int64(1), b.metrics.PeerTrustedSessions.Get())
}
```

- [ ] **Step 2: Run the test**

```bash
bazel test //cmd/integration:integration_test --test_filter=TestTwoProxyPivot_HelloDatabaseRoutesToPeer --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/two_proxy_pivot_test.go
git commit -m "integration: agent→A→pivot-to-B at hello time"
```

---

## Phase 2 — Wrap-up

```bash
bazel test //...
git push -u origin feat/forward-decision-hello
gh pr create --title "feat: forward-decision plugin (OnHello + RebindToPeer)" --body '...'
```

Wait for review + merge before Phase 3.

---

# Phase 3 — USE rebind

**Goal of phase:** mid-session `USE <db>` for a database hosted on a different proxy triggers `RebindToPeer` on the new target.

**Branch:** `feat/forward-decision-use`

```bash
git switch main && git pull --ff-only && git switch -c feat/forward-decision-use
```

---

## Task 3.1: Implement `matchUse` regex

**Files:**
- Modify: `pkg/plugins/forward/use_regex.go`
- Test: `pkg/plugins/forward/use_regex_test.go`

- [ ] **Step 1: Write the failing tests**

`pkg/plugins/forward/use_regex_test.go`:

```go
func TestMatchUse(t *testing.T) {
    cases := []struct {
        sql    string
        wantDB string
        wantOK bool
    }{
        {"USE tenant1",     "tenant1", true},
        {"use tenant1",     "tenant1", true},  // case-insensitive
        {"  USE  tenant1 ", "tenant1", true},
        {"USE tenant1;",    "tenant1", true},
        {"USE `tenant-1`",  "tenant-1", true}, // backticks stripped
        {"USE \"tenant1\"", "tenant1",  true}, // quotes stripped
        {"USE tenant1 SETTINGS x=1", "", false}, // not a standalone USE
        {"SELECT 1",        "",        false},
        {"-- USE comment",  "",        false},
    }
    for _, c := range cases {
        gotDB, gotOK := matchUse(c.sql)
        if gotDB != c.wantDB || gotOK != c.wantOK {
            t.Errorf("matchUse(%q) = (%q, %v) want (%q, %v)",
                c.sql, gotDB, gotOK, c.wantDB, c.wantOK)
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
bazel test //pkg/plugins/forward:forward_test --test_filter=TestMatchUse --test_arg=-test.v
```

Expected: FAIL.

- [ ] **Step 3: Implement**

In `pkg/plugins/forward/use_regex.go`, replace the stub:

```go
package forward

import (
    "regexp"
    "strings"
)

var useRegex = regexp.MustCompile(`(?i)^\s*USE\s+([` + "`" + `"]?)([A-Za-z0-9_\-]+)\1\s*;?\s*$`)

func matchUse(sql string) (string, bool) {
    m := useRegex.FindStringSubmatch(sql)
    if m == nil {
        return "", false
    }
    return strings.TrimSpace(m[2]), true
}
```

- [ ] **Step 4: Run test to verify pass**

```bash
bazel test //pkg/plugins/forward:forward_test --test_filter=TestMatchUse --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/forward/use_regex.go pkg/plugins/forward/use_regex_test.go
git commit -m "forward: implement standalone USE regex matcher

Narrow on purpose: only matches \`USE <name>\` (with optional quoting/
trailing semicolon). Anything more elaborate (USE … SETTINGS …) falls
through to the rewriter's maybeUpdateLogicalDatabase path."
```

---

## Task 3.2: Add `OnQuery` USE-rebind to forward plugin

**Files:**
- Modify: `pkg/plugins/forward/forward.go`
- Test: `pkg/plugins/forward/forward_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestForwardPlugin_OnQuery_USESameDb_NoRebind(t *testing.T) {
    var rebindCalled bool
    p := newTestForwardPlugin(t, fakeNS_TwoTenants(t))
    p.SetRebinder(func(...) error { rebindCalled = true; return nil })

    sess := newTestSessionWithRouteTarget(t, "peer.internal:9001")  // already on B
    sess.State().LogicalDatabase = "tenant2"

    qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE tenant2"}
    if err := p.OnQuery(context.Background(), qctx); err != nil {
        t.Fatalf("OnQuery: %v", err)
    }
    if rebindCalled {
        t.Errorf("USE same DB must not trigger rebind")
    }
}

func TestForwardPlugin_OnQuery_USEDifferentDb_TriggersRebind(t *testing.T) {
    var rebindAddr string
    p := newTestForwardPlugin(t, fakeNS_TwoTenants(t))
    p.SetDialer(func(_ context.Context, addr string) (*chproto.Codec, error) {
        rebindAddr = addr
        _, codec := newPipedCodec(t)
        return codec, nil
    })
    p.SetRebinder(func(...) error { return nil })

    sess := newTestSessionWithRouteTarget(t, "")  // currently local
    qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "USE tenant3"}

    if err := p.OnQuery(context.Background(), qctx); err != nil {
        t.Fatalf("OnQuery: %v", err)
    }
    if rebindAddr != "peer3.internal:9001" {
        t.Errorf("rebind addr = %q want peer3.internal:9001", rebindAddr)
    }
    if !sess.State().IsForwarding {
        t.Errorf("USE remote DB must set IsForwarding")
    }
}

func TestForwardPlugin_OnQuery_NonUseQuery_NoOp(t *testing.T) {
    p := newTestForwardPlugin(t, fakeNS_TwoTenants(t))
    sess := newTestSession(t)
    qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1"}

    if err := p.OnQuery(context.Background(), qctx); err != nil {
        t.Fatalf("OnQuery: %v", err)
    }
    if sess.State().IsForwarding {
        t.Errorf("non-USE must not flip IsForwarding")
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
bazel test //pkg/plugins/forward:forward_test --test_filter=TestForwardPlugin_OnQuery --test_arg=-test.v
```

Expected: FAIL — `Plugin.OnQuery` doesn't exist or no-ops.

- [ ] **Step 3: Implement OnQuery**

In `pkg/plugins/forward/forward.go`, add:

```go
// OnQuery intercepts standalone USE statements. If USE targets a logical
// database hosted on a peer, re-pivots the session via RebindToPeer.
// Anything other than a bare USE is a no-op (the rewriter handles
// LogicalDatabase tracking for complex forms).
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
    newDB, ok := matchUse(qctx.OriginalSQL)
    if !ok {
        return nil
    }
    info, found := p.NetworkState.RetrieveDatabaseInfo(network.Database(newDB))
    if !found {
        return nil  // let CH return the real "doesn't exist" error
    }
    sess := qctx.Session
    var newAddr string
    if info.IndexerId == p.SelfIndexerID {
        newAddr = ""
    } else {
        peerInfo, ok := p.NetworkState.RetrieveIndexerInfo(network.IndexId(info.IndexerId))
        if !ok {
            return fmt.Errorf("indexer %d not in NetworkState", info.IndexerId)
        }
        newAddr = fmt.Sprintf("%s:%d", peerInfo.IndexerUrl, peerInfo.ClickhouseProxyPort)
    }
    if newAddr == sess.State().RouteTarget {
        return nil  // already on the right target
    }
    if newAddr == "" {
        // remote → local: not yet supported. Fall through; the local CH
        // will likely error if it doesn't host the DB. (Open question
        // §12.4 in spec.)
        return nil
    }
    return p.pivotToPeer(ctx, sess, &chproto.ClientHello{
        Database: newDB,
        // ClientName / version copied from the existing bound upstream
        // would be ideal, but for v1 we use the session's recorded values:
        ClientName:   sess.State().ClientHostname,
    }, info.IndexerId)
}
```

Add the `QueryPlugin` interface assertion:

```go
var _ plugin.QueryPlugin = (*Plugin)(nil)
```

- [ ] **Step 4: Run tests to verify pass**

```bash
bazel test //pkg/plugins/forward:forward_test --test_arg=-test.v
```

Expected: PASS for all forward tests.

- [ ] **Step 5: Wire forward plugin into the QueryPlugins slice in `buildServer`**

In `build.go`, add `forwardPlugin` to `QueryPlugins` (alongside the existing OnQuery plugins; before `rewritePlugin`):

```go
queryPlugins := []plugin.QueryPlugin{
    authPlugin,
    forwardPlugin,    // ← new: catches USE before rewrite
    usagePlugin,
    concurrencyPlugin,
    rewritePlugin,
    routeSigner,
    metrics,
}
```

- [ ] **Step 6: Run full test suite**

```bash
bazel test //...
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/plugins/forward/forward.go pkg/plugins/forward/forward_test.go build.go
git commit -m "forward: detect USE on OnQuery and rebind to target peer

Standalone USE <db> resolves the new target via NetworkState. If the
target peer differs from the current RouteTarget, the session pivots
via RebindToPeer; replay re-issues USE+SET on the new upstream."
```

---

## Task 3.3: Phase 3 integration test — USE pivots mid-session

**Files:**
- Test: `cmd/integration/two_proxy_use_pivot_test.go` (new)

- [ ] **Step 1: Write the integration test**

Like Task 2.7 but: client connects to A with `Database: ""`, runs `USE tenant2` (on B), then `SELECT 1`. Asserts B sees the SELECT, not A. Asserts A's session_replays counter incremented.

```go
func TestTwoProxyPivot_USERoutesMidSession(t *testing.T) {
    ns := /* same as Task 2.7 */
    a, b := startServerPair(t, ns)

    conn := dialClient(t, a.ExternalAddr())
    sendHello(t, conn, "alice", "pw", "")  // empty DB
    expectServerHello(t, conn)

    sendQuery(t, conn, "USE tenant2")
    expectEndOfStream(t, conn)

    sendQuery(t, conn, "SELECT 1")
    rows := readDataBlock(t, conn)

    require.Equal(t, [][]any{{1}}, rows)
    require.Equal(t, int64(1), b.metrics.PeerTrustedSessions.Get())
    require.Equal(t, int64(1), a.metrics.SessionRebinds.Get())
}
```

- [ ] **Step 2: Run**

```bash
bazel test //cmd/integration:integration_test --test_filter=TestTwoProxyPivot_USERoutesMidSession --test_arg=-test.v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/two_proxy_use_pivot_test.go
git commit -m "integration: USE mid-session pivots from A to B"
```

---

## Phase 3 — Wrap-up

```bash
bazel test //...
git push -u origin feat/forward-decision-use
gh pr create --title "feat: USE rebind in forward-decision plugin" --body '...'
```

Wait for merge.

---

# Phase 4 — Agent auto-discovery

**Goal of phase:** agent reads NetworkState; picks a permissioned random peer; falls back to any bound peer for new accounts. Bootstrap fallback is logged + metered.

**Branch:** `feat/agent-auto-discover`

```bash
git switch main && git pull --ff-only && git switch -c feat/agent-auto-discover
```

---

## Task 4.1: Add `agent_network_state` config block

**Files:**
- Modify: `pkg/config/config.go`
- Test: `pkg/config/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestConfig_Validate_Agent_AutoDiscover(t *testing.T) {
    cfg := minimalAgentConfig(t)
    cfg.Agent.Upstream = ""  // auto-discover
    cfg.Agent.NetworkState = config.NetworkStateConfig{Source: "yaml://test.yaml"}

    if err := cfg.Validate(); err != nil {
        t.Fatalf("auto-discover agent must validate: %v", err)
    }

    cfg.Agent.Upstream = ""
    cfg.Agent.NetworkState = config.NetworkStateConfig{}
    if err := cfg.Validate(); err == nil {
        t.Fatalf("auto-discover agent without NetworkState must error")
    }
}
```

- [ ] **Step 2: Run, verify fail; add field; verify pass; commit**

In `pkg/config/config.go`, in `AgentConfig` struct add:

```go
NetworkState NetworkStateConfig `yaml:"network_state" json:"network_state"`
```

In `Agent.Validate()`, accept either non-empty `Upstream` or non-empty `NetworkState.Source`:

```go
if c.Upstream == "" && !c.NetworkState.IsConfigured() {
    return fmt.Errorf("agent: must set either upstream or network_state")
}
```

Run, commit:

```bash
git add pkg/config/config.go pkg/config/config_test.go
git commit -m "config: agent accepts network_state for upstream auto-discovery"
```

---

## Task 4.2: Implement two-tier upstream selector

**Files:**
- Create: `pkg/plugins/agent/upstream_select.go`
- Test: `pkg/plugins/agent/upstream_select_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestSelectUpstream_PermissionedTier(t *testing.T) {
    ns := fakeNS{
        indexers: map[uint64]network.IndexerInfo{
            1: {IndexerId: 1, IndexerUrl: "a", ClickhouseProxyPort: 9000},
            2: {IndexerId: 2, IndexerUrl: "b", ClickhouseProxyPort: 9000},
            3: {IndexerId: 3, IndexerUrl: "c", ClickhouseProxyPort: 9000},
        },
        databases: map[string]network.DatabaseInfo{
            "tenantA": {IndexerId: 1},
            "tenantB": {IndexerId: 2},
        },
        permissions: map[string]map[string]bool{
            "0xalice": {"tenantA": true},  // alice has perms only on tenantA → indexer 1
        },
    }

    sel := &Selector{NS: ns, Account: "0xalice"}
    chosen, err := sel.Pick(testRand(t, 0))  // deterministic
    require.NoError(t, err)
    require.Equal(t, "a:9000", chosen.Addr())
    require.False(t, chosen.IsBootstrap)
}

func TestSelectUpstream_BootstrapFallback(t *testing.T) {
    ns := fakeNS{
        indexers: map[uint64]network.IndexerInfo{
            1: {IndexerId: 1, IndexerUrl: "a", ClickhouseProxyPort: 9000},
        },
        permissions: map[string]map[string]bool{},  // no perms for anyone
    }
    sel := &Selector{NS: ns, Account: "0xnewuser"}
    chosen, err := sel.Pick(testRand(t, 0))
    require.NoError(t, err)
    require.Equal(t, "a:9000", chosen.Addr())
    require.True(t, chosen.IsBootstrap, "new account must take bootstrap path")
}

func TestSelectUpstream_NoBoundIndexers_Errors(t *testing.T) {
    ns := fakeNS{indexers: map[uint64]network.IndexerInfo{}}
    sel := &Selector{NS: ns, Account: "0xalice"}
    _, err := sel.Pick(testRand(t, 0))
    require.ErrorContains(t, err, "no bound indexers")
}
```

- [ ] **Step 2: Run, verify fail; implement; run, verify pass; commit**

`pkg/plugins/agent/upstream_select.go`:

```go
package agent

import (
    "errors"
    "fmt"
    "math/rand"

    "github.com/sentioxyz/housegate/pkg/network"
)

type Selector struct {
    NS      network.State
    Account string
}

type Choice struct {
    Indexer     network.IndexerInfo
    IsBootstrap bool
}

func (c Choice) Addr() string {
    return fmt.Sprintf("%s:%d", c.Indexer.IndexerUrl, c.Indexer.ClickhouseProxyPort)
}

func (s *Selector) Pick(r *rand.Rand) (Choice, error) {
    perms, _ := s.NS.RetrieveDatabasePermissions(network.AccountAddress(s.Account))

    permissionedIndexers := map[uint64]struct{}{}
    for db := range perms {
        if info, ok := s.NS.RetrieveDatabaseInfo(db); ok {
            permissionedIndexers[uint64(info.IndexerId)] = struct{}{}
        }
    }

    var bound, permissioned []network.IndexerInfo
    for _, info := range s.NS.RetrieveAllIndexerInfos() {
        if info.ClickhouseProxyPort == 0 {
            continue
        }
        bound = append(bound, info)
        if _, ok := permissionedIndexers[uint64(info.IndexerId)]; ok {
            permissioned = append(permissioned, info)
        }
    }

    switch {
    case len(permissioned) > 0:
        return Choice{Indexer: permissioned[r.Intn(len(permissioned))]}, nil
    case len(bound) > 0:
        return Choice{Indexer: bound[r.Intn(len(bound))], IsBootstrap: true}, nil
    default:
        return Choice{}, errors.New("no bound indexers in NetworkState")
    }
}
```

Run, verify pass, commit:

```bash
git add pkg/plugins/agent/upstream_select.go pkg/plugins/agent/upstream_select_test.go
git commit -m "agent: two-tier random upstream selector

Permissioned tier first; bootstrap fallback to any bound peer for new
accounts (open question §8.1 of spec)."
```

---

## Task 4.3: Wire selector into `buildAgent` + observability

**Files:**
- Modify: `build.go::buildAgent`
- Test: integration in `cmd/integration/agent_auto_discover_test.go`

- [ ] **Step 1: Write integration test**

Set up two server-mode proxies + a agent configured with `NetworkState` only (no `Upstream`). Agent dials, asserts it lands on a permissioned indexer when perms are set; on a random indexer when no perms (and the bootstrap-fallback log line + metric appear).

- [ ] **Step 2: Wire selector in `buildAgent`**

Replace the fixed-upstream dialer with one that calls `Selector.Pick` per session:

```go
selector := &agent.Selector{NS: agentNS, Account: deriveAddress(cfg.Agent.PrivateKeyHex)}
dialer := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
    choice, err := selector.Pick(rand.New(rand.NewSource(time.Now().UnixNano())))
    if err != nil {
        return nil, fmt.Errorf("agent select upstream: %w", err)
    }
    if choice.IsBootstrap {
        log.Warnw("agent bootstrap fallback: no permissioned DBs",
            "account", selector.Account, "chosen", choice.Indexer.IndexerId)
        agentBootstrapFallbackTotal.WithLabelValues(selector.Account).Inc()
    }
    return chproto.DialNew(ctx, choice.Addr())
}
```

Honor `cfg.Agent.Upstream` as override when set.

- [ ] **Step 3: Run integration test, commit**

```bash
git add build.go pkg/plugins/agent/ cmd/integration/agent_auto_discover_test.go
git commit -m "build: agent auto-discovers upstream from NetworkState

Permissioned-random first; bootstrap fallback for new accounts emits
log + agent_bootstrap_fallback_total metric. agent_upstream still
honored as override when set."
```

---

## Phase 4 — Wrap-up

```bash
bazel test //...
git push -u origin feat/agent-auto-discover
gh pr create --title "feat: agent auto-discovers upstream from NetworkState" --body '...'
```

Wait for merge.

---

# Phase 5 — Remove `forwarding-only` mode

**Goal of phase:** delete the now-redundant code path. A "router-only" deployment is just a server with no shard configured.

**Branch:** `chore/remove-forwarding-only-mode`

```bash
git switch main && git pull --ff-only && git switch -c chore/remove-forwarding-only-mode
```

---

## Task 5.1: Make server mode tolerate missing shard

**Files:**
- Modify: `pkg/config/config.go::Config.Validate`
- Modify: `build.go::buildServer` (handle nil shard)
- Test: existing tests + new test for "router-only server"

- [ ] **Step 1: Write the failing test**

```go
func TestBuildServer_NoShardConfig_BuildsRouterOnly(t *testing.T) {
    cfg := minimalServerConfig(t)
    cfg.Shard = nil
    cfg.Upstream = ""
    cfg.NetworkState.Source = "yaml://test.yaml"  // still required

    bs, err := buildServer(Options{Config: cfg}, newTestRedisFactory(t))
    require.NoError(t, err)
    defer bs.teardown()

    require.GreaterOrEqual(t, len(bs.listeners), 1)
    // No replica pool created when no shard:
    require.Nil(t, bs.replicas, "router-only server has no local replicas")
}
```

- [ ] **Step 2: Run, fail, implement, pass**

In `Config.Validate`, allow ServerMode without `Shard` (drop the requirement). In `buildServer`, when `cfg.Shard == nil`, skip replica pool construction; the dialer fails any local-serving attempt (which means every session must forward — equivalent to today's forwarding-only).

- [ ] **Step 3: Commit**

```bash
git add pkg/config/config.go build.go build_test.go
git commit -m "build: server mode tolerates missing shard (router-only deployment)"
```

---

## Task 5.2: Delete `ModeForwardingOnly` and `buildForwarding`

**Files:**
- Modify: `pkg/config/config.go`
- Modify: `build.go`
- Test: existing tests must keep passing

- [ ] **Step 1: Update `Config.Mode()`**

In `pkg/config/config.go`:

```go
func (c *Config) Mode() Mode {
    if c.Agent.Mode {
        return ModeAgent
    }
    return ModeServer
}

const (
    ModeAgent Mode = "agent"
    ModeServer  Mode = "server"
)
```

Delete `ModeForwardingOnly` constant + the `case` in `String()`.

- [ ] **Step 2: Delete `buildForwarding` and `pickRandomBoundProxy`**

In `build.go`, delete:
- `buildForwarding` (lines 546–597)
- `pickRandomBoundProxy` (lines 608–627)

Update `New(opts)` to dispatch only on `ModeAgent` vs `ModeServer`.

- [ ] **Step 3: Run full test suite**

```bash
bazel test //...
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/config/config.go build.go
git commit -m "build: delete forwarding-only mode + pickRandomBoundProxy

Routing-only deployments are now expressed as 'server with no shard'.
Phase 1-4 of the two-port server-mode rollout subsumed every
forwarding-only capability."
```

---

## Task 5.3: Update `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the runtime-modes table**

Find the section "### 1. Three runtime modes (one binary, selected at startup)" and update:

- Title: "### 1. Two runtime modes ..."
- Drop the `Forwarding-only` row.
- Add to the Server-mode row: "If `shard` is empty, the proxy is router-only — every session forwards to a peer."
- Update §2 packet-level pipeline if any forwarding-only-specific lines exist.
- Add a one-paragraph section describing the internal-port + forward plugin (cite the spec).

- [ ] **Step 2: Run docs check (if any)**

```bash
grep -n forwarding-only CLAUDE.md
```

Expected: only references to historical context (or none).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(CLAUDE.md): drop forwarding-only mode; add internal-port note"
```

---

## Phase 5 — Wrap-up

```bash
bazel test //...
git push -u origin chore/remove-forwarding-only-mode
gh pr create --title "chore: remove forwarding-only mode" --body '...'
```

---

# Self-Review

Run after writing the plan. Findings recorded inline.

**1. Spec coverage:** every spec section has a task:

| Spec § | Implemented in |
|---|---|
| §3 invariants | enforced by Phase 1 (internal-port firewall + Stripper rejection) and Phase 2 (rewriter unchanged) |
| §4 topology | Phase 1 (listeners) + Phase 2 (forward plugin) + Phase 3 (USE rebind) |
| §5 listener+chain composition | Phase 1.2/1.3/1.4 (preflag, two listeners, Stripper hard-reject) |
| §6.1 forward-decision OnHello | Phase 2.5/2.6 |
| §6.2 USE detection | Phase 3.1/3.2 |
| §6.3 cross-DB SQL falls back | implicit — forward plugin doesn't peek SQL bodies; rewriter `remote()` path unchanged |
| §7 ForwardAware marker | Phase 2.2/2.3 |
| §8 agent selection | Phase 4.1/4.2/4.3 |
| §8.1 bootstrap fallback | Phase 4.2 (Selector) + 4.3 (logging/metric) |
| §9 RebindUpstream-to-peer | Phase 2.4 (`RebindToPeer`) |
| §10 config | Phase 1.1 + Phase 4.1 |
| §11 phasing | this plan's phase 1-5 mirrors the spec phasing |
| §13 file map | matches the file structure section above |

**2. Placeholder scan:** none. Every step has either runnable code, an exact command, or a specific edit pointing at file:line.

**3. Type consistency:** `Selector.Pick(r *rand.Rand) (Choice, error)` — referenced as `selector.Pick(rand.New(...))` in 4.3. ✔ `RebindToPeer(ctx, newUp, peerHello)` — same signature in tests (2.4) and call sites (2.5/3.2). ✔ `ForwardAware.RunOnForward() bool` — same signature throughout. ✔ `forward.Plugin` field `RouteTarget` written via `sess.State().RouteTarget = addr` everywhere. ✔ `network.Database` (newtype) used in `RetrieveDatabaseInfo(network.Database(name))` consistently. ✔

---

# Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-28-two-port-server-mode.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Resilient to context bloat across the 5 phases.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Given the plan spans 5 phase-PRs and many tasks, **subagent-driven** is the better fit for this one — fresh context per task keeps each implementation tight and lets you (the user) review between tasks.

Which approach?
