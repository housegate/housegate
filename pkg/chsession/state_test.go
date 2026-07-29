package chsession

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestSessionState_MaintenanceFlag(t *testing.T) {
	s := NewSessionState()
	if s.Snapshot().Maintenance {
		t.Fatalf("new session: Snapshot().Maintenance=true, want false")
	}

	s.SetMaintenance(true)
	if !s.Snapshot().Maintenance {
		t.Fatalf("after SetMaintenance(true): Snapshot().Maintenance=false, want true")
	}

	s.SetMaintenance(false)
	if s.Snapshot().Maintenance {
		t.Fatalf("after SetMaintenance(false): Snapshot().Maintenance=true, want false")
	}
}

func TestSessionState_SnapshotIsIsolated(t *testing.T) {
	s := NewSessionState()
	s.SetDatabase("db1")
	s.AddSetting("max_execution_time", chproto.Setting{Key: "max_execution_time", Value: "30"})

	snap := s.Snapshot()
	// Mutate the original; snapshot must not change.
	s.SetDatabase("db2")
	s.AddSetting("max_memory", chproto.Setting{Key: "max_memory", Value: "1G"})

	if snap.Database != "db1" {
		t.Fatalf("snap.Database=%q, want db1", snap.Database)
	}
	if _, exists := snap.Settings["max_memory"]; exists {
		t.Fatalf("snap leaked post-snapshot Settings entry")
	}
}

func TestSessionState_ConcurrentMutateAndSnapshot(t *testing.T) {
	s := NewSessionState()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.SetDatabase("db")
			s.AddSetting("k", chproto.Setting{Key: "k", Value: "v"})
			s.MarkActiveRewrite(nil)
			s.ClearActiveRewrite()
		}()
		go func() {
			defer wg.Done()
			_ = s.Snapshot()
		}()
	}
	wg.Wait()
	// No panic / race is the assertion; run under -race.
}

func TestSession_BindAndRebindUpstream(t *testing.T) {
	clientConn, _ := net.Pipe()
	sess := New(42, clientConn)

	up1Client, _ := net.Pipe()
	up1 := chproto.NewCodec(up1Client, chproto.DirToUpstream)
	if err := sess.BindUpstream(context.Background(), up1); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	if sess.Upstream() != up1 {
		t.Fatalf("Upstream() did not return bound codec")
	}

	up2Client, _ := net.Pipe()
	up2 := chproto.NewCodec(up2Client, chproto.DirToUpstream)
	// Rebind without replay — MVP path.
	if err := sess.RebindUpstream(context.Background(), up2, false); err != nil {
		t.Fatalf("RebindUpstream: %v", err)
	}
	if sess.Upstream() != up2 {
		t.Fatalf("Upstream() after rebind did not swap")
	}
}

func TestSession_CloseIsIdempotent(t *testing.T) {
	clientConn, _ := net.Pipe()
	sess := New(1, clientConn)
	if err := sess.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// writeCapture is a read+write io.ReadWriter used for test codecs.
type writeCapture struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (c *writeCapture) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *writeCapture) Write(p []byte) (int, error) { return c.w.Write(p) }

func TestSessionState_Replay_EmitsUseAndSet(t *testing.T) {
	s := NewSessionState()
	s.ClientRevision = 54453
	s.SetDatabase("analytics")
	s.AddSetting("max_execution_time", chproto.Setting{Key: "max_execution_time", Value: "30"})

	cap := &writeCapture{r: &bytes.Buffer{}, w: &bytes.Buffer{}}
	up := chproto.NewCodec(cap, chproto.DirToUpstream)
	up.SetRevision(54453)

	if err := s.Replay(context.Background(), up); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	out := cap.w.String()
	if !bytes.Contains(cap.w.Bytes(), []byte("USE analytics")) {
		t.Fatalf("Replay did not emit USE analytics; out=%q", out)
	}
	if !bytes.Contains(cap.w.Bytes(), []byte("SET max_execution_time=30")) {
		t.Fatalf("Replay did not emit SET; out=%q", out)
	}
}

// TestSession_RebindToPeer verifies that RebindToPeer:
//  1. Writes a ClientHello carrying the peer envelope onto the new codec.
//  2. Reads the peer's ServerHello, negotiating the revision.
//  3. Sends the addendum (forcing notchunked framing).
//  4. Swaps the upstream pointer.
//  5. Does NOT replay Database/Settings. Forward-pivot replay queries are
//     proxy-generated and therefore do not carry the client's JWS auth
//     setting; the peer hello plus the triggering client query carry the
//     database switch instead.
func TestSession_RebindToPeer(t *testing.T) {
	// rev must be >= chproto.RevisionMinAddendum (54458) so SendAddendum fires,
	// and < chproto.RevisionMinChunkedPackets (54470) so the ServerHello the
	// fake peer writes (via proto.ServerHello.EncodeAware) does not need to
	// include proto_send_chunked_srv / proto_recv_chunked_srv fields that
	// ch-go's EncodeAware does not emit.
	const rev = chproto.RevisionMinAddendum

	// Build the session with an initial (placeholder) upstream.
	clientConn, _ := net.Pipe()
	defer clientConn.Close()
	sess := New(1, clientConn)

	placeholder, _ := net.Pipe()
	defer placeholder.Close()
	if err := sess.BindUpstream(context.Background(), chproto.NewCodec(placeholder, chproto.DirToUpstream)); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	// Pre-populate replayable state. RebindToPeer must not emit it on the
	// peer proxy connection because those internal Query packets would not
	// carry the client's per-query JWS auth setting.
	sess.State().SetDatabase("tenant1")
	sess.State().AddSetting("max_execution_time", chproto.Setting{Key: "max_execution_time", Value: "30"})

	// Build the new upstream pipe (peerServer ↔ newUpCodec).
	peerServer, peerClient := net.Pipe()
	newUp := chproto.NewCodec(peerClient, chproto.DirToUpstream)

	// captured holds what the fake peer server received (for assertions).
	var captured bytes.Buffer
	var capturedMu sync.Mutex
	peerHelloUser := make(chan string, 1)
	peerHelloPassword := make(chan string, 1)
	done := make(chan struct{})

	// Fake peer server goroutine: read ClientHello, write ServerHello, drain.
	go func() {
		defer close(done)
		defer peerServer.Close()
		_ = peerServer.SetDeadline(time.Now().Add(5 * time.Second))

		// Read the ClientHello. We use a raw codec in the server direction so
		// ReadPacket decodes it as a ClientHello (DirFromClient).
		srvCodec := chproto.NewCodec(peerServer, chproto.DirFromClient)
		pkt, err := srvCodec.ReadPacket(uint64(chproto.ClientHelloCode))
		if err != nil {
			peerHelloUser <- "READ_ERROR: " + err.Error()
			peerHelloPassword <- ""
			return
		}
		hello, ok := pkt.Decoded.(*chproto.ClientHello)
		if !ok {
			peerHelloUser <- "NOT_CLIENT_HELLO"
			peerHelloPassword <- ""
			return
		}
		peerHelloUser <- hello.User
		peerHelloPassword <- hello.Password

		// Write a stub ServerHello.
		srv := &proto.ServerHello{
			Name:     "peer-proxy",
			Major:    24,
			Minor:    1,
			Revision: rev,
		}
		var buf proto.Buffer
		srv.EncodeAware(&buf, rev)
		if _, err := peerServer.Write(buf.Buf); err != nil {
			return
		}

		// Drain remaining bytes (addendum only; no replay queries) so writes don't block.
		capturedMu.Lock()
		_, _ = io.Copy(&captured, peerServer)
		capturedMu.Unlock()
	}()

	// Build the peer hello with the peer envelope in User/Password.
	peerHello := &chproto.ClientHello{
		Name:            "housegate",
		Major:           24,
		Minor:           1,
		ProtocolVersion: rev,
		User:            "__peer__|self:9000",
		Password:        "fake-jws",
		Database:        "default",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sess.RebindToPeer(ctx, newUp, peerHello); err != nil {
		t.Fatalf("RebindToPeer: %v", err)
	}

	// Assert the upstream was swapped.
	if sess.Upstream() != newUp {
		t.Fatalf("Upstream() after RebindToPeer did not point to newUp")
	}

	// Assert PeerServerHelloRaw and PeerRevision were populated.
	if len(sess.State().PeerServerHelloRaw) == 0 {
		t.Error("PeerServerHelloRaw not populated after RebindToPeer")
	}
	if sess.State().PeerRevision != rev {
		t.Errorf("PeerRevision = %d, want %d", sess.State().PeerRevision, rev)
	}

	// Assert the fake peer server received the correct peer envelope.
	gotUser := <-peerHelloUser
	if gotUser != "__peer__|self:9000" {
		t.Fatalf("peer server received User=%q, want __peer__|self:9000", gotUser)
	}
	gotPassword := <-peerHelloPassword
	if gotPassword != "fake-jws" {
		t.Fatalf("peer server received Password=%q, want fake-jws", gotPassword)
	}

	// Close the client end so io.Copy in the goroutine unblocks, then assert
	// no unauthenticated replay content was emitted.
	peerClient.Close()
	<-done // deterministic: wait for the goroutine's io.Copy to finish

	capturedMu.Lock()
	got := captured.Bytes()
	capturedMu.Unlock()

	if bytes.Contains(got, []byte("USE tenant1")) {
		t.Fatalf("RebindToPeer must not replay USE tenant1; captured=%q", got)
	}
	if bytes.Contains(got, []byte("SET max_execution_time=30")) {
		t.Fatalf("RebindToPeer must not replay SET max_execution_time; captured=%q", got)
	}
}

// TestSessionState_IsRouted_ExcludesForwardPivot is the regression for
// the ProxyB-sees-ProxyA-as-user-identity bug found 2026-04-29:
// forward.Plugin.pivotToPeer calls SetRouteTarget to memoize the current
// peer for OnQuery USE detection, but the side effect IsRouted()=true
// would cause routeplugin.Signer (RouteAware-opt-in) to fire on
// forward-pivoted sessions and overwrite the agent's per-query JWS in
// settings with the relay's JWS. The receiving proxy's auth plugin then
// validated the relay JWS instead of the agent's, recovering the wrong
// account.
//
// IsRouted() must return false for forward-pivoted sessions even though
// RouteTarget is non-empty — they are conceptually "forwarding" not
// "routed".
func TestSessionState_IsRouted_ExcludesForwardPivot(t *testing.T) {
	tests := []struct {
		name        string
		routeTarget string
		forwarding  bool
		want        bool
	}{
		{name: "fresh session", routeTarget: "", forwarding: false, want: false},
		{name: "stripper-routed (real __route__)", routeTarget: "peer:9001", forwarding: false, want: true},
		{name: "forward-pivoted", routeTarget: "peer:9001", forwarding: true, want: false},
		{name: "forwarding without route target", routeTarget: "", forwarding: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSessionState()
			if tt.routeTarget != "" {
				s.SetRouteTarget(tt.routeTarget)
			}
			if tt.forwarding {
				s.SetForwarding(true)
			}
			if got := s.IsRouted(); got != tt.want {
				t.Errorf("IsRouted()=%v want %v (route=%q forwarding=%v)",
					got, tt.want, tt.routeTarget, tt.forwarding)
			}
		})
	}
}

// TestSession_RebindToLocal verifies the remote→local pivot path used
// by forward.Plugin when a USE statement targets a database hosted on
// THIS proxy after a previous USE pivoted the session to a peer.
// Mirrors RebindToPeer but with three differences:
//   - hello carries no peer envelope (regular CH credentials)
//   - PeerServerHelloRaw / PeerRevision are NOT populated (peer state
//     belongs to the peer-rebind code path, not local)
//   - IsForwarding and RouteTarget are CLEARED so the chain stops
//     treating the session as forwarded
func TestSession_RebindToLocal(t *testing.T) {
	const rev = chproto.RevisionMinAddendum

	clientConn, _ := net.Pipe()
	defer clientConn.Close()
	sess := New(1, clientConn)

	placeholder, _ := net.Pipe()
	defer placeholder.Close()
	if err := sess.BindUpstream(context.Background(), chproto.NewCodec(placeholder, chproto.DirToUpstream)); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}

	// Pre-conditions: session was previously forwarded to a peer.
	sess.State().SetForwarding(true)
	sess.State().SetRouteTarget("peer.internal:9001")
	// Also pre-populate replayable state so we can verify RebindToLocal
	// replaces stale logical DB state with the physical DB from the local
	// hello before replaying.
	sess.State().SetDatabase("stale_logical_db")
	sess.State().AddSetting("max_execution_time", chproto.Setting{Key: "max_execution_time", Value: "30"})

	localServer, localClient := net.Pipe()
	newUp := chproto.NewCodec(localClient, chproto.DirToUpstream)

	var captured bytes.Buffer
	var capturedMu sync.Mutex
	helloUser := make(chan string, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer localServer.Close()
		_ = localServer.SetDeadline(time.Now().Add(5 * time.Second))

		srvCodec := chproto.NewCodec(localServer, chproto.DirFromClient)
		pkt, err := srvCodec.ReadPacket(uint64(chproto.ClientHelloCode))
		if err != nil {
			helloUser <- "READ_ERROR: " + err.Error()
			return
		}
		hello := pkt.Decoded.(*chproto.ClientHello)
		helloUser <- hello.User

		srv := &proto.ServerHello{Name: "local-ch", Major: 24, Minor: 1, Revision: rev}
		var buf proto.Buffer
		srv.EncodeAware(&buf, rev)
		if _, err := localServer.Write(buf.Buf); err != nil {
			return
		}

		capturedMu.Lock()
		_, _ = io.Copy(&captured, localServer)
		capturedMu.Unlock()
	}()

	localHello := &chproto.ClientHello{
		Name:            "housegate",
		Major:           24,
		Minor:           1,
		ProtocolVersion: rev,
		User:            "default", // regular CH creds — NO peer envelope
		Password:        "",
		Database:        "physical_db",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sess.RebindToLocal(ctx, newUp, localHello); err != nil {
		t.Fatalf("RebindToLocal: %v", err)
	}

	// Upstream was swapped.
	if sess.Upstream() != newUp {
		t.Fatalf("Upstream() did not swap to newUp")
	}

	// Forward state cleared.
	if sess.State().IsForwarding {
		t.Errorf("IsForwarding still true after RebindToLocal")
	}
	if got := sess.State().GetRouteTarget(); got != "" {
		t.Errorf("RouteTarget=%q after RebindToLocal, want empty", got)
	}
	if got := sess.State().Database; got != "physical_db" {
		t.Errorf("Database=%q after RebindToLocal, want physical_db", got)
	}

	// Peer-specific state must NOT be populated by a local rebind.
	if len(sess.State().PeerServerHelloRaw) != 0 {
		t.Errorf("PeerServerHelloRaw must not be populated by RebindToLocal")
	}

	// Hello must carry the regular user, not a peer envelope.
	gotUser := <-helloUser
	if gotUser != "default" {
		t.Fatalf("local CH received User=%q, want %q", gotUser, "default")
	}

	// Verify RebindToLocal did not emit an internal USE query. The local
	// hello already selected physical_db, and the triggering client USE
	// still flows through the normal query path after rebind; replaying a
	// second USE here can surface an unauthenticated internal query through
	// proxy upstreams.
	localClient.Close()
	<-done
	capturedMu.Lock()
	got := captured.Bytes()
	capturedMu.Unlock()
	if bytes.Contains(got, []byte("USE stale_logical_db")) {
		t.Errorf("Replay used stale logical database; captured=%q", got)
	}
	if bytes.Contains(got, []byte("USE physical_db")) {
		t.Errorf("RebindToLocal must not replay USE physical_db; captured=%q", got)
	}
	if !bytes.Contains(got, []byte("SET max_execution_time=30")) {
		t.Errorf("Replay missing SET max_execution_time; captured=%q", got)
	}
}
