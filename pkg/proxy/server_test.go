package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// TestServer_HandshakeRoundTrip proves the Server accepts a client, dials
// upstream via the provided dialer, runs the handshake, and forwards the
// ServerHello back to the client.
func TestServer_HandshakeRoundTrip(t *testing.T) {
	// Fake upstream: a net.Pipe half we hand to the dialer.
	upstreamProxy, proxyUpstream := net.Pipe()
	defer upstreamProxy.Close()

	dial := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
		return chproto.NewCodec(proxyUpstream, chproto.DirToUpstream), nil
	}

	srv := NewServer(plugin.NoopHooks{}, dial)

	// Listen on an ephemeral TCP port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Serve(ctx, ln) }()

	// Fake upstream goroutine: drain ClientHello, write ServerHello.
	go func() {
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
		drain := make([]byte, 4096)
		_, _ = upstreamProxy.Read(drain)
		srvHello := &proto.ServerHello{
			Name: "test-server", Major: 24, Minor: 1,
			Revision: 54453, Timezone: "UTC", DisplayName: "test",
		}
		var b proto.Buffer
		srvHello.EncodeAware(&b, 54453)
		_, _ = upstreamProxy.Write(b.Buf)
	}()

	// Client side: connect to the listener, send ClientHello, read ServerHello.
	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	var hb proto.Buffer
	(&proto.ClientHello{
		Name: "c", Major: 1, ProtocolVersion: 54453,
		Database: "default", User: "alice",
	}).Encode(&hb)
	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("client write hello: %v", err)
	}

	// Read ServerHello back.
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	sink := make([]byte, 512)
	n, err := clientConn.Read(sink)
	if err != nil && err != io.EOF {
		t.Fatalf("client read: %v", err)
	}
	if n == 0 {
		t.Fatalf("client received no bytes from server")
	}
	// First byte should be ServerHello type (0).
	if sink[0] != byte(proto.ServerCodeHello) {
		t.Fatalf("first byte=%d, want ServerCodeHello (0)", sink[0])
	}
}

// recordingHooks captures OnConnect / OnDisconnect invocations so tests
// can assert the Server fires them regardless of dial outcome.
type recordingHooks struct {
	plugin.NoopHooks
	connects     int
	disconnects  int
	connectCh    chan struct{}
	disconnectCh chan struct{}
}

func (r *recordingHooks) OnConnect(_ context.Context, _ chsession.Session) error {
	r.connects++
	closeOnce(r.connectCh)
	return nil
}

func (r *recordingHooks) OnDisconnect(_ chsession.Session) {
	r.disconnects++
	closeOnce(r.disconnectCh)
}

func closeOnce(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// TestServer_ConnLifecycleHooks_FireOnDialFailure proves that
// OnConnect / OnDisconnect are invoked for every accepted connection
// even when the upstream dial fails — the whole reason these hooks are
// separate from OnClose (which only fires after BindUpstream succeeds).
//
// Dial now runs inside Relay.handshake (post-OnHello) rather than at
// connection-accept time, so the test sends a real ClientHello before
// the failing dialer is called. OnConnect still fires at accept; the
// deferred OnDisconnect still fires after Relay returns.
func TestServer_ConnLifecycleHooks_FireOnDialFailure(t *testing.T) {
	hooks := &recordingHooks{
		connectCh:    make(chan struct{}),
		disconnectCh: make(chan struct{}),
	}
	dial := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
		return nil, errors.New("dial-failure")
	}

	srv := NewServer(hooks, dial)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan struct{})
	go func() { _ = srv.Serve(ctx, ln); close(serveDone) }()

	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer clientConn.Close()

	// Send a valid ClientHello so the relay reaches the dialer.
	// Without this, the relay would block in ReadPacket and the test
	// would still pass — but for the wrong reason (timeout, not dial
	// failure). The hello body matches what TestServer_HandshakeRoundTrip
	// uses.
	var hb proto.Buffer
	(&proto.ClientHello{
		Name: "c", Major: 1, ProtocolVersion: 54453,
		Database: "default", User: "alice",
	}).Encode(&hb)
	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("client write hello: %v", err)
	}

	// Wait until the failing dial path has returned through the connection
	// lifecycle frame. Cancelling before Accept/handle is scheduled makes the
	// test assert server shutdown timing instead of lifecycle hooks.
	select {
	case <-hooks.disconnectCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnDisconnect did not fire after dial failure")
	}

	cancel()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not drain in time")
	}

	if hooks.connects != 1 {
		t.Errorf("connects=%d, want 1", hooks.connects)
	}
	if hooks.disconnects != 1 {
		t.Errorf("disconnects=%d, want 1", hooks.disconnects)
	}
}

// testHooks is a minimal plugin.Hooks implementation for tests that need to
// intercept specific hook calls. Unset callbacks default to no-ops.
type testHooks struct {
	onConnect func(context.Context, chsession.Session) error
	onHello   func(context.Context, chsession.Session, *chproto.ClientHello) error
}

var _ plugin.Hooks = testHooks{}

func (h testHooks) OnConnect(ctx context.Context, sess chsession.Session) error {
	if h.onConnect != nil {
		return h.onConnect(ctx, sess)
	}
	return nil
}

func (h testHooks) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	if h.onHello != nil {
		return h.onHello(ctx, sess, hello)
	}
	return nil
}

func (testHooks) OnHandshakeComplete(context.Context, chsession.Session, time.Duration) {}
func (testHooks) OnQuery(context.Context, *plugin.QueryContext) error                   { return nil }
func (testHooks) RejectUndecodableQuery(chsession.Session) bool                         { return false }
func (testHooks) ClientDataReadLimit(*plugin.QueryContext) (uint64, bool)               { return 0, false }
func (testHooks) OnClientDataStrict(context.Context, *plugin.QueryContext, []byte) error {
	return nil
}
func (testHooks) OnClientData(context.Context, *plugin.QueryContext, []byte) error { return nil }
func (testHooks) OnException(context.Context, chsession.Session, *chproto.Exception) error {
	return nil
}
func (testHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	return nil
}
func (testHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {}
func (testHooks) OnQueryAbort(context.Context, *plugin.QueryContext)         {}
func (testHooks) OnQueryComplete(context.Context, chsession.Session)         {}
func (testHooks) OnClose(chsession.Session)                                  {}
func (testHooks) OnDisconnect(chsession.Session)                             {}

// nopDialer returns a codec backed by one end of a net.Pipe; a goroutine on
// the other end drains the ClientHello and writes a minimal ServerHello so
// the handshake round-trip completes.
func nopDialer(_ context.Context, _ chsession.Session) (*chproto.Codec, error) {
	proxyEnd, peerEnd := net.Pipe()
	go func() {
		defer peerEnd.Close()
		_ = peerEnd.SetDeadline(time.Now().Add(2 * time.Second))
		drain := make([]byte, 4096)
		_, _ = peerEnd.Read(drain)
		srvHello := &proto.ServerHello{
			Name: "nop-upstream", Major: 24, Minor: 1,
			Revision: 54453, Timezone: "UTC",
		}
		var b proto.Buffer
		srvHello.EncodeAware(&b, 54453)
		_, _ = peerEnd.Write(b.Buf)
		// drain remaining bytes so the pipe doesn't block
		_, _ = io.Copy(io.Discard, peerEnd)
	}()
	return chproto.NewCodec(proxyEnd, chproto.DirToUpstream), nil
}

// runOneClientHello drives one ClientHello → ServerHello round-trip against s
// using a real TCP listener. Returns when the client receives the ServerHello
// or the test fails.
func runOneClientHello(t *testing.T, s *Server) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Serve(ctx, ln) }()

	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	var hb proto.Buffer
	(&proto.ClientHello{
		Name: "c", Major: 1, ProtocolVersion: 54453,
		Database: "default", User: "alice",
	}).Encode(&hb)
	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("client write hello: %v", err)
	}

	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	sink := make([]byte, 512)
	n, err := clientConn.Read(sink)
	if err != nil && err != io.EOF {
		t.Fatalf("client read: %v", err)
	}
	if n == 0 {
		t.Fatalf("client received no bytes from server")
	}
}

// TestServer_GracefulDrain verifies that after ctx is cancelled, Serve waits
// for in-flight sessions to finish (up to ShutdownTimeout) before returning,
// and returns ctx.Err().
//
// Test plan:
//  1. Construct a Server with ShutdownTimeout = 500ms and a blocking UpstreamDialer
//     that holds the session for handlerDelay (200ms) by simply sleeping before
//     returning an error (so the goroutine launched by handle runs for ~200ms).
//  2. Connect a client so handle() is invoked and the wg count rises to 1.
//  3. Cancel ctx — this should close the listener, unblock Accept, and trigger
//     waitForDrain.
//  4. Serve must return within ~300ms (200ms session + margin) with ctx.Err().
//  5. The done channel written by handle proves the in-flight session completed.
func TestServer_GracefulDrain(t *testing.T) {
	const handlerDelay = 200 * time.Millisecond
	const shutdownTimeout = 500 * time.Millisecond

	// done is closed by the fake handle goroutine when it exits.
	handlerFinished := make(chan struct{})

	// A dialer that blocks for handlerDelay then fails — this gives us a
	// realistic in-flight goroutine without needing a full protocol stack.
	dial := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
		select {
		case <-time.After(handlerDelay):
		case <-ctx.Done():
		}
		close(handlerFinished)
		return nil, errors.New("fake-dial-failure") // causes handle() to return
	}

	srv := &Server{
		Hooks:           plugin.NoopHooks{},
		UpstreamDialer:  dial,
		ShutdownTimeout: shutdownTimeout,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, ln) }()

	// Connect a client and send a ClientHello so handle() reaches the
	// dialer (post-OnHello). Without the hello the relay would block
	// in ReadPacket and never call the dialer, so handlerFinished
	// would never close.
	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer clientConn.Close()

	var hb proto.Buffer
	(&proto.ClientHello{
		Name: "c", Major: 1, ProtocolVersion: 54453,
		Database: "default", User: "alice",
	}).Encode(&hb)
	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("client write hello: %v", err)
	}

	// Give the goroutine time to reach the dialer.
	time.Sleep(20 * time.Millisecond)

	// Cancel ctx — should trigger drain.
	cancel()

	deadline := time.NewTimer(handlerDelay + 300*time.Millisecond)
	defer deadline.Stop()

	select {
	case err := <-serveErr:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Serve returned wrong error: got %v, want context.Canceled", err)
		}
	case <-deadline.C:
		t.Fatal("Serve did not return within expected drain window")
	}

	// The in-flight handler must have completed (wg.Wait returned).
	select {
	case <-handlerFinished:
		// expected
	default:
		t.Error("in-flight handler had not finished when Serve returned")
	}
}

func TestServer_PreflagSession_RunsBeforeOnConnectAndOnHello(t *testing.T) {
	var preflagCalled, connectCalled, helloCalled bool
	var counter atomic.Int64
	var preflagOrder, connectOrder, helloOrder atomic.Int64

	s := &Server{
		Hooks: testHooks{
			onConnect: func(ctx context.Context, sess chsession.Session) error {
				connectOrder.Store(counter.Add(1))
				connectCalled = true
				return nil
			},
			onHello: func(ctx context.Context, sess chsession.Session, _ *chproto.ClientHello) error {
				helloOrder.Store(counter.Add(1))
				helloCalled = true
				if !sess.State().IsInternalPort {
					t.Errorf("preflag did not set IsInternalPort before OnHello")
				}
				return nil
			},
		},
		UpstreamDialer: nopDialer,
		PreflagSession: func(st *chsession.SessionState) {
			preflagOrder.Store(counter.Add(1))
			preflagCalled = true
			st.IsInternalPort = true
		},
	}

	runOneClientHello(t, s)

	if !preflagCalled || !connectCalled || !helloCalled {
		t.Fatalf("preflag=%v connect=%v hello=%v", preflagCalled, connectCalled, helloCalled)
	}
	if !(preflagOrder.Load() < connectOrder.Load() && connectOrder.Load() < helloOrder.Load()) {
		t.Fatalf("want preflag < connect < hello; got preflag=%d connect=%d hello=%d",
			preflagOrder.Load(), connectOrder.Load(), helloOrder.Load())
	}
}
