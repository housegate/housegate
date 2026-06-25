package housegate

// pivot_integration_test.go — end-to-end test for ClientHello-time peer pivot.
//
// Origin: docs/superpowers/specs/2026-04-28-two-port-server-mode.md
//
// Confirms that the forward plugin pivots a session to a peer proxy at
// ClientHello time when the requested database is hosted on a different
// indexer. Specifically:
//
//  1. A real *builtServer (proxy A, indexer 42) is wired with a
//     NetworkState that maps "tenant2" → indexer 200.
//  2. A fake peer-internal TCP listener stands in for proxy B. It reads
//     the incoming ClientHello, sends a stub ServerHello, then drains
//     the rest of the connection.
//  3. A test client connects to A's external listener and sends a
//     ClientHello with Database="tenant2".
//
// Assertions:
//   - The fake peer received a ClientHello whose User starts with
//     "__peer__|", proving the peer envelope was constructed.
//   - Password is non-empty (JWS token present).
//   - Database is "tenant2" (preserved across the rebind).
//   - The test client received the first byte ServerCodeHello (0), proving
//     A completed the handshake with the fake peer and forwarded the result.

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/network"
)

// startFakePeerListener starts a TCP listener that acts as a peer proxy's
// internal port. For each accepted connection it:
//  1. Reads the incoming ClientHello (sent by RebindToPeer).
//  2. Writes a minimal ServerHello (revision below RevisionMinAddendum so
//     the addendum exchange is skipped, keeping the fake simple).
//  3. Drains remaining bytes (addendum, replayed settings, etc.) until the
//     connection closes.
//
// The captured ClientHello is sent on receivedCh (buffered to avoid blocking
// the goroutine). The listener is closed via t.Cleanup.
func startFakePeerListener(t *testing.T) (addr string, receivedCh <-chan chproto.ClientHello) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake peer listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ch := make(chan chproto.ClientHello, 1)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go handleFakePeerConn(conn, ch)
		}
	}()

	return ln.Addr().String(), ch
}

// handleFakePeerConn handles a single connection to the fake peer listener.
func handleFakePeerConn(c net.Conn, out chan<- chproto.ClientHello) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))

	// Read the proxy's forwarded ClientHello.
	codec := chproto.NewCodec(c, chproto.DirFromClient)
	pkt, err := codec.ReadPacket(uint64(proto.ClientCodeHello))
	if err != nil {
		return
	}
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		return
	}

	// Send captured hello to the assertion channel (non-blocking because it's
	// buffered; a second connection would drop silently, which is fine for this
	// test).
	select {
	case out <- *hello:
	default:
	}

	// Reply with a minimal ServerHello. Use revision 54453 — below
	// RevisionMinAddendum (54458) — so RebindToPeer skips the addendum
	// exchange and the fake peer does not have to handle it.
	const fakeRevision = 54453
	srvHello := &proto.ServerHello{
		Name:     "fake-peer",
		Major:    24,
		Minor:    1,
		Revision: fakeRevision,
		Timezone: "UTC",
	}
	var buf proto.Buffer
	srvHello.EncodeAware(&buf, fakeRevision)
	_, _ = c.Write(buf.Buf)

	// Drain remaining bytes so the proxy's write path doesn't block.
	_, _ = io.Copy(io.Discard, c)
}

// TestPhase2Pivot_ExternalHello_PivotsToFakePeer is the Phase 2 integration
// test. It drives a real buildServer (proxy A, indexer 42) through a
// ClientHello with Database="tenant2". NetworkState says tenant2 lives on
// indexer 200, whose ClickhouseProxyPort points at a fake TCP listener. The
// test asserts:
//   - The fake peer received a __peer__-envelope ClientHello.
//   - The test client got a ServerHello back (byte 0).
func TestPhase2Pivot_ExternalHello_PivotsToFakePeer(t *testing.T) {
	// Step 1: start the fake upstream (for A's local CH — required by server
	// mode config, though the forward path bypasses it for tenant2).
	fakeUpstream := startFakeUpstream(t)

	// Step 2: start the fake peer-internal listener that stands in for proxy B.
	peerAddr, receivedCh := startFakePeerListener(t)
	peerHost, peerPortStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		t.Fatalf("split peer addr: %v", err)
	}
	peerPort, err := strconv.Atoi(peerPortStr)
	if err != nil {
		t.Fatalf("parse peer port: %v", err)
	}

	// Step 3: build NetworkState with tenant2 → indexer 200 → fake peer.
	ns := network.NewInMemoryNetworkState()
	ns.DatabaseInfos[network.Database("tenant2")] = network.DatabaseInfo{
		DatabaseId: "tenant2",
		IndexerId:  200,
	}
	ns.IndexerInfos[200] = network.IndexerInfo{
		IndexerId:           200,
		IndexerUrl:          peerHost,
		ClickhouseProxyPort: uint16(peerPort),
	}

	// Step 4: build proxy A (indexer 42; tenant2 is NOT ours).
	cfg, _ := minimalServerCfgWithRelayKey(t, fakeUpstream)
	cfg.IndexerID = 42
	cfg.InternalListen = "" // external listener only for this test

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: ns,
		GetIndexerId: func() uint64 { return 42 },
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	// Find the external listener.
	var externalSL *serverListener
	for i := range bs.listeners {
		if bs.listeners[i].Label == "external" {
			externalSL = &bs.listeners[i]
			break
		}
	}
	if externalSL == nil {
		t.Fatal("buildServer returned no external listener")
	}

	// Step 5: bind a real TCP listener and start serving.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bs.preServe(ctx)
	externalServer := requireProxyServer(t, *externalSL)
	go func() { _ = externalServer.Serve(ctx, ln) }()

	// Step 6: connect test client and send a ClientHello with Database="tenant2".
	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial external port: %v", err)
	}
	defer clientConn.Close()

	var hb proto.Buffer
	(&proto.ClientHello{
		Name:            "test-client",
		Major:           1,
		Minor:           0,
		ProtocolVersion: 54453, // below RevisionMinAddendum to keep test simple
		Database:        "tenant2",
		User:            "alice",
		Password:        "pw",
	}).Encode(&hb)

	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("write ClientHello: %v", err)
	}

	// Step 7: assert the fake peer received the __peer__-envelope ClientHello.
	var received chproto.ClientHello
	select {
	case received = <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("fake peer did not receive ClientHello within 5s — forward plugin may not have dialed the peer")
	}

	if !strings.HasPrefix(received.User, "__peer__|") {
		t.Errorf("peer hello User = %q; want prefix __peer__|...", received.User)
	}
	if received.Password == "" {
		t.Error("peer hello Password is empty; expected a JWS token")
	}
	if received.Database != "tenant2" {
		t.Errorf("peer hello Database = %q; want tenant2 (original database must be preserved)", received.Database)
	}

	// Step 8: assert the test client received a ServerHello from A (which
	// relayed the fake peer's response). First byte must be ServerCodeHello (0).
	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	sink := make([]byte, 512)
	n, err := clientConn.Read(sink)
	if err != nil && err != io.EOF {
		t.Fatalf("read ServerHello from proxy A: %v", err)
	}
	if n == 0 {
		t.Fatal("proxy A sent 0 bytes — handshake did not complete")
	}
	if sink[0] != byte(proto.ServerCodeHello) {
		t.Fatalf("first byte=%d, want ServerCodeHello (0) — A may not have forwarded the peer's ServerHello", sink[0])
	}
}
