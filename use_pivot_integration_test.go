package housegate

// use_pivot_integration_test.go — end-to-end test for mid-session USE pivot.
//
// Origin: docs/superpowers/specs/2026-04-28-two-port-server-mode.md
//
// Confirms that the forward plugin's OnQuery USE-rebind works end-to-end.
//
// Test scenario:
//  1. A real *builtServer (proxy A, indexer 42) is wired with a
//     NetworkState that maps "tenant2" → indexer 200.
//  2. A fake peer-internal TCP listener stands in for proxy B. It reads
//     the incoming ClientHello, sends a stub ServerHello, then reads and
//     captures any subsequent Query packets until the connection closes.
//  3. A test client connects to A's external listener and sends a
//     ClientHello with Database="" (empty — so OnHello defers).
//  4. The client then sends a USE tenant2 Query packet.
//
// Assertions:
//   - The fake peer is NOT contacted during the initial hello round-trip
//     (premature-pivot guard: no ClientHello arrives in the first 50ms).
//   - After USE is sent, the fake peer receives a ClientHello whose User
//     starts with "__peer__|", proving the peer envelope was constructed.
//   - Password is non-empty (JWS token present).
//   - Database is "tenant2" (preserved in the rebind hello).
//   - The fake peer eventually receives a USE tenant2 query, confirming
//     the relay forwarded the original client query to the peer.

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

// startFakePeerListenerWithQueryCapture starts a TCP listener that acts as a
// peer proxy's internal port. For each accepted connection it:
//  1. Reads the incoming ClientHello (sent by RebindToPeer).
//  2. Writes a minimal ServerHello (revision below RevisionMinAddendum so
//     the addendum exchange is skipped).
//  3. Reads subsequent Query packets until the connection closes, sending
//     each captured body on queryCh.
//  4. Drains remaining bytes (addendum, data blocks, etc.) until close.
//
// The captured ClientHello is sent on helloCh (buffered); each Query body
// is sent on queryCh (buffered).
func startFakePeerListenerWithQueryCapture(
	t *testing.T,
) (addr string, helloCh <-chan chproto.ClientHello, queryCh <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake peer listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	hc := make(chan chproto.ClientHello, 1)
	qc := make(chan string, 8)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go handleFakePeerConnWithQueryCapture(conn, hc, qc)
		}
	}()

	return ln.Addr().String(), hc, qc
}

// handleFakePeerConnWithQueryCapture handles a single connection to the
// extended fake peer listener. It captures the ClientHello and then reads
// Query packets until the connection is closed.
func handleFakePeerConnWithQueryCapture(
	c net.Conn,
	helloCh chan<- chproto.ClientHello,
	queryCh chan<- string,
) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))

	// Step 1: Read the proxy's forwarded ClientHello.
	codec := chproto.NewCodec(c, chproto.DirFromClient)
	pkt, err := codec.ReadPacket(uint64(proto.ClientCodeHello))
	if err != nil {
		return
	}
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		return
	}

	// Send captured hello to the assertion channel (non-blocking).
	select {
	case helloCh <- *hello:
	default:
	}

	// Step 2: Reply with a minimal ServerHello. Revision 54453 is below
	// RevisionMinAddendum (54458) so neither RebindToPeer nor the relay's
	// forwarding path needs to handle an addendum exchange — keeping the
	// fake peer simple.
	const fakeRevision = 54453
	srvHello := &proto.ServerHello{
		Name:     "fake-peer-phase3",
		Major:    24,
		Minor:    1,
		Revision: fakeRevision,
		Timezone: "UTC",
	}
	var buf proto.Buffer
	srvHello.EncodeAware(&buf, fakeRevision)
	if _, err := c.Write(buf.Buf); err != nil {
		return
	}

	// Step 3: Set the revision so the codec's Query decoder works.
	codec.SetRevision(fakeRevision)

	// Step 4: Read Query packets until the connection closes.
	// Relay's Replay() may send USE/SET packets first, followed by the
	// original client USE. We capture all of them so the test can search
	// for the user-sent USE tenant2.
	for {
		// Reset deadline per packet so we don't time out on short waits.
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		qpkt, err := codec.ReadPacket(uint64(proto.ClientCodeQuery))
		if err != nil {
			// io.EOF or deadline — connection done
			return
		}
		if qpkt.Decoded != nil {
			q, ok := qpkt.Decoded.(*chproto.Query)
			if ok {
				select {
				case queryCh <- q.Body:
				default:
				}
			}
		}
	}
}

// TestPhase3UsePivot_USEMidSession_PivotsToFakePeer is the Phase 3
// integration test. A test client connects to proxy A with an empty
// Database (OnHello defers). After the hello round-trip the client sends
// USE tenant2. NetworkState maps tenant2 → indexer 200 → fake peer.
//
// The test asserts:
//   - The fake peer is NOT contacted prematurely (50ms window after hello).
//   - After USE, the fake peer receives a __peer__-envelope ClientHello
//     with a JWS password and Database="tenant2".
//   - The fake peer receives a "USE tenant2" query after the rebind.
func TestPhase3UsePivot_USEMidSession_PivotsToFakePeer(t *testing.T) {
	// Step 1: start the fake upstream (A's local CH stand-in).
	fakeUpstream := startFakeUpstream(t)

	// Step 2: start the extended fake peer listener.
	peerAddr, receivedHelloCh, receivedQueryCh := startFakePeerListenerWithQueryCapture(t)
	peerHost, peerPortStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		t.Fatalf("split peer addr: %v", err)
	}
	peerPort, err := strconv.Atoi(peerPortStr)
	if err != nil {
		t.Fatalf("parse peer port: %v", err)
	}

	// Step 3: build NetworkState — tenant2 hosted on indexer 200, pointing
	// at the fake peer listener.
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
	cfg.InternalListen = "" // external listener only

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
	go func() { _ = externalSL.Server.Serve(ctx, ln) }()

	// Step 6: connect test client.
	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial external port: %v", err)
	}
	defer clientConn.Close()

	// Use revision 54453 — below RevisionMinAddendum (54458) — so no
	// addendum exchange is needed, keeping the test simple. This matches
	// Phase 2's approach.
	const clientRev = 54453

	// Step 7: send ClientHello with EMPTY Database.
	var hb proto.Buffer
	(&proto.ClientHello{
		Name:            "test-client-phase3",
		Major:           1,
		Minor:           0,
		ProtocolVersion: clientRev,
		Database:        "", // empty — OnHello must defer, no peer contact yet
		User:            "alice",
		Password:        "pw",
	}).Encode(&hb)

	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("write ClientHello: %v", err)
	}

	// Step 8: read the ServerHello from A (proxied from A's local upstream).
	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	sink := make([]byte, 512)
	n, err := clientConn.Read(sink)
	if err != nil && err != io.EOF {
		t.Fatalf("read ServerHello from proxy A: %v", err)
	}
	if n == 0 {
		t.Fatal("proxy A sent 0 bytes during initial hello")
	}
	if sink[0] != byte(proto.ServerCodeHello) {
		t.Fatalf("first byte=%d, want ServerCodeHello (0)", sink[0])
	}

	// Step 9: assert the fake peer was NOT contacted during the hello round-trip.
	// Give a 200ms window — if the forward plugin spuriously dialed the peer on
	// empty Database, the ClientHello would arrive here.
	select {
	case h := <-receivedHelloCh:
		t.Fatalf("fake peer received ClientHello prematurely (before USE): User=%q Database=%q", h.User, h.Database)
	case <-time.After(200 * time.Millisecond):
		// Good — no premature pivot. 200ms window accommodates slower CI.
	}

	// Step 10: send USE tenant2 as a Query packet.
	// We encode directly via EncodeQuery at the client revision.
	// FeatureClientWriteInfo.In(54453) is true (>= 54032), so we must
	// provide a ClientInfo; the relay's decoder will reject the packet
	// otherwise. Stage=Complete and Compression=Disabled are the standard
	// defaults for a simple statement.
	var qb proto.Buffer
	chproto.EncodeQuery(&qb, &chproto.Query{
		ID: "use-tenant2-qid",
		Info: chproto.ClientInfo{
			Query:           proto.ClientQueryInitial,
			InitialUser:     "alice",
			InitialQueryID:  "use-tenant2-qid",
			InitialAddress:  "[::ffff:127.0.0.1]:0",
			Interface:       proto.InterfaceTCP,
			OSUser:          "alice",
			ClientHostname:  "test-client-phase3",
			ClientName:      "test-client-phase3",
			Major:           1,
			Minor:           0,
			ProtocolVersion: clientRev,
		},
		Stage:       proto.StageComplete,
		Compression: proto.CompressionDisabled,
		Body:        "USE tenant2",
	}, clientRev)

	_ = clientConn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := clientConn.Write(qb.Buf); err != nil {
		t.Fatalf("write USE query: %v", err)
	}

	// Step 11: assert the fake peer received a __peer__-envelope ClientHello
	// (the rebind handshake triggered by OnQuery).
	var rebindHello chproto.ClientHello
	select {
	case rebindHello = <-receivedHelloCh:
	case <-time.After(5 * time.Second):
		t.Fatal("fake peer did not receive ClientHello after USE within 5s — forward OnQuery may not be wired")
	}

	if !strings.HasPrefix(rebindHello.User, "__peer__|") {
		t.Errorf("rebind hello User = %q; want __peer__|...", rebindHello.User)
	}
	if rebindHello.Password == "" {
		t.Error("rebind hello Password is empty; expected JWS token")
	}
	if rebindHello.Database != "tenant2" {
		t.Errorf("rebind hello Database = %q; want tenant2", rebindHello.Database)
	}

	// Step 12: assert the USE tenant2 query crossed to the peer.
	// Replay may also send queries (e.g. a USE for the prior empty Database,
	// which Replay skips because Database is still ""), so we drain all
	// captured queries and look for the user's USE.
	sawUse := false
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !sawUse {
		select {
		case body := <-receivedQueryCh:
			if strings.EqualFold(strings.TrimSpace(body), "use tenant2") {
				sawUse = true
			}
		case <-deadline.C:
			t.Errorf("fake peer did not receive 'USE tenant2' query within 2s after rebind")
			return
		}
	}
}
