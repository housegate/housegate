package housegate

// internal_port_integration_test.go — Phase 1 end-to-end test.
//
// Confirms that a peer-relay ClientHello (__peer__|<addr> user + relay
// JWS password) sent to the internal-port listener succeeds with a real
// ServerHello round-trip. The credential.Plugin validates the JWS,
// sets IsPeerTrusted=true, and the handshake completes — auth and
// rewrite plugins are bypassed because the session is peer-trusted.
//
// The test:
//   1. Builds a real *builtServer via buildServer (same as production).
//   2. Starts a fake upstream TCP server that just writes a ServerHello.
//   3. Starts the internal-port listener on a free port.
//   4. Dials the internal port and sends a peer-relay ClientHello.
//   5. Asserts the first byte back from the proxy is ServerCodeHello (0).

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/peer"
	authplugin "github.com/housegate/housegate/pkg/plugins/auth"
)

// testRelayKeyHex is a deterministic secp256k1 private key used only in
// tests. Never used in any real deployment.
const testRelayKeyHex = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// minimalServerCfgWithRelayKey returns a server-mode Config with:
//   - RelayPrivateKeyHex set so buildServer wires a RelaySigner + PeerSigner
//   - Auth.Enabled = true, Auth.AllowedAddresses = [signerAddr] so the
//     EthValidator used as PeerValidator accepts the test signer
//   - Both Listen and InternalListen as 127.0.0.1:0 (port assigned by OS)
//   - Upstream pointing at a caller-controlled fake upstream address
func minimalServerCfgWithRelayKey(t *testing.T, fakeUpstreamAddr string) (*config.Config, string) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	cfg := minimalServerCfg(t)
	cfg.Listen = "127.0.0.1:0"
	cfg.InternalListen = "127.0.0.1:0"
	cfg.Upstream = fakeUpstreamAddr
	cfg.IndexerID = 42 // arbitrary non-zero; exercises the production audience-binding path

	cfg.RelayPrivateKeyHex = testRelayKeyHex

	// Auth must be enabled so buildServer constructs an EthValidator,
	// which is also pulled out as the PeerValidator in the credential
	// plugin. AllowedAddresses restricts it to our test signer's address.
	cfg.Auth = authplugin.Config{
		Enabled:          true,
		AllowedAddresses: []string{signer.Address()},
		MaxTokenAge:      cfg.Auth.MaxTokenAge, // 1m from config.Default(); ValidatePeerLogin uses the JWS exp claim, not this field
		AllowNoAuth:      true,                 // queries without a token pass (we test the hello, not queries)
	}

	return cfg, signer.Address()
}

// startFakeUpstream starts a TCP listener that accepts connections in a loop.
// For each connection that sends a valid ClientHello, it writes a minimal
// ServerHello then drains remaining bytes. Connections that send no bytes
// (e.g. health-checker TCP probes) are silently dropped. The accept loop
// exits when the listener is closed via t.Cleanup.
func startFakeUpstream(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake upstream listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))

				// Drain the proxy's forwarded ClientHello before writing ServerHello.
				upCodec := chproto.NewCodec(c, chproto.DirFromClient)
				if _, err := upCodec.ReadPacket(uint64(proto.ClientCodeHello)); err != nil {
					return
				}

				// Reply with a minimal ServerHello.
				srvHello := &proto.ServerHello{
					Name:     "test-upstream",
					Major:    24,
					Minor:    1,
					Revision: 54453,
					Timezone: "UTC",
				}
				var b proto.Buffer
				srvHello.EncodeAware(&b, 54453)
				_, _ = c.Write(b.Buf)

				// Drain the rest so the proxy's write path doesn't block.
				_, _ = io.Copy(io.Discard, c)
			}(conn)
		}
	}()

	return ln.Addr().String()
}

// TestInternalPort_AcceptsPeerTrustedHandshake is the Phase 1 integration
// test.  A peer-style ClientHello sent to the internal-port listener
// must succeed: the credential.Plugin validates the peer-relay JWS,
// marks the session IsPeerTrusted, and the proxy completes the
// ServerHello round-trip.
func TestInternalPort_AcceptsPeerTrustedHandshake(t *testing.T) {
	fakeUpstream := startFakeUpstream(t)

	cfg, _ := minimalServerCfgWithRelayKey(t, fakeUpstream)

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
		GetIndexerId: func() uint64 { return cfg.IndexerID },
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	// Find the internal listener.
	var internalSL *serverListener
	for i := range bs.listeners {
		if bs.listeners[i].Label == "internal" {
			internalSL = &bs.listeners[i]
			break
		}
	}
	if internalSL == nil {
		t.Fatal("buildServer returned no internal listener")
	}

	// Start the internal listener on a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := bs.preServe(ctx); err != nil {
		t.Fatalf("preServe: %v", err)
	}
	internalServer := requireProxyServer(t, *internalSL)
	go func() { _ = internalServer.Serve(ctx, ln) }()

	// Build the peer-relay JWS. We use the same key that buildServer
	// wired as the relay signer — the validator accepts its own key.
	peerSigner, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("peer signer: %v", err)
	}

	// Sign with the audience matching cfg.IndexerID so the credential
	// plugin's audience enforcement is exercised on the production path
	// (GetSelfIndexerID returns 42, expectedAud = "42").
	audience := strconv.FormatUint(cfg.IndexerID, 10)
	token, err := peerSigner.SignPeerLogin(audience, 30*time.Second)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}

	// Dial the internal port.
	clientConn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial internal port: %v", err)
	}
	defer clientConn.Close()

	// Send a peer-relay ClientHello.
	// user  = "__peer__|<signerAddr>"
	// password = peer-relay JWS
	var hb proto.Buffer
	(&proto.ClientHello{
		Name:            "test-peer-client",
		Major:           1,
		Minor:           0,
		ProtocolVersion: 54453,
		Database:        "default",
		User:            peer.FormatUser(peerSigner.Address()),
		Password:        token,
	}).Encode(&hb)

	if _, err := clientConn.Write(hb.Buf); err != nil {
		t.Fatalf("write ClientHello: %v", err)
	}

	// Expect a ServerHello back. First byte must be ServerCodeHello (0).
	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	sink := make([]byte, 512)
	n, err := clientConn.Read(sink)
	if err != nil && err != io.EOF {
		t.Fatalf("read ServerHello: %v", err)
	}
	if n == 0 {
		t.Fatalf("proxy sent 0 bytes — handshake did not complete")
	}
	if sink[0] != byte(proto.ServerCodeHello) {
		t.Fatalf("first byte=%d, want ServerCodeHello (0) — proxy may have rejected the peer hello", sink[0])
	}
}
