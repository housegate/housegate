package housegate

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/endpointtls/endpointtlstest"
)

const tlsTestAgentKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// acceptHandshakes reports, per accepted connection, whether its TLS
// handshake completed (nil) or failed.
func acceptHandshakes(t *testing.T, ln net.Listener) <-chan error {
	t.Helper()
	out := make(chan error, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			tc := c.(*tls.Conn)
			_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
			out <- tc.Handshake()
			_ = c.Close()
		}
	}()
	return out
}

func awaitHandshake(t *testing.T, got <-chan error, wantOK bool) {
	t.Helper()
	select {
	case err := <-got:
		if wantOK && err != nil {
			t.Fatalf("server handshake: %v", err)
		}
		if !wantOK && err == nil {
			t.Fatal("server handshake completed for a plaintext client")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the TLS listener was not dialed")
	}
}

func requireTLSCodec(t *testing.T, codec *chproto.Codec, wantAddr string) {
	t.Helper()
	c, ok := codec.Conn().(*configuredAddressConn)
	if !ok {
		t.Fatalf("codec conn = %T, want *configuredAddressConn", codec.Conn())
	}
	t.Cleanup(func() { _ = c.Close() })
	if _, ok := c.Conn.(*tls.Conn); !ok {
		t.Fatalf("upstream conn = %T, want *tls.Conn", c.Conn)
	}
	if c.UpstreamAddress() != wantAddr {
		t.Fatalf("UpstreamAddress = %q, want %q", c.UpstreamAddress(), wantAddr)
	}
}

// localhostAddr is ln's port on the name its certificate covers.
func localhostAddr(t *testing.T, ln net.Listener) (string, uint16) {
	t.Helper()
	_, p, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var port uint16
	if _, err := fmt.Sscan(p, &port); err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("localhost", p), port
}

// networkStateYAML writes a YAML network state with one indexer at
// localhost:port.
func networkStateYAML(t *testing.T, port uint16) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "network_state.yaml")
	body := fmt.Sprintf("indexer_infos:\n  7:\n    indexer_id: 7\n    indexer_url: \"localhost\"\n    clickhouse_proxy_port: %d\n", port)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func tlsOn() *bool { v := true; return &v }

func TestAgentPinnedUpstreamTLS(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	handshakes := acceptHandshakes(t, ln)
	addr, _ := localhostAddr(t, ln)

	cfg := config.Default()
	cfg.Agent.Mode = true
	cfg.Agent.PrivateKeyHex = tlsTestAgentKey
	cfg.Agent.Upstream = addr
	cfg.Agent.UpstreamTLS = true
	cfg.NetworkState.TLSCAFile = ca.File
	dialer, err := buildAgentDialer(Options{Config: &cfg}, nil, "0xaa", "0xaa", nil)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := dialer(context.Background(), nil)
	if err != nil {
		t.Fatalf("pinned TLS dial: %v", err)
	}
	requireTLSCodec(t, codec, addr)
	awaitHandshake(t, handshakes, true)

	// Without upstream_tls the pinned dial stays plaintext, as before.
	cfg.Agent.UpstreamTLS = false
	cfg.NetworkState.TLSCAFile = ""
	dialer, err = buildAgentDialer(Options{Config: &cfg}, nil, "0xaa", "0xaa", nil)
	if err != nil {
		t.Fatal(err)
	}
	codec, err = dialer(context.Background(), nil)
	if err != nil {
		t.Fatalf("pinned plaintext dial: %v", err)
	}
	c := codec.Conn().(*configuredAddressConn)
	if _, ok := c.Conn.(*tls.Conn); ok {
		t.Fatal("a pinned upstream without upstream_tls was dialed over TLS")
	}
	_, _ = c.Write([]byte("plaintext hello"))
	_ = c.Close()
	awaitHandshake(t, handshakes, false)
}

func TestAgentDiscoveredUpstreamFollowsEndpointTLS(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	handshakes := acceptHandshakes(t, ln)
	addr, port := localhostAddr(t, ln)

	cfg := config.Default()
	cfg.Agent.Mode = true
	cfg.Agent.PrivateKeyHex = tlsTestAgentKey
	cfg.NetworkState.Source = networkStateYAML(t, port)
	cfg.NetworkState.EndpointTLS = tlsOn()
	cfg.NetworkState.TLSCAFile = ca.File
	dialer, err := buildAgentDialer(Options{Config: &cfg}, nil, "0xaa", "0xaa", nil)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := dialer(context.Background(), nil)
	if err != nil {
		t.Fatalf("discovered TLS dial: %v", err)
	}
	requireTLSCodec(t, codec, addr)
	awaitHandshake(t, handshakes, true)

	// The redial a completion probe uses keeps TLS and the server name.
	again, err := codec.Conn().(*configuredAddressConn).RedialExact(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("RedialExact: %v", err)
	}
	t.Cleanup(func() { _ = again.Close() })
	if _, ok := again.(*configuredAddressConn).Conn.(*tls.Conn); !ok {
		t.Fatal("redial lost TLS")
	}
	awaitHandshake(t, handshakes, true)

	// An endpoint whose certificate the agent cannot verify is refused.
	cfg.NetworkState.TLSCAFile = ""
	dialer, err = buildAgentDialer(Options{Config: &cfg}, nil, "0xaa", "0xaa", nil)
	if err != nil {
		t.Fatal(err)
	}
	if codec, err := dialer(context.Background(), nil); err == nil {
		_ = codec.Conn().(net.Conn).Close()
		t.Fatal("dial verified a certificate from an unknown CA")
	}
}

func TestRouterOnlyServerDialsPeersWithEndpointTLS(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	handshakes := acceptHandshakes(t, ln)
	addr, port := localhostAddr(t, ln)

	cfg := minimalRouterOnlyCfg(t)
	cfg.NetworkState.Source = networkStateYAML(t, port)
	cfg.NetworkState.EndpointTLS = tlsOn()
	cfg.NetworkState.TLSCAFile = ca.File
	bs, err := buildServer(Options{Config: cfg}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(bs.teardown)
	srv := requireProxyServer(t, bs.listeners[0])

	client, other := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = other.Close() })

	// Router-only pick.
	codec, err := srv.UpstreamDialer(context.Background(), chsession.New(1, client))
	if err != nil {
		t.Fatalf("router-only TLS dial: %v", err)
	}
	requireTLSCodec(t, codec, addr)
	awaitHandshake(t, handshakes, true)

	// A route target (forward pivot or remote() route) is an advertised
	// address too.
	sess := chsession.New(2, client)
	sess.State().SetRouteTarget(addr)
	codec, err = srv.UpstreamDialer(context.Background(), sess)
	if err != nil {
		t.Fatalf("route target TLS dial: %v", err)
	}
	requireTLSCodec(t, codec, addr)
	awaitHandshake(t, handshakes, true)
}
