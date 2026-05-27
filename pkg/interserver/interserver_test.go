package interserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// startGateway runs an interserver gateway in front of target on an
// ephemeral port and returns its address.
func startGateway(t *testing.T, ctx context.Context, target string, allow ...*net.IPNet) (*Server, string) {
	t.Helper()
	srv, err := NewServer(ServerConfig{
		Target:     func() string { return target },
		AllowCIDRs: allow,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = srv.Serve(ctx, ln) }()
	return srv, addr
}

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("parse CIDR %q: %v", s, err)
	}
	return n
}

// TestGatewayRelaysHTTP proves the L4 relay carries a full HTTP
// request/response between a client and a backend (a stand-in for the
// local CH interserver, which speaks HTTP).
func TestGatewayRelaysHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const body = "BACKEND-PART-DATA"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer backend.Close()
	backendAddr := backend.Listener.Addr().String()

	srv, gw := startGateway(t, ctx, backendAddr)

	resp, err := http.Get("http://" + gw + "/")
	if err != nil {
		t.Fatalf("GET via gateway: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != body {
		t.Fatalf("body via gateway = %q, want %q", got, body)
	}

	if srv.Served() < 1 {
		t.Errorf("Served = %d, want >= 1", srv.Served())
	}
	if _, down := srv.Bytes(); down == 0 {
		t.Errorf("bytesDown = 0, want > 0 (response should have flowed back through the gateway)")
	}
}

// TestGatewayAllowlistAllows lets a loopback source through when the
// allowlist includes it.
func TestGatewayAllowlistAllows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	_, gw := startGateway(t, ctx, backend.Listener.Addr().String(), mustCIDR(t, "127.0.0.0/8"))

	resp, err := http.Get("http://" + gw + "/")
	if err != nil {
		t.Fatalf("GET via allowlisted gateway: %v", err)
	}
	resp.Body.Close()
}

// TestGatewayAllowlistRejects drops a source not covered by the allowlist
// before any bytes reach the backend.
func TestGatewayAllowlistRejects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var backendHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendHits++
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	// 10.0.0.0/8 excludes the loopback client, so the gateway must refuse.
	srv, gw := startGateway(t, ctx, backend.Listener.Addr().String(), mustCIDR(t, "10.0.0.0/8"))

	c, err := net.DialTimeout("tcp", gw, 2*time.Second)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer c.Close()
	// The gateway closes the connection without forwarding. A read returns
	// EOF (or the write is followed by an immediate close).
	_, _ = c.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	n, rerr := c.Read(buf)
	if rerr == nil && n > 0 {
		t.Fatalf("expected rejected connection (EOF), got %d bytes: %q", n, buf[:n])
	}
	// Give any (erroneously) forwarded request a moment to land.
	time.Sleep(100 * time.Millisecond)
	if backendHits != 0 {
		t.Errorf("backend received %d hits, want 0 (gateway should reject before forwarding)", backendHits)
	}
	if srv.Rejected() < 1 {
		t.Errorf("Rejected = %d, want >= 1", srv.Rejected())
	}
}

// TestGatewayNoTarget drops the connection cleanly when no target resolves.
func TestGatewayNoTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := NewServer(ServerConfig{Target: func() string { return "" }})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = srv.Serve(ctx, ln) }()

	c, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	if n, rerr := c.Read(buf); rerr == nil && n > 0 {
		t.Fatalf("expected closed connection, got %d bytes", n)
	}
}
