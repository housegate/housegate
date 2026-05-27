package integration

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/interserver"
)

// TestInterserverProxy exercises pkg/interserver (link B of the keeper-pool
// design) against a real clickhouse-server:25.8 interserver listener. It
// pins the gateway's behaviour: it relays the real CH interserver HTTP
// surface, and its IP allowlist refuses sources outside the configured
// CIDRs before any bytes reach CH. (End-to-end part replication through two
// gateways is the docker testbed's domain; here we keep to the gateway's
// own contract against a real interserver port.)
func TestInterserverProxy(t *testing.T) {
	chInterserver := testenv.StartClickHouseInterserver(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("RelaysToRealCHInterserver", func(t *testing.T) {
		srv, err := interserver.NewServer(interserver.ServerConfig{
			Target: func() string { return chInterserver },
		})
		if err != nil {
			t.Fatalf("interserver.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("gateway listen: %v", err)
		}
		go func() { _ = srv.Serve(ctx, ln) }()

		resp := httpGetRaw(t, ln.Addr().String())
		if !strings.HasPrefix(resp, "HTTP/") {
			t.Fatalf("expected an HTTP response relayed from CH interserver, got %q", resp)
		}
		if srv.Served() < 1 {
			t.Errorf("Served = %d, want >= 1", srv.Served())
		}
		if _, down := srv.Bytes(); down == 0 {
			t.Errorf("bytesDown = 0, want > 0 (CH response should flow back through the gateway)")
		}
	})

	t.Run("AllowlistRejectsForeignSource", func(t *testing.T) {
		_, allowOnly10, _ := net.ParseCIDR("10.0.0.0/8") // excludes loopback
		srv, err := interserver.NewServer(interserver.ServerConfig{
			Target:     func() string { return chInterserver },
			AllowCIDRs: []*net.IPNet{allowOnly10},
		})
		if err != nil {
			t.Fatalf("interserver.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("gateway listen: %v", err)
		}
		go func() { _ = srv.Serve(ctx, ln) }()

		c, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial gateway: %v", err)
		}
		defer c.Close()
		_, _ = c.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 16)
		if n, rerr := c.Read(buf); rerr == nil && n > 0 {
			t.Fatalf("expected rejected connection (EOF), got %d bytes: %q", n, buf[:n])
		}
		if srv.Rejected() < 1 {
			t.Errorf("Rejected = %d, want >= 1", srv.Rejected())
		}
	})
}

// httpGetRaw sends a minimal HTTP/1.0 GET over a fresh connection to addr
// and returns whatever bytes come back (the relayed CH interserver reply).
func httpGetRaw(t *testing.T, addr string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte("GET / HTTP/1.0\r\nHost: housegate-test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	b, err := io.ReadAll(c)
	if err != nil && len(b) == 0 {
		t.Fatalf("read response: %v", err)
	}
	return string(b)
}
