package endpointtls_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/endpointtls"
	"github.com/housegate/housegate/pkg/endpointtls/endpointtlstest"
)

// serveEcho accepts one connection at a time and echoes what it reads,
// completing the TLS handshake on accept.
func serveEcho(t *testing.T, ln net.Listener) {
	t.Helper()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
}

func roundTrip(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("read = %q, %v", buf, err)
	}
}

func port(t *testing.T, ln net.Listener) string {
	t.Helper()
	_, p, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNilConfigDialsPlaintext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	serveEcho(t, ln)

	var cfg *endpointtls.Config
	if cfg.Enabled() {
		t.Fatal("nil config reports TLS")
	}
	c, err := cfg.DialConn(context.Background(), ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, ok := c.Conn.(*tls.Conn); ok {
		t.Fatal("nil config dialed TLS")
	}
	if c.UpstreamAddress() != ln.Addr().String() {
		t.Fatalf("UpstreamAddress = %q", c.UpstreamAddress())
	}
	roundTrip(t, c)
}

func TestDialVerifiesAgainstCAFileWithHostAsServerName(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	serveEcho(t, ln)

	cfg, err := endpointtls.New(ca.File)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	addr := net.JoinHostPort("localhost", port(t, ln))
	c, err := cfg.DialConn(context.Background(), addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	tc, ok := c.Conn.(*tls.Conn)
	if !ok {
		t.Fatalf("conn = %T, want *tls.Conn", c.Conn)
	}
	if got := tc.ConnectionState().ServerName; got != "localhost" {
		t.Fatalf("ServerName = %q, want localhost", got)
	}
	roundTrip(t, c)

	// A redial reaches the same resolved server and verifies it under the
	// configured name, not the IP it resolved to (the cert has no IP SAN).
	again, err := c.RedialExact(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("RedialExact: %v", err)
	}
	defer again.Close()
	if a := again.(*endpointtls.Conn); a.UpstreamAddress() != addr || a.TLS != cfg {
		t.Fatalf("redial = %+v", a)
	}
	roundTrip(t, again)
}

func TestDialRefusesUnverifiedServers(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	serveEcho(t, ln)
	addr := net.JoinHostPort("localhost", port(t, ln))

	// The system roots do not know the test CA.
	system, err := endpointtls.New("")
	if err != nil {
		t.Fatal(err)
	}
	if c, err := system.Dial(context.Background(), addr, 5*time.Second); err == nil {
		c.Close()
		t.Fatal("dial verified a server signed by an unknown CA")
	} else if !strings.Contains(err.Error(), "tls handshake with "+addr) {
		t.Fatalf("err = %v", err)
	}

	// The right CA but a name the certificate does not cover.
	cfg, err := endpointtls.New(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	wrongName := net.JoinHostPort("127.0.0.1", port(t, ln))
	if c, err := cfg.Dial(context.Background(), wrongName, 5*time.Second); err == nil {
		c.Close()
		t.Fatal("dial accepted a certificate for another name")
	}
}

func TestDialRefusesPlaintextServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err == nil {
			// A ClickHouse-style server answers garbage to a ClientHello.
			_, _ = c.Write([]byte("not tls at all"))
			_ = c.Close()
		}
	}()
	ca := endpointtlstest.NewCA(t)
	cfg, err := endpointtls.New(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := cfg.Dial(context.Background(), ln.Addr().String(), 5*time.Second); err == nil {
		c.Close()
		t.Fatal("TLS dial succeeded against a plaintext server")
	}
}

func TestNewRejectsBadCAFile(t *testing.T) {
	if _, err := endpointtls.New(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("missing CA file accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("no pem here"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := endpointtls.New(empty); err == nil || !strings.Contains(err.Error(), "no PEM certificate") {
		t.Fatalf("err = %v", err)
	}
}

func TestHTTPClientTrustsCAFile(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.TLS = ca.ServerConfig(t, "127.0.0.1")
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cfg, err := endpointtls.New(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cfg.HTTPClient(5 * time.Second).Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	var none *endpointtls.Config
	if _, err := none.HTTPClient(5 * time.Second).Get(srv.URL); err == nil {
		t.Fatal("system-roots client trusted the test CA")
	}
}
