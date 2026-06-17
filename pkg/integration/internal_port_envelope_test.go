package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate"
	"housegate/housegate/pkg/cfgtypes"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/network"
	authplugin "housegate/housegate/pkg/plugins/auth"
	"housegate/housegate/pkg/route"
)

// startServerWithInternal starts a server-mode proxy configured with
// both an external listener (ephemeral port) and an internal listener
// (specific port). The internal listener pre-flags IsPeerTrusted=true
// and IsInternalPort=true on every session via PreflagSession.
//
// Returns the external address (via proxy.Addr()) and the internal
// address. The proxy is registered for cleanup via t.Cleanup.
func startServerWithInternal(t *testing.T, chAddr string, opts ...testenv.ProxyOption) (extAddr, intAddr string) {
	t.Helper()

	// Reserve a specific port for the internal listener so we can
	// return it to the caller. Released before proxy.Run binds it.
	intLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve internal port: %v", err)
	}
	intAddr = intLn.Addr().String()
	intLn.Close()

	// Write the ckh_manager YAML so the credential provider can
	// replace envelope users with real CH credentials.
	tempDir := t.TempDir()
	ckhManagerPath := filepath.Join(tempDir, "ckh_manager.yaml")
	ckhManagerYAML := fmt.Sprintf(`
credential:
  subgraph:
    username: %q
    password: %q
shards:
  - index: 0
    name: "test-shard"
    allow_tiers: [0]
    addresses:
      external_tcp_proxy: %q
`, chEnv.User, chEnv.Password, chAddr)
	if err := os.WriteFile(ckhManagerPath, []byte(ckhManagerYAML), 0o644); err != nil {
		t.Fatalf("write ckh_manager.yaml: %v", err)
	}

	cfgVal := config.Default()
	cfg := &cfgVal
	cfg.Listen = "127.0.0.1:0"   // ephemeral — captured via proxy.Addr()
	cfg.InternalListen = intAddr // specific — returned to caller
	cfg.MetricsListen = ""
	cfg.Upstream = chAddr
	cfg.CkhManagerConfigPath = ckhManagerPath
	cfg.Auth = authplugin.Config{
		Enabled:     true,
		AllowNoAuth: false,
		MaxTokenAge: cfgtypes.Duration{Duration: 5 * time.Minute},
	}
	cfg.Rewriter.ServiceAddr = ""
	cfg.Rewriter.PhysicalDatabase = ""

	ns := network.NewInMemoryNetworkState()
	ns.DatabaseInfos[network.Database("system")] = network.DatabaseInfo{IndexerId: 0}
	ns.DatabaseInfos[network.Database(chEnv.Database)] = network.DatabaseInfo{IndexerId: 0}

	hgOpts := housegate.Options{
		Config:       cfg,
		NetworkState: ns,
	}
	for _, o := range opts {
		o(cfg, &hgOpts)
	}

	proxy, err := housegate.New(hgOpts)
	if err != nil {
		t.Fatalf("housegate.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() { doneCh <- proxy.Run(ctx) }()

	// Wait for the external listener to bind.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && proxy.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if proxy.Addr() == nil {
		cancel()
		t.Fatal("proxy did not bind external listener within 5s")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			t.Log("proxy.Run did not finish within 10s")
		}
	})

	return proxy.Addr().String(), intAddr
}

// TestInternalPort_PreflagsPeerTrust verifies that the internal port
// pre-flags IsPeerTrusted=true on every session, causing auth and
// commitgate to be skipped.
//
// Topology:
//
//	client ──→ external port (auth on, unsigned query → rejected)
//	client ──→ internal port (pre-flagged IsPeerTrusted → unsigned query succeeds)
//
// A client connecting to the external port without a JWS token is
// rejected by the auth plugin. The same client connecting to the
// internal port succeeds because the pre-flagged IsPeerTrusted skips
// the auth plugin (RunOnPeerTrust=false) and commitgate (also
// RunOnPeerTrust=false).
func TestInternalPort_PreflagsPeerTrust(t *testing.T) {
	extAddr, intAddr := startServerWithInternal(t, chEnv.Addr)

	// External port: unsigned query must be rejected.
	extConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{extAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("external clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = extConn.Close() })

	if err := extConn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8)); err == nil {
		t.Error("unsigned query on external port succeeded; expected auth rejection")
	}

	// Internal port: unsigned query must succeed (auth + commitgate
	// skipped due to pre-flagged IsPeerTrusted).
	intConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{intAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("internal clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = intConn.Close() })

	var v uint8
	if err := intConn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("unsigned query on internal port failed (expected success): %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}

// TestInternalPort_RejectsRouteEnvelope verifies that the Stripper
// plugin hard-rejects a __route__ envelope arriving on the internal
// port. This prevents forwarding loops when peer proxies accidentally
// route traffic back to the internal port.
//
// The rejection happens during OnHello, before any upstream dial or
// query processing, so this test succeeds regardless of auth config.
func TestInternalPort_RejectsRouteEnvelope(t *testing.T) {
	_, intAddr := startServerWithInternal(t, chEnv.Addr)

	// Build a route envelope. The target is irrelevant — the Stripper
	// rejects based on IsInternalPort before processing the target.
	envelopeUser := route.FormatRouteUser("127.0.0.1:1", chEnv.User)

	intConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{intAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: envelopeUser,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("internal clickhouse.Open with route envelope: %v", err)
	}
	t.Cleanup(func() { _ = intConn.Close() })

	// The Stripper rejects the route envelope during OnHello, so the
	// handshake fails and the first query returns an error.
	if err := intConn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8)); err == nil {
		t.Fatal("route envelope on internal port succeeded; expected rejection")
	}
}
