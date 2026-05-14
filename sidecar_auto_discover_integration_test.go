package housegate

// sidecar_auto_discover_integration_test.go — end-to-end test for sidecar
// upstream auto-discovery (permissioned tier + bootstrap fallback).
//
// Origin: docs/superpowers/specs/2026-04-28-two-port-server-mode.md
//
// Exercises buildSidecarDialer with two fake indexer-proxy listeners
// and an InMemoryNetworkState. Confirms:
//
//   1. Permissioned tier — when the sidecar's account has perms on a
//      database hosted by indexer A, every Pick lands on A's port.
//   2. Bootstrap tier — when the account has no perms, picks distribute
//      across every bound indexer (no permissioned filter applies).
//   3. Explicit Sidecar.Upstream wins — even when NetworkState is
//      configured, an explicit address overrides the Selector path.

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/registry"
	sidecarcfg "housegate/housegate/pkg/plugins/sidecar"
	"housegate/housegate/pkg/proxy"
)

// startFakeIndexerProxy stands up a TCP listener that accepts a
// ClientHello and writes a minimal ServerHello back. The returned
// "host:port" is what NetworkState should advertise as the indexer's
// ClickhouseProxyPort target.
func startFakeIndexerProxy(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake indexer proxy listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))

				// We do not read from c here — the dialer test only
				// exercises the dial path, not the handshake. Drain
				// outbound bytes so the kernel doesn't refuse SYN-ACK.
				_, _ = io.Copy(io.Discard, c)
			}(conn)
		}
	}()

	return ln.Addr().String()
}

// indexerInfoForAddr converts a host:port produced by
// startFakeIndexerProxy into the IndexerInfo shape the production
// dialer code expects.
func indexerInfoForAddr(t *testing.T, id uint64, addr string) network.IndexerInfo {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("port parse: %v", err)
	}
	return network.IndexerInfo{
		IndexerId:           id,
		IndexerUrl:          host,
		ClickhouseProxyPort: uint16(port),
	}
}

// minimalSidecarCfgAutoDiscover returns a Config in sidecar-mode with
// an empty Upstream — Selector path is the one under test.
func minimalSidecarCfgAutoDiscover(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.Sidecar = sidecarcfg.Config{
		Mode:          true,
		PrivateKeyHex: testRelayKeyHex,
	}
	// IsConfigured (yaml suffix) — the path is not actually loaded
	// because we'll inject NetworkState directly via Options.
	cfg.NetworkState.Source = "configs/local.network_state.yaml"
	return &cfg
}

// closeCodec returns the underlying connection of a Codec (which may
// be a *net.TCPConn) so the test can pull RemoteAddr out for assertion.
func codecRemoteAddr(c *chproto.Codec) string {
	if conn, ok := c.Conn().(net.Conn); ok {
		return conn.RemoteAddr().String()
	}
	return ""
}

func TestSidecarAutoDiscover_PermissionedTier(t *testing.T) {
	addrA := startFakeIndexerProxy(t)
	addrB := startFakeIndexerProxy(t)

	signer, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	account := signer.Address()

	ns := network.NewInMemoryNetworkState()
	ns.IndexerInfos[1] = indexerInfoForAddr(t, 1, addrA)
	ns.IndexerInfos[2] = indexerInfoForAddr(t, 2, addrB)
	ns.DatabaseInfos["dbOnA"] = network.DatabaseInfo{DatabaseId: "dbOnA", IndexerId: 1}
	ns.DatabaseInfos["dbOnB"] = network.DatabaseInfo{DatabaseId: "dbOnB", IndexerId: 2}
	ns.DatabasePermissions[network.AccountAddress(account)] = network.DatabasePermissions{
		"dbOnA": registry.DbAuthRead, // perms only on dbOnA → indexer 1
	}

	cfg := minimalSidecarCfgAutoDiscover(t)
	bs, err := buildSidecar(Options{
		Config:       cfg,
		NetworkState: ns,
		Signer:       signer,
	}, nil)
	if err != nil {
		t.Fatalf("buildSidecar: %v", err)
	}
	defer bs.teardown()
	if len(bs.listeners) != 1 {
		t.Fatalf("expected 1 listener, got %d", len(bs.listeners))
	}

	// Reach the dialer through the Server. The Server exposes its dialer
	// via the public Dial method since the codec it returns is the
	// upstream we care about. Without that, run the dialer logic
	// directly via the package-private helper.
	dialer, err := buildSidecarDialer(Options{
		Config:       cfg,
		NetworkState: ns,
	}, nil, account, proxy.NewMetricsObserver())
	if err != nil {
		t.Fatalf("buildSidecarDialer: %v", err)
	}

	const trials = 30
	hits := map[string]int{}
	ctx := context.Background()
	for i := 0; i < trials; i++ {
		c, err := dialer(ctx, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		hits[codecRemoteAddr(c)]++
		if conn, ok := c.Conn().(net.Conn); ok {
			_ = conn.Close()
		}
	}

	// All trials must land on addrA — only addrA hosts a database the
	// account has perms on.
	if hits[addrA] != trials {
		t.Errorf("expected %d hits on addrA=%s, got hits=%v", trials, addrA, hits)
	}
	if hits[addrB] != 0 {
		t.Errorf("expected 0 hits on non-permissioned addrB=%s, got %d", addrB, hits[addrB])
	}
}

func TestSidecarAutoDiscover_BootstrapTier_NoPerms(t *testing.T) {
	addrA := startFakeIndexerProxy(t)
	addrB := startFakeIndexerProxy(t)

	signer, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	account := signer.Address()

	ns := network.NewInMemoryNetworkState()
	ns.IndexerInfos[1] = indexerInfoForAddr(t, 1, addrA)
	ns.IndexerInfos[2] = indexerInfoForAddr(t, 2, addrB)
	// No DatabaseInfos and no DatabasePermissions — brand-new account.

	cfg := minimalSidecarCfgAutoDiscover(t)
	bs, err := buildSidecar(Options{
		Config:       cfg,
		NetworkState: ns,
		Signer:       signer,
	}, nil)
	if err != nil {
		t.Fatalf("buildSidecar: %v", err)
	}
	defer bs.teardown()

	dialer, err := buildSidecarDialer(Options{
		Config:       cfg,
		NetworkState: ns,
	}, nil, account, proxy.NewMetricsObserver())
	if err != nil {
		t.Fatalf("buildSidecarDialer: %v", err)
	}

	// Use a fairly high trial count so randomness almost certainly hits
	// both indexers at least once. Probability of skipping either over
	// 50 trials is 2 * (1/2)^50 — vanishingly small.
	const trials = 50
	hits := map[string]int{}
	ctx := context.Background()
	for i := 0; i < trials; i++ {
		c, err := dialer(ctx, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		hits[codecRemoteAddr(c)]++
		if conn, ok := c.Conn().(net.Conn); ok {
			_ = conn.Close()
		}
	}

	if hits[addrA] == 0 || hits[addrB] == 0 {
		t.Errorf("bootstrap tier did not distribute across both bound indexers: hits=%v", hits)
	}
	if hits[addrA]+hits[addrB] != trials {
		t.Errorf("hits do not sum to trials: hits=%v", hits)
	}
}

func TestSidecarAutoDiscover_ExplicitUpstreamOverride(t *testing.T) {
	// When cfg.Sidecar.Upstream is set, the Selector path is skipped.
	// We verify by setting Upstream to a port we control and providing
	// a NetworkState pointing at a DIFFERENT port — every dial should
	// land on Upstream.
	addrUpstream := startFakeIndexerProxy(t)
	addrIndexer := startFakeIndexerProxy(t)

	signer, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	account := signer.Address()

	ns := network.NewInMemoryNetworkState()
	ns.IndexerInfos[1] = indexerInfoForAddr(t, 1, addrIndexer)

	cfg := minimalSidecarCfgAutoDiscover(t)
	cfg.Sidecar.Upstream = addrUpstream

	bs, err := buildSidecar(Options{
		Config:       cfg,
		NetworkState: ns,
		Signer:       signer,
	}, nil)
	if err != nil {
		t.Fatalf("buildSidecar: %v", err)
	}
	defer bs.teardown()

	dialer, err := buildSidecarDialer(Options{
		Config:       cfg,
		NetworkState: ns,
	}, nil, account, proxy.NewMetricsObserver())
	if err != nil {
		t.Fatalf("buildSidecarDialer: %v", err)
	}

	const trials = 10
	hits := map[string]int{}
	ctx := context.Background()
	for i := 0; i < trials; i++ {
		c, err := dialer(ctx, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		hits[codecRemoteAddr(c)]++
		if conn, ok := c.Conn().(net.Conn); ok {
			_ = conn.Close()
		}
	}

	if hits[addrUpstream] != trials {
		t.Errorf("expected all %d dials on explicit upstream %s, got %v", trials, addrUpstream, hits)
	}
}

// silenceUnusedImports references everything imported but only used
// when the dialer happens to actually round-trip a handshake. Keeps
// goimports happy without bloating the active test code.
var _ = proto.ServerCodeHello
