package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// TestProxy is a running in-process housegate proxy started via RunWith.
// Cleanup is registered via t.Cleanup; tests rarely need to touch Close
// directly.
type TestProxy struct {
	// Addr is the bound listener address ("127.0.0.1:<port>").
	Addr string

	// Proxy is the underlying started proxy, exposed so tests can reach
	// accessors such as MetricsRegistry().
	Proxy housegate.Proxy

	cancel    context.CancelFunc
	done      <-chan error
	closeOnce sync.Once
}

// ProxyOption mutates the Config and/or Options before the proxy starts.
type ProxyOption func(cfg *config.Config, opts *housegate.Options)

// WithConfigMutator returns a ProxyOption that only touches Config —
// useful for one-off field overrides (e.g. flipping auth on for an
// auth-specific test).
func WithConfigMutator(m func(*config.Config)) ProxyOption {
	return func(cfg *config.Config, _ *housegate.Options) { m(cfg) }
}

// WithExtraDatabases registers additional logical databases on the
// in-memory NetworkState so forward.Plugin lets hellos/queries through.
// `system` and the testenv default database are already registered;
// tests that USE / open new databases must register them here.
func WithExtraDatabases(names ...string) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		ns, ok := opts.NetworkState.(*network.InMemoryNetworkState)
		if !ok {
			panic("WithExtraDatabases requires the default in-memory NetworkState")
		}
		for _, name := range names {
			ns.DatabaseInfos[network.Database(name)] = network.DatabaseInfo{IndexerId: 0}
		}
	}
}

// WithDatabasePermission grants `auth` on `database` to `account` in the
// in-memory NetworkState. Account is normalised to lower-case to match
// the canonical form PermissionsFor / IsOperator use (the auth plugin
// passes the lower-cased Ethereum address as ev.User; an upper-case key
// here would silently miss).
//
// Use registry.DbAuthOwner for tests that need CREATE TABLE / DROP TABLE
// to clear PermissionObserver; DbAuthRead/Write/Admin for narrower
// scenarios. The auth bits are bitwise-OR'd onto any prior grant so
// callers can layer multiple ops without re-stating earlier bits.
func WithDatabasePermission(account, database string, auth registry.DbAuth) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		ns, ok := opts.NetworkState.(*network.InMemoryNetworkState)
		if !ok {
			panic("WithDatabasePermission requires the default in-memory NetworkState")
		}
		addr := network.AccountAddress(strings.ToLower(account))
		if ns.DatabasePermissions[addr] == nil {
			ns.DatabasePermissions[addr] = make(network.DatabasePermissions)
		}
		ns.DatabasePermissions[addr][network.Database(database)] |= auth
	}
}

// StartServerProxy starts a server-mode housegate proxy pointing at the
// given ClickHouse address. The proxy:
//
//   - binds 127.0.0.1:0 (ephemeral) and exposes the resolved address via
//     TestProxy.Addr,
//   - has rewriter disabled (no gRPC dial),
//   - has auth disabled (AllowNoAuth=true),
//   - has an in-memory NetworkState pre-registered with system / the test
//     database so forward.Plugin treats hello-time database selection as
//     locally owned.
//
// Tests that need a different posture (auth on, rewriter pointed at a
// real service, etc.) pass ProxyOptions to override.
func StartServerProxy(t *testing.T, chAddr string, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()
	cfg := buildDefaultServerConfig(t, chAddr)
	return startProxy(t, cfg, proxyOpts...)
}

// StartReplicatedProxy starts a server-mode housegate proxy configured
// with two upstream replicas — exercises the multi-replica cluster
// manager path (cfg.Shard) instead of the single-upstream path
// (cfg.Upstream). The default routing strategy is round-robin (set by
// cluster.NewManager when cfg.Routing is nil).
//
// chA and chB must both be reachable at start time; passing a stopped
// container is undefined (cluster.NewManager dials lazily, so the proxy
// itself will start, but the first query may take the failed-replica
// path immediately).
func StartReplicatedProxy(t *testing.T, chA, chB *ClickHouseEnv, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()
	cfg := buildReplicatedServerConfig(t, chA, chB)
	return startProxy(t, cfg, proxyOpts...)
}

// StartRouterOnlyProxy starts a server-mode housegate proxy with NO
// upstream and NO shard configured — the "router" deployment role.
// Without a cluster manager, every session falls through to a peer
// pick over the NetworkState's bound proxies (pickRandomBoundProxy in
// build.go). The peer must already be running so its bound address
// can be registered as an IndexerInfo on the router's NetworkState.
//
// Typical wiring: an upstream server-mode proxy `peer` already points
// at a real CH; clients connect to the router, the router splices the
// entire TCP connection through to `peer`, which handles the actual
// query forwarding to CH. This mirrors production "ingress" + "indexer
// proxy" pairings where the ingress role has no CH of its own.
func StartRouterOnlyProxy(t *testing.T, peer *TestProxy, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()
	cfg := buildRouterOnlyServerConfig(t)
	opts := append([]ProxyOption{withPeerIndexer(peer.Addr)}, proxyOpts...)
	return startProxy(t, cfg, opts...)
}

// withPeerIndexer registers `peerAddr` as the only bound proxy in the
// router-only NetworkState. pickRandomBoundProxy then has exactly one
// choice — the peer — for every incoming session.
func withPeerIndexer(peerAddr string) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		ns, ok := opts.NetworkState.(*network.InMemoryNetworkState)
		if !ok {
			panic("withPeerIndexer requires the default in-memory NetworkState")
		}
		host, portStr, err := net.SplitHostPort(peerAddr)
		if err != nil {
			panic(fmt.Sprintf("withPeerIndexer: split %q: %v", peerAddr, err))
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			panic(fmt.Sprintf("withPeerIndexer: parse port from %q: %v", peerAddr, err))
		}
		ns.IndexerInfos[1] = network.IndexerInfo{
			IndexerId:           1,
			IndexerUrl:          host,
			ClickhouseProxyPort: uint16(port),
		}
	}
}

// startProxy is the shared launch path. Both StartServerProxy and
// StartReplicatedProxy funnel through here so the NetworkState
// pre-registration, RunWith bind-wait, and t.Cleanup wiring live in one
// place.
func startProxy(t *testing.T, cfg *config.Config, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()

	ns := network.NewInMemoryNetworkState()
	// forward.Plugin rejects unknown databases at hello time with
	// "Database X doesn't exist". Register the bootstrap set so the
	// default clickhouse-go connection (which sends hello.Database
	// equal to whatever testenv created) gets through.
	ns.DatabaseInfos[network.Database("system")] = network.DatabaseInfo{IndexerId: 0}
	ns.DatabaseInfos[network.Database(chDatabase)] = network.DatabaseInfo{IndexerId: 0}

	hgOpts := housegate.Options{
		Config:       cfg,
		NetworkState: ns,
	}

	for _, o := range proxyOpts {
		o(cfg, &hgOpts)
	}

	proxy, err := housegate.New(hgOpts)
	if err != nil {
		t.Fatalf("housegate.New: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() { doneCh <- proxy.RunWith(ctx, ln) }()

	// RunWith sets Addr synchronously before the accept loop, but the
	// goroutine has not necessarily reached that line by the time
	// startProxy returns. Poll briefly.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && proxy.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if proxy.Addr() == nil {
		cancel()
		t.Fatal("proxy did not bind within 5s")
	}

	tp := &TestProxy{
		Addr:   proxy.Addr().String(),
		Proxy:  proxy,
		cancel: cancel,
		done:   doneCh,
	}
	t.Cleanup(tp.Close)
	return tp
}

// Close shuts the proxy down and waits for RunWith to return. Idempotent
// — repeated calls return immediately. The second caller (typically the
// t.Cleanup hook firing after an explicit test-level Close) will not
// re-block on the already-drained done channel.
func (p *TestProxy) Close() {
	p.closeOnce.Do(func() {
		p.cancel()
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
		}
	})
}

// buildDefaultServerConfig assembles a minimal server-mode Config with
// no auth and no rewriter, pointing at chAddr. It also writes the
// ckh_manager YAML the Config.Validate() invariant requires when
// Upstream is set.
func buildDefaultServerConfig(t *testing.T, chAddr string) *config.Config {
	t.Helper()

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
`, chUser, chPassword, chAddr)
	if err := os.WriteFile(ckhManagerPath, []byte(ckhManagerYAML), 0o644); err != nil {
		t.Fatalf("write ckh_manager.yaml: %v", err)
	}

	cfgVal := config.Default()
	cfg := &cfgVal
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = "" // disable /metrics in tests
	cfg.Upstream = chAddr
	cfg.CkhManagerConfigPath = ckhManagerPath
	cfg.Auth.Enabled = false
	cfg.Auth.AllowNoAuth = true
	// Empty ServiceAddr → no rewriter gRPC dial; OnQuery becomes a
	// no-op for SQL rewriting. The rewrite plugin still loads and is
	// fail-open at the Factory layer.
	cfg.Rewriter.ServiceAddr = ""
	cfg.Rewriter.PhysicalDatabase = ""
	return cfg
}

// buildReplicatedServerConfig assembles a multi-replica server-mode
// Config. The two upstream addresses go into cfg.Shard.Replicas; auth
// and rewriter stay off; the ckh_manager YAML is still written because
// Config.Validate() requires it whenever Shard is set, even though the
// cluster manager itself reads the replica list from the Config struct
// rather than the YAML.
func buildReplicatedServerConfig(t *testing.T, a, b *ClickHouseEnv) *config.Config {
	t.Helper()

	// Start from the single-upstream baseline (writes the YAML, sets
	// auth/rewriter to off) and then override the shard wiring.
	cfg := buildDefaultServerConfig(t, a.Addr)
	cfg.Upstream = "" // shard takes precedence; clear to avoid the "upstream ignored" log

	cfg.Shard = &cluster.ShardConfig{
		Name: "test-shard",
		Replicas: []cluster.ReplicaConfig{
			mustReplica(t, a.Addr),
			mustReplica(t, b.Addr),
		},
	}
	return cfg
}

// buildRouterOnlyServerConfig assembles a server-mode Config with no
// Shard and no Upstream — the proxy will fall through to the peer-pick
// path on every session. ckh_manager YAML is NOT required in this mode
// (Config.Validate only demands it when Shard or Upstream is set).
func buildRouterOnlyServerConfig(t *testing.T) *config.Config {
	t.Helper()
	cfgVal := config.Default()
	cfg := &cfgVal
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = ""
	cfg.Upstream = ""
	cfg.Shard = nil
	cfg.Auth.Enabled = false
	cfg.Auth.AllowNoAuth = true
	cfg.Rewriter.ServiceAddr = ""
	cfg.Rewriter.PhysicalDatabase = ""
	return cfg
}

func mustReplica(t *testing.T, addr string) cluster.ReplicaConfig {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host/port from %q: %v", addr, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse port from %q: %v", addr, err)
	}
	return cluster.ReplicaConfig{Host: host, Port: p, Weight: 1}
}
