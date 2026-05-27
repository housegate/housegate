package housegate

import (
	"testing"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/interserver"
	"housegate/housegate/pkg/keeper"
	"housegate/housegate/pkg/network"
)

// These cover the config→build.go→listener seam for the keeper_proxy and
// interserver_proxy blocks — what the kpx/igw container tests bypass by
// constructing the proxy ServerConfig directly. They assert buildServer
// appends the right listener (addr + concrete type) when the block is
// configured, and that CIDR parsing in buildInterserverServer works.

func findListener(t *testing.T, bs *builtServer, label string) serverListener {
	t.Helper()
	for _, l := range bs.listeners {
		if l.Label == label {
			return l
		}
	}
	t.Fatalf("no %q listener among %d listeners", label, len(bs.listeners))
	return serverListener{}
}

func TestBuildServer_KeeperProxyListener(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.KeeperProxy = config.KeeperProxyConfig{
		Listen:   "127.0.0.1:0",
		Members:  []string{"127.0.0.1:9181", "127.0.0.1:9182"},
		Strategy: "leader_pref",
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	l := findListener(t, bs, "keeper")
	if l.ListenAddr != cfg.KeeperProxy.Listen {
		t.Errorf("keeper listener addr = %q, want %q", l.ListenAddr, cfg.KeeperProxy.Listen)
	}
	if _, ok := l.Server.(*keeper.Server); !ok {
		t.Errorf("keeper listener Server is %T, want *keeper.Server", l.Server)
	}
}

func TestBuildServer_InterserverProxyListener(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.InterserverProxy = config.InterserverProxyConfig{
		Listen:     "127.0.0.1:0",
		Target:     "127.0.0.1:9010",
		AllowCIDRs: []string{"10.0.0.0/8"},
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	l := findListener(t, bs, "interserver")
	if l.ListenAddr != cfg.InterserverProxy.Listen {
		t.Errorf("interserver listener addr = %q, want %q", l.ListenAddr, cfg.InterserverProxy.Listen)
	}
	if _, ok := l.Server.(*interserver.Server); !ok {
		t.Errorf("interserver listener Server is %T, want *interserver.Server", l.Server)
	}
}

func TestBuildServer_NoProxyListenersWhenDisabled(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()
	for _, l := range bs.listeners {
		if l.Label == "keeper" || l.Label == "interserver" {
			t.Errorf("unexpected %q listener when proxy is disabled", l.Label)
		}
	}
}

func TestBuildInterserverServer_CIDRParsing(t *testing.T) {
	if _, err := buildInterserverServer(config.InterserverProxyConfig{
		Listen: ":9009", Target: "127.0.0.1:9010", AllowCIDRs: []string{"not-a-cidr"},
	}); err == nil {
		t.Fatal("bad CIDR must error")
	}
	srv, err := buildInterserverServer(config.InterserverProxyConfig{
		Listen: ":9009", Target: "127.0.0.1:9010", AllowCIDRs: []string{"10.0.0.0/8"},
	})
	if err != nil || srv == nil {
		t.Fatalf("valid interserver build failed: srv=%v err=%v", srv, err)
	}
}
