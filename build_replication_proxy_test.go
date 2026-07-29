package housegate

import (
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
)

func TestBuildServer_ReplicationProxyKeeperAddsListenerRunner(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.Listen = "127.0.0.1:9000"
	cfg.ReplicationProxy.Keeper.Enabled = true
	cfg.ReplicationProxy.Keeper.Listen = "127.0.0.1:9181"
	cfg.ReplicationProxy.Keeper.Upstreams = []string{"127.0.0.1:2181"}

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	if got, want := len(bs.listeners), 2; got != want {
		t.Fatalf("listeners: got %d want %d", got, want)
	}

	got := map[string]string{}
	for _, listener := range bs.listeners {
		got[listener.Label] = listener.ListenAddr
	}
	if got["external"] != "127.0.0.1:9000" {
		t.Fatalf("external listen addr: got %q", got["external"])
	}
	if got["replication-keeper"] != "127.0.0.1:9181" {
		t.Fatalf("replication keeper listen addr: got %q", got["replication-keeper"])
	}
}

func TestBuildServer_ReplicationProxyInterserverAddsListenerRunner(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.Listen = "127.0.0.1:9000"
	cfg.IndexerID = 1000
	cfg.RelayPrivateKeyHex = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	cfg.PeerTokenTTL = config.Duration{Duration: time.Minute}
	signer, err := auth.NewRelaySigner(cfg.RelayPrivateKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg.Auth.Enabled = true
	cfg.Auth.AllowedAddresses = []string{signer.Address()}
	cfg.ReplicationProxy.Interserver.Enabled = true
	cfg.ReplicationProxy.Interserver.Listen = "127.0.0.1:9010"
	cfg.ReplicationProxy.Interserver.LocalUpstream = "127.0.0.1:9009"
	cfg.ReplicationProxy.Interserver.Routes = []config.ReplicationProxyInterserverRoute{
		{Peer: "1001", Upstream: "127.0.0.1:9011"},
	}

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	if got, want := len(bs.listeners), 2; got != want {
		t.Fatalf("listeners: got %d want %d", got, want)
	}

	got := map[string]string{}
	for _, listener := range bs.listeners {
		got[listener.Label] = listener.ListenAddr
	}
	if got["external"] != "127.0.0.1:9000" {
		t.Fatalf("external listen addr: got %q", got["external"])
	}
	if got["replication-interserver"] != "127.0.0.1:9010" {
		t.Fatalf("replication interserver listen addr: got %q", got["replication-interserver"])
	}
}
