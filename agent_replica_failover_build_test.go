package housegate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/agent"
	"github.com/housegate/housegate/pkg/proxy"
)

// TestBuildAgentWiresReplicaFailoverOnlyForAnUnpinnedRPCNetworkState: the
// agent's server gets agent.ReplicaFailover (Plan S3-C4 K46) exactly when it
// discovers its upstreams through an RPC network state; a pinned
// agent.upstream, a YAML file and a host-injected in-memory state keep every
// relay as before.
func TestBuildAgentWiresReplicaFailoverOnlyForAnUnpinnedRPCNetworkState(t *testing.T) {
	yamlState := filepath.Join(t.TempDir(), "network.yaml")
	if err := os.WriteFile(yamlState, []byte("indexer_infos: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	build := func(t *testing.T, mutate func(*config.Config, *Options)) *proxy.Server {
		t.Helper()
		cfg := config.Default()
		cfg.Listen = "127.0.0.1:0"
		cfg.MetricsListen = ""
		cfg.Agent.Mode = true
		cfg.Agent.PrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		opts := Options{Config: &cfg}
		mutate(&cfg, &opts)
		bs, err := buildAgent(opts, nil)
		if err != nil {
			t.Fatalf("buildAgent: %v", err)
		}
		t.Cleanup(bs.teardown)
		return requireProxyServer(t, bs.listeners[0])
	}

	srv := build(t, func(cfg *config.Config, _ *Options) { cfg.NetworkState.Source = "http://127.0.0.1:9" })
	failover, ok := srv.UpstreamFailover.(*agent.ReplicaFailover)
	if !ok {
		t.Fatalf("an RPC network state: UpstreamFailover = %T, want *agent.ReplicaFailover", srv.UpstreamFailover)
	}
	if _, ok := failover.Discovery.(*network.RpcNetworkState); !ok || failover.Databases == nil || failover.Topology == nil {
		t.Fatalf("ReplicaFailover = %+v, want the RPC network state as discovery, databases and topology", failover)
	}

	for name, mutate := range map[string]func(*config.Config, *Options){
		"pinned upstream": func(cfg *config.Config, _ *Options) {
			cfg.Agent.Upstream = "127.0.0.1:9000"
			cfg.NetworkState.Source = "http://127.0.0.1:9"
		},
		"yaml network state": func(cfg *config.Config, _ *Options) { cfg.NetworkState.Source = yamlState },
		"host-injected in-memory state": func(_ *config.Config, opts *Options) {
			opts.NetworkState = network.NewInMemoryNetworkState()
		},
	} {
		t.Run(name, func(t *testing.T) {
			if srv := build(t, mutate); srv.UpstreamFailover != nil {
				t.Fatalf("UpstreamFailover = %T, want nil", srv.UpstreamFailover)
			}
		})
	}
}
