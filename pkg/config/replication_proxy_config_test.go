package config

import (
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/plugins/agent"
)

const trustedInterserverPeerAddress = "0x0000000000000000000000000000000000001001"

func minimalServerConfigWithPeerAuth(t *testing.T) Config {
	t.Helper()
	cfg := minimalServerConfig(t)
	cfg.RelayPrivateKeyHex = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	cfg.Auth.Enabled = true
	cfg.Auth.AllowedAddresses = []string{trustedInterserverPeerAddress}
	cfg.Auth.MaxTokenAge = Duration{time.Minute}
	return cfg
}

func validReplicationProxyConfig() ReplicationProxyConfig {
	return ReplicationProxyConfig{
		Keeper: ReplicationProxyKeeperConfig{
			Enabled:     true,
			Listen:      "127.0.0.1:9181",
			Upstreams:   []string{"keeper-1:9181", "keeper-2:9181"},
			DialTimeout: Duration{5 * time.Second},
		},
		Interserver: ReplicationProxyInterserverConfig{
			Enabled:         true,
			Listen:          "127.0.0.1:9010",
			LocalUpstream:   "127.0.0.1:9009",
			PeerUserHeader:  "X-Housegate-Peer-User",
			PeerTokenHeader: "X-Housegate-Peer-Token",
			DialTimeout:     Duration{5 * time.Second},
			ReadTimeout:     Duration{30 * time.Second},
			WriteTimeout:    Duration{30 * time.Second},
			Routes: []ReplicationProxyInterserverRoute{
				{Peer: "1001", Upstream: "peer-2.example:9010"},
				{Peer: "1002", Upstream: "peer-3.example:9010"},
			},
		},
	}
}

func TestConfigValidate_ReplicationProxy(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		mutate      func(*Config)
		wantErr     bool
		wantContain []string
	}{
		{
			name: "disabled default is backward compatible",
			cfg:  minimalServerConfig(t),
		},
		{
			name: "enabled valid config",
			cfg:  minimalServerConfigWithPeerAuth(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
			},
		},
		{
			name: "interserver missing relay private key",
			cfg:  minimalServerConfigWithPeerAuth(t),
			mutate: func(c *Config) {
				c.RelayPrivateKeyHex = ""
				c.ReplicationProxy = validReplicationProxyConfig()
			},
			wantErr:     true,
			wantContain: []string{"relay_private_key_hex", "replication_proxy.interserver.enabled"},
		},
		{
			name: "interserver missing auth validator",
			cfg:  minimalServerConfigWithPeerAuth(t),
			mutate: func(c *Config) {
				c.Auth.Enabled = false
				c.ReplicationProxy = validReplicationProxyConfig()
			},
			wantErr:     true,
			wantContain: []string{"auth.enabled", "replication_proxy.interserver.enabled"},
		},
		{
			name: "interserver missing trusted peer allowlist",
			cfg:  minimalServerConfigWithPeerAuth(t),
			mutate: func(c *Config) {
				c.Auth.AllowedAddresses = nil
				c.ReplicationProxy = validReplicationProxyConfig()
			},
			wantErr:     true,
			wantContain: []string{"auth.allowed_addresses", "replication_proxy.interserver.enabled"},
		},
		{
			name: "bad keeper listen",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Keeper.Listen = "not-a-host-port"
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy.keeper.listen"},
		},
		{
			name: "keeper missing upstream",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Keeper.Upstreams = nil
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy.keeper.upstreams"},
		},
		{
			name: "bad interserver route target",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Interserver.Routes[0].Upstream = "not-a-host-port"
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy.interserver.routes[0].upstream"},
		},
		{
			name: "interserver duplicate route",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Interserver.Routes[1].Peer = "1001"
			},
			wantErr:     true,
			wantContain: []string{"duplicate peer"},
		},
		{
			name: "interserver non-decimal route peer",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Interserver.Routes[0].Peer = "indexer-2"
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy.interserver.routes[0].peer", "decimal target indexer id"},
		},
		{
			name: "interserver invalid timeout",
			cfg:  minimalServerConfig(t),
			mutate: func(c *Config) {
				c.ReplicationProxy = validReplicationProxyConfig()
				c.ReplicationProxy.Interserver.DialTimeout = Duration{}
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy.interserver.dial_timeout"},
		},
		{
			name: "agent mode rejection",
			cfg: Config{
				Listen: ":9001",
				Agent: agent.Config{
					Mode:          true,
					Upstream:      "proxy:9001",
					PrivateKeyHex: "0xdeadbeef",
				},
				ReplicationProxy: validReplicationProxyConfig(),
			},
			wantErr:     true,
			wantContain: []string{"replication_proxy", "server mode only"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}

			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			for _, s := range tt.wantContain {
				if err == nil || !strings.Contains(err.Error(), s) {
					t.Errorf("error %q missing substring %q", err, s)
				}
			}
		})
	}
}

func TestConfigValidate_ReplicationProxy_defaultDisabled(t *testing.T) {
	cfg := Default()

	if cfg.ReplicationProxy.Keeper.Enabled {
		t.Fatalf("Default().ReplicationProxy.Keeper.Enabled = true, want false")
	}
	if cfg.ReplicationProxy.Interserver.Enabled {
		t.Fatalf("Default().ReplicationProxy.Interserver.Enabled = true, want false")
	}
	if cfg.ReplicationProxy.Interserver.PeerUserHeader == "" || cfg.ReplicationProxy.Interserver.PeerTokenHeader == "" {
		t.Fatalf("default interserver peer header names must be populated: %+v", cfg.ReplicationProxy.Interserver)
	}
}
