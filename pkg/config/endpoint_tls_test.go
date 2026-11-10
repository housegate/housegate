package config

import (
	"strings"
	"testing"
)


func TestNetworkStateEndpointTLSHelpers(t *testing.T) {
	for name, tc := range map[string]struct {
		v       *bool
		enabled bool
		scheme  string
	}{
		"unset keeps the legacy behaviour": {nil, false, ""},
		"on":                               {boolPtr(true), true, "https"},
		"off":                              {boolPtr(false), false, "http"},
	} {
		c := NetworkStateConfig{EndpointTLS: tc.v}
		if c.EndpointTLSEnabled() != tc.enabled || c.IndexerRPCScheme() != tc.scheme {
			t.Errorf("%s: enabled=%v scheme=%q", name, c.EndpointTLSEnabled(), c.IndexerRPCScheme())
		}
	}
}

func TestEndpointTLSEnv(t *testing.T) {
	t.Setenv("HOUSEGATE_NETWORK_STATE_ENDPOINT_TLS", "true")
	t.Setenv("HOUSEGATE_NETWORK_STATE_TLS_CA_FILE", "/etc/ca.pem")
	t.Setenv("HOUSEGATE_AGENT_UPSTREAM_TLS", "true")
	d := Default()
	if !d.NetworkState.EndpointTLSEnabled() || d.NetworkState.TLSCAFile != "/etc/ca.pem" || !d.Agent.UpstreamTLS {
		t.Fatalf("env not applied: %+v upstream_tls=%v", d.NetworkState, d.Agent.UpstreamTLS)
	}
	t.Setenv("HOUSEGATE_NETWORK_STATE_ENDPOINT_TLS", "false")
	if d := Default(); d.NetworkState.EndpointTLS == nil || *d.NetworkState.EndpointTLS {
		t.Fatalf("endpoint_tls=false env = %v", d.NetworkState.EndpointTLS)
	}
	t.Setenv("HOUSEGATE_NETWORK_STATE_ENDPOINT_TLS", "")
	if d := Default(); d.NetworkState.EndpointTLS != nil {
		t.Fatal("an empty env var must leave endpoint_tls unset")
	}
}

func TestDefaultLeavesEndpointTLSOff(t *testing.T) {
	d := Default()
	if d.NetworkState.EndpointTLS != nil || d.NetworkState.TLSCAFile != "" || d.Agent.UpstreamTLS {
		t.Fatalf("defaults must keep plaintext dials: %+v upstream_tls=%v", d.NetworkState, d.Agent.UpstreamTLS)
	}
}

func TestValidateTLSSettings(t *testing.T) {
	agent := func() *Config {
		c := quickstartBase()
		c.Agent.Mode = true
		c.NetworkState.Source = "http://node:10003"
		return c
	}

	c := agent()
	c.NetworkState.TLSCAFile = "/etc/ca.pem"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "network_state.tls_ca_file is set but nothing uses TLS") {
		t.Fatalf("a CA file nothing uses must be refused: %v", err)
	}
	c.NetworkState.EndpointTLS = boolPtr(true)
	if err := c.Validate(); err != nil {
		t.Fatalf("CA file with endpoint_tls: %v", err)
	}
	c = agent()
	c.NetworkState.Source = "https://boot.example.com"
	c.NetworkState.TLSCAFile = "/etc/ca.pem"
	if err := c.Validate(); err != nil {
		t.Fatalf("CA file with an https source: %v", err)
	}

	c = agent()
	c.Agent.UpstreamTLS = true
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "agent.upstream_tls is set without agent.upstream") {
		t.Fatalf("upstream_tls without a pinned upstream must be refused: %v", err)
	}
	c.Agent.Upstream = "hg.example.com:9001"
	c.NetworkState.TLSCAFile = "/etc/ca.pem"
	if err := c.Validate(); err != nil {
		t.Fatalf("pinned upstream with upstream_tls: %v", err)
	}
}

func TestQuickstartPresetSetsEndpointTLS(t *testing.T) {
	const name = "tls-test-net"
	AgentNetworkPresets[name] = AgentNetworkPreset{Source: "https://boot.example.com", EndpointTLS: true}
	t.Cleanup(func() { delete(AgentNetworkPresets, name) })

	c := quickstartBase()
	if err := ApplyAgentQuickstart(c, AgentQuickstart{Network: name}); err != nil {
		t.Fatal(err)
	}
	if c.NetworkState.Source != "https://boot.example.com" || !c.NetworkState.EndpointTLSEnabled() {
		t.Fatalf("preset not applied: %+v", c.NetworkState)
	}

	// An explicit endpoint_tls (flag, env or file) wins over the preset.
	c = quickstartBase()
	c.NetworkState.EndpointTLS = boolPtr(false)
	if err := ApplyAgentQuickstart(c, AgentQuickstart{Network: name}); err != nil {
		t.Fatal(err)
	}
	if c.NetworkState.EndpointTLSEnabled() {
		t.Fatal("explicit endpoint_tls=false was overridden by the preset")
	}

	// A -state that wins over the preset leaves endpoint_tls as configured.
	c = quickstartBase()
	c.NetworkState.Source = "http://node:10003"
	if err := ApplyAgentQuickstart(c, AgentQuickstart{Network: name}); err != nil {
		t.Fatal(err)
	}
	if c.NetworkState.EndpointTLS != nil {
		t.Fatalf("a losing preset changed endpoint_tls: %v", *c.NetworkState.EndpointTLS)
	}
}

func TestDevnet2PresetStaysPlaintext(t *testing.T) {
	c := quickstartBase()
	if err := ApplyAgentQuickstart(c, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if c.NetworkState.Source != "http://64.38.144.158:32003" || c.NetworkState.EndpointTLSEnabled() || c.NetworkState.IndexerRPCScheme() != "http" {
		t.Fatalf("devnet2 preset changed: %+v", c.NetworkState)
	}
}
