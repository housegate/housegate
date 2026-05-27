package config

import "testing"

// These pin the validation of the keeper_proxy / interserver_proxy config
// blocks — the seam the container-based integration tests (kpx/igw) bypass
// by constructing the proxy ServerConfig directly.

func TestConfig_Validate_KeeperProxy(t *testing.T) {
	base := minimalServerConfig(t)

	// Disabled (empty Listen) is valid.
	cfg := base
	if err := cfg.Validate(); err != nil {
		t.Fatalf("keeper_proxy disabled must be valid: %v", err)
	}

	valid := func(kp KeeperProxyConfig) error {
		c := base
		c.KeeperProxy = kp
		return c.Validate()
	}

	if err := valid(KeeperProxyConfig{Listen: ":9181", Members: []string{"k1:9181", "k2:9181"}, Strategy: "leader_pref"}); err != nil {
		t.Fatalf("valid keeper_proxy rejected: %v", err)
	}
	if err := valid(KeeperProxyConfig{Listen: ":9181", Members: []string{"k1:9181"}, Strategy: ""}); err != nil {
		t.Fatalf("empty strategy must default, not error: %v", err)
	}

	for name, kp := range map[string]KeeperProxyConfig{
		"no members":       {Listen: ":9181"},
		"bad listen":       {Listen: "not-a-hostport", Members: []string{"k1:9181"}},
		"bad member":       {Listen: ":9181", Members: []string{"bad-member"}},
		"unknown strategy": {Listen: ":9181", Members: []string{"k1:9181"}, Strategy: "bogus"},
	} {
		if err := valid(kp); err == nil {
			t.Errorf("keeper_proxy %q must error", name)
		}
	}
}

func TestConfig_Validate_InterserverProxy(t *testing.T) {
	base := minimalServerConfig(t)

	cfg := base
	if err := cfg.Validate(); err != nil {
		t.Fatalf("interserver_proxy disabled must be valid: %v", err)
	}

	valid := func(ip InterserverProxyConfig) error {
		c := base
		c.InterserverProxy = ip
		return c.Validate()
	}

	if err := valid(InterserverProxyConfig{Listen: ":9009", Target: "127.0.0.1:9010", AllowCIDRs: []string{"10.0.0.0/8", "127.0.0.0/8"}}); err != nil {
		t.Fatalf("valid interserver_proxy rejected: %v", err)
	}
	if err := valid(InterserverProxyConfig{Listen: ":9009", Target: "127.0.0.1:9010"}); err != nil {
		t.Fatalf("interserver_proxy without allow_cidrs must be valid: %v", err)
	}

	for name, ip := range map[string]InterserverProxyConfig{
		"no target":  {Listen: ":9009"},
		"bad listen": {Listen: "nope", Target: "127.0.0.1:9010"},
		"bad target": {Listen: ":9009", Target: "nope"},
		"bad cidr":   {Listen: ":9009", Target: "127.0.0.1:9010", AllowCIDRs: []string{"not-a-cidr"}},
	} {
		if err := valid(ip); err == nil {
			t.Errorf("interserver_proxy %q must error", name)
		}
	}
}
