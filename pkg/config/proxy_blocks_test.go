package config

import (
	"path/filepath"
	"testing"
)

// These pin the validation of the keeper_proxy / interserver_mesh config
// blocks — the seam the container-based integration tests (kpx/imesh)
// bypass by constructing the proxy ServerConfig directly.

func TestConfig_Validate_KeeperProxy(t *testing.T) {
	base := minimalServerConfig(t)

	// Disabled (no shards) is valid.
	cfg := base
	if err := cfg.Validate(); err != nil {
		t.Fatalf("keeper_proxy disabled must be valid: %v", err)
	}

	valid := func(kp KeeperProxyConfig) error {
		c := base
		c.KeeperProxy = kp
		return c.Validate()
	}

	one := func(sh KeeperShardConfig) KeeperProxyConfig {
		return KeeperProxyConfig{Shards: []KeeperShardConfig{sh}}
	}

	if err := valid(one(KeeperShardConfig{Name: "default", Listen: ":9181", Members: []string{"k1:9181", "k2:9181"}, Strategy: "leader_pref"})); err != nil {
		t.Fatalf("valid keeper_proxy rejected: %v", err)
	}
	if err := valid(one(KeeperShardConfig{Name: "default", Listen: ":9181", Members: []string{"k1:9181"}, Strategy: ""})); err != nil {
		t.Fatalf("empty strategy must default, not error: %v", err)
	}
	// Multiple shards on distinct listen addresses are valid (§6).
	if err := valid(KeeperProxyConfig{Shards: []KeeperShardConfig{
		{Name: "default", Listen: ":9181", Members: []string{"k1:9181"}},
		{Name: "shard_2", Listen: ":9182", Members: []string{"k2:9181"}},
	}}); err != nil {
		t.Fatalf("two-shard keeper_proxy rejected: %v", err)
	}

	for name, sh := range map[string]KeeperShardConfig{
		"no name":          {Listen: ":9181", Members: []string{"k1:9181"}},
		"no members":       {Name: "default", Listen: ":9181"},
		"bad listen":       {Name: "default", Listen: "not-a-hostport", Members: []string{"k1:9181"}},
		"bad member":       {Name: "default", Listen: ":9181", Members: []string{"bad-member"}},
		"unknown strategy": {Name: "default", Listen: ":9181", Members: []string{"k1:9181"}, Strategy: "bogus"},
	} {
		if err := valid(one(sh)); err == nil {
			t.Errorf("keeper_proxy shard %q must error", name)
		}
	}

	// Duplicate shard name is rejected.
	if err := valid(KeeperProxyConfig{Shards: []KeeperShardConfig{
		{Name: "default", Listen: ":9181", Members: []string{"k1:9181"}},
		{Name: "default", Listen: ":9182", Members: []string{"k2:9181"}},
	}}); err == nil {
		t.Error("duplicate shard name must error")
	}
}

func TestConfig_Validate_InterserverMesh(t *testing.T) {
	base := minimalServerConfig(t)

	// Disabled (empty EgressListen) is valid.
	cfg := base
	if err := cfg.Validate(); err != nil {
		t.Fatalf("interserver_mesh disabled must be valid: %v", err)
	}

	// Three placeholder cert paths; Validate only checks they're non-empty,
	// it does not load them (loading happens in build.go).
	dir := t.TempDir()
	cert := filepath.Join(dir, "mesh.crt")
	key := filepath.Join(dir, "mesh.key")
	ca := filepath.Join(dir, "ca.crt")

	valid := func(m InterserverMeshConfig) error {
		c := base
		c.InterserverMesh = m
		return c.Validate()
	}

	full := InterserverMeshConfig{
		EgressListen:       "127.0.0.1:9010",
		IngressListen:      "0.0.0.0:19009",
		LocalCHInterserver: "127.0.0.2:9010",
		TLS:                InterserverMeshTLS{CertFile: cert, KeyFile: key, CAFile: ca},
	}
	if err := valid(full); err != nil {
		t.Fatalf("valid interserver_mesh rejected: %v", err)
	}

	cases := map[string]func(*InterserverMeshConfig){
		"bad egress_listen":        func(m *InterserverMeshConfig) { m.EgressListen = "nope" },
		"no ingress_listen":        func(m *InterserverMeshConfig) { m.IngressListen = "" },
		"bad ingress_listen":       func(m *InterserverMeshConfig) { m.IngressListen = "nope" },
		"no local_ch_interserver":  func(m *InterserverMeshConfig) { m.LocalCHInterserver = "" },
		"bad local_ch_interserver": func(m *InterserverMeshConfig) { m.LocalCHInterserver = "nope" },
		"no cert":                  func(m *InterserverMeshConfig) { m.TLS.CertFile = "" },
		"no key":                   func(m *InterserverMeshConfig) { m.TLS.KeyFile = "" },
		"no ca":                    func(m *InterserverMeshConfig) { m.TLS.CAFile = "" },
	}
	for name, mutate := range cases {
		m := full
		mutate(&m)
		if err := valid(m); err == nil {
			t.Errorf("interserver_mesh %q must error", name)
		}
	}
}
