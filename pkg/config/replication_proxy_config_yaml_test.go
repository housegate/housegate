package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigValidate_ReplicationProxy_loadsYAMLSurface(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replication-proxy.yaml")
	body := `
listen: ":9001"
upstream: "ch:9000"
redis_default_addr: "localhost:6379"
ckh_manager_config_path: "/etc/ckh.yaml"
relay_private_key_hex: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
auth:
  enabled: true
  allowed_addresses:
    - "0x0000000000000000000000000000000000001001"
  max_token_age: "1m"
replication_proxy:
  keeper:
    enabled: true
    listen: "127.0.0.1:9181"
    upstreams:
      - "keeper-1:9181"
    dial_timeout: "5s"
  interserver:
    enabled: true
    listen: "127.0.0.1:9010"
    local_upstream: "127.0.0.1:9009"
    peer_user_header: "X-Housegate-Peer-User"
    peer_token_header: "X-Housegate-Peer-Token"
    dial_timeout: "5s"
    read_timeout: "30s"
    write_timeout: "30s"
    routes:
      - peer: "1001"
        upstream: "peer-2.example:9010"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp cfg: %v", err)
	}

	cfg := Load(path)
	err := cfg.Validate()

	if err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
	if !cfg.ReplicationProxy.Keeper.Enabled || cfg.ReplicationProxy.Keeper.Listen != "127.0.0.1:9181" {
		t.Fatalf("keeper surface parsed incorrectly: %+v", cfg.ReplicationProxy.Keeper)
	}
	if !cfg.ReplicationProxy.Interserver.Enabled || len(cfg.ReplicationProxy.Interserver.Routes) != 1 {
		t.Fatalf("interserver surface parsed incorrectly: %+v", cfg.ReplicationProxy.Interserver)
	}
	t.Logf("parsed replication_proxy keeper.listen=%s keeper.upstreams=%v interserver.listen=%s interserver.local_upstream=%s interserver.routes=%+v",
		cfg.ReplicationProxy.Keeper.Listen,
		cfg.ReplicationProxy.Keeper.Upstreams,
		cfg.ReplicationProxy.Interserver.Listen,
		cfg.ReplicationProxy.Interserver.LocalUpstream,
		cfg.ReplicationProxy.Interserver.Routes)
}
