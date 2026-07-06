package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/cluster"
	"housegate/housegate/pkg/plugins/agent"
	materializeplugin "housegate/housegate/pkg/plugins/materialize"
)

func TestDurationUnmarshal(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"5s"`), &d); err != nil {
		t.Fatalf("unmarshal duration string: %v", err)
	}
	if d.Duration != 5*time.Second {
		t.Fatalf("duration = %s, want 5s", d)
	}

	if err := json.Unmarshal([]byte(`1000000`), &d); err != nil {
		t.Fatalf("unmarshal duration number: %v", err)
	}
	if d.Duration != time.Duration(1000000) {
		t.Fatalf("duration = %d, want 1000000", d.Duration)
	}
}

func TestDefault(t *testing.T) {
	t.Setenv("HOUSEGATE_LISTEN", "0.0.0.0:19001")
	t.Setenv("HOUSEGATE_UPSTREAM", "127.0.0.1:19000")

	cfg := Default()
	if cfg.Listen != "0.0.0.0:19001" {
		t.Fatalf("Listen = %s, want env override", cfg.Listen)
	}
	if cfg.Upstream != "127.0.0.1:19000" {
		t.Fatalf("Upstream = %s, want env override", cfg.Upstream)
	}
	if cfg.StatsInterval.Duration == 0 || cfg.DialTimeout.Duration == 0 || cfg.IdleTimeout.Duration == 0 {
		t.Fatalf("expected non-zero defaults, got %+v", cfg)
	}
	if !cfg.Logging.Queries || cfg.Logging.MaxQueryBytes == 0 {
		t.Fatalf("expected Logging defaults populated, got %+v", cfg.Logging)
	}
	if cfg.Rewriter.Timeout.Duration == 0 || cfg.Rewriter.ServiceAddr == "" {
		t.Fatalf("expected Rewriter defaults populated, got %+v", cfg.Rewriter)
	}
}

func TestConfigMode(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want Mode
	}{
		{"agent beats everything", Config{Agent: agent.Config{Mode: true}, Upstream: "x", Shard: &cluster.ShardConfig{}}, ModeAgent},
		{"server with upstream", Config{Listen: ":9001", Upstream: "ch:9000"}, ModeServer},
		{"server with shard", Config{Listen: ":9001", Shard: &cluster.ShardConfig{}}, ModeServer},
		{"router-only server when nothing bound", Config{Listen: ":9001"}, ModeServer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Mode(); got != tt.want {
				t.Fatalf("Mode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadDispatchesByExtension(t *testing.T) {
	dir := t.TempDir()

	jsonBody := `{
		"listen": ":9101",
		"upstream": "ch-json:9000",
		"dial_timeout": "7s",
		"redis_default_addr": "redis-json:6379",
		"ckh_manager_config_path": "/etc/ckh.json",
		"auth": { "enabled": true, "max_token_age": "90s" }
	}`
	yamlBody := `
listen: ":9102"
upstream: "ch-yaml:9000"
dial_timeout: "11s"
redis_default_addr: "redis-yaml:6379"
ckh_manager_config_path: "/etc/ckh.yaml"
auth:
  enabled: true
  max_token_age: "120s"
shard:
  name: "shard-local"
  replicas:
    - host: "127.0.0.1"
      port: 9000
      weight: 1
health_check:
  interval: "6s"
`

	cases := []struct {
		name         string
		filename     string
		body         string
		wantListen   string
		wantUpstream string
		wantDial     time.Duration
		wantRedis    string
		wantMaxAge   time.Duration
	}{
		{"json by extension", "cfg.json", jsonBody, ":9101", "ch-json:9000", 7 * time.Second, "redis-json:6379", 90 * time.Second},
		{"yaml by extension", "cfg.yaml", yamlBody, ":9102", "ch-yaml:9000", 11 * time.Second, "redis-yaml:6379", 120 * time.Second},
		{"yml alias", "cfg.yml", yamlBody, ":9102", "ch-yaml:9000", 11 * time.Second, "redis-yaml:6379", 120 * time.Second},
		{"no extension falls back to json", "cfg", jsonBody, ":9101", "ch-json:9000", 7 * time.Second, "redis-json:6379", 90 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.filename)
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatalf("write temp cfg: %v", err)
			}
			cfg := Load(path)
			if cfg.Listen != tc.wantListen {
				t.Errorf("Listen = %q, want %q", cfg.Listen, tc.wantListen)
			}
			if cfg.Upstream != tc.wantUpstream {
				t.Errorf("Upstream = %q, want %q", cfg.Upstream, tc.wantUpstream)
			}
			if cfg.DialTimeout.Duration != tc.wantDial {
				t.Errorf("DialTimeout = %s, want %s", cfg.DialTimeout, tc.wantDial)
			}
			if cfg.RedisDefaultAddr != tc.wantRedis {
				t.Errorf("RedisDefaultAddr = %q, want %q", cfg.RedisDefaultAddr, tc.wantRedis)
			}
			if cfg.Auth.MaxTokenAge.Duration != tc.wantMaxAge {
				t.Errorf("Auth.MaxTokenAge = %s, want %s", cfg.Auth.MaxTokenAge, tc.wantMaxAge)
			}
			if !cfg.Auth.Enabled {
				t.Errorf("Auth.Enabled = false, want true")
			}
		})
	}

	t.Run("yaml shard+health_check durations parse", func(t *testing.T) {
		path := filepath.Join(dir, "with_shard.yaml")
		if err := os.WriteFile(path, []byte(yamlBody), 0o600); err != nil {
			t.Fatalf("write temp cfg: %v", err)
		}
		cfg := Load(path)
		if cfg.Shard == nil || cfg.Shard.Name != "shard-local" || len(cfg.Shard.Replicas) != 1 {
			t.Fatalf("Shard not parsed correctly: %+v", cfg.Shard)
		}
		if cfg.HealthCheck == nil || cfg.HealthCheck.Interval.Duration != 6*time.Second {
			t.Fatalf("HealthCheck.Interval = %+v, want 6s", cfg.HealthCheck)
		}
	})
}

func TestResolveRedisAddr(t *testing.T) {
	cfg := Config{RedisDefaultAddr: "default:6379"}
	if got := cfg.ResolveRedisAddr(""); got != "default:6379" {
		t.Errorf("empty section addr should fall back to default, got %q", got)
	}
	if got := cfg.ResolveRedisAddr("plugin:6380"); got != "plugin:6380" {
		t.Errorf("non-empty section addr should win, got %q", got)
	}
	empty := Config{}
	if got := empty.ResolveRedisAddr(""); got != "" {
		t.Errorf("no default, empty section addr should resolve to empty, got %q", got)
	}
}

func minimalServerConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Listen:               ":9001",
		Upstream:             "ch:9000",
		RedisDefaultAddr:     "localhost:6379",
		CkhManagerConfigPath: "/etc/ckh.json",
	}
}

func TestConfig_Validate_ServerMode_InternalListenOptional(t *testing.T) {
	cfg := minimalServerConfig(t)
	cfg.InternalListen = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("internal_listen empty must be valid (back-compat): %v", err)
	}

	cfg.InternalListen = "0.0.0.0:9001"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("internal_listen set must be valid: %v", err)
	}

	cfg.InternalListen = "not-a-host-port"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("internal_listen with malformed addr must error")
	}
}

func TestConfigValidateStorageIntegrityRequiresExternalEndpoints(t *testing.T) {
	cfg := minimalServerConfig(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.UnsafeDatabase = "hg_unsafe"
	cfg.StorageIntegrity.SafeDatabase = "hg_safe"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.da_endpoint is required") {
		t.Fatalf("Validate missing DA endpoint err = %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.arbiter_endpoint is required") {
		t.Fatalf("Validate missing Arbiter endpoint err = %v", err)
	}

	cfg.StorageIntegrity.DAEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.ArbiterEndpoint = "http://127.0.0.1:18080"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with storage_integrity endpoints: %v", err)
	}
}

func TestConfigValidateStorageIntegrityAcceptsLegacySequencerEndpoint(t *testing.T) {
	cfg := minimalServerConfig(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.DAEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.SequencerEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.UnsafeDatabase = "hg_unsafe"
	cfg.StorageIntegrity.SafeDatabase = "hg_safe"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with legacy sequencer_endpoint: %v", err)
	}
}

func TestConfigValidateStorageIntegrityMutationAdmission(t *testing.T) {
	cfg := minimalServerConfig(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.DAEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.ArbiterEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.UnsafeDatabase = "hg_unsafe"
	cfg.StorageIntegrity.SafeDatabase = "hg_safe"
	cfg.StorageIntegrity.Mutations.Enabled = true
	cfg.StorageIntegrity.Mutations.ScratchDatabase = "hg_mutation"
	cfg.StorageIntegrity.Mutations.RequirePartitionPredicate = true
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.mutations.partition_columns is required") {
		t.Fatalf("Validate missing mutation partition columns err = %v", err)
	}

	cfg.StorageIntegrity.Mutations.PartitionColumns = []string{"day"}
	cfg.StorageIntegrity.Mutations.ProtectedColumns = []string{"day", "id"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with mutation admission config: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	baseServer := minimalServerConfig(t)
	baseAgent := Config{
		Listen: ":9001",
		Agent: agent.Config{
			Mode:          true,
			Upstream:      "proxy:9001",
			PrivateKeyHex: "0xdeadbeef",
		},
	}
	// Router-only server: no shard, no upstream — server mode forwards
	// every session to a peer via NetworkState. CkhManagerConfigPath is
	// unused (no rewriter runs) so it is intentionally absent here.
	baseRouterOnly := Config{
		Listen:           ":9001",
		RedisDefaultAddr: "localhost:6379",
	}

	tests := []struct {
		name        string
		mutate      func(*Config)
		cfg         Config
		wantErr     bool
		wantContain []string // substrings the error must contain
	}{
		{name: "server happy path", cfg: baseServer, wantErr: false},
		{name: "agent happy path", cfg: baseAgent, wantErr: false},
		{name: "router-only server happy path", cfg: baseRouterOnly, wantErr: false},

		{
			name: "server missing redis (both defaults and section empty)",
			cfg:  baseServer, mutate: func(c *Config) { c.RedisDefaultAddr = "" },
			wantErr: true, wantContain: []string{"network_state.source"},
		},
		{
			name: "server resolves redis via section addr when default empty",
			cfg:  baseServer,
			mutate: func(c *Config) {
				c.RedisDefaultAddr = ""
				c.NetworkState.Source = "scoped:6379"
			},
			wantErr: false,
		},
		{
			name: "server missing ckh_manager_config_path",
			cfg:  baseServer, mutate: func(c *Config) { c.CkhManagerConfigPath = "" },
			wantErr: true, wantContain: []string{"ckh_manager_config_path"},
		},
		{
			name: "agent missing private key",
			cfg:  baseAgent, mutate: func(c *Config) { c.Agent.PrivateKeyHex = "" },
			wantErr: true, wantContain: []string{"agent.private_key_hex"},
		},
		{
			name: "agent owner optional", cfg: baseAgent,
			mutate:  func(c *Config) { c.Agent.Owner = "" },
			wantErr: false,
		},
		{
			name: "agent owner valid hex address", cfg: baseAgent,
			mutate:  func(c *Config) { c.Agent.Owner = "0x1111111111111111111111111111111111111111" },
			wantErr: false,
		},
		{
			name: "agent owner invalid hex address", cfg: baseAgent,
			mutate:  func(c *Config) { c.Agent.Owner = "not-an-address" },
			wantErr: true, wantContain: []string{"agent.owner"},
		},
		{
			name: "agent missing upstream and network_state",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.Agent.Upstream = ""
				c.NetworkState.Source = ""
				c.RedisDefaultAddr = ""
			},
			wantErr: true, wantContain: []string{"agent.upstream", "network_state"},
		},
		{
			name: "agent auto-discovery via yaml network_state",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.Agent.Upstream = ""
				c.NetworkState.Source = "configs/local.network_state.yaml"
			},
			wantErr: false,
		},
		{
			name: "agent auto-discovery via redis network_state",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.Agent.Upstream = ""
				c.RedisDefaultAddr = "127.0.0.1:6379"
			},
			wantErr: false,
		},
		{
			name: "agent auto-discovery via rpc network_state",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.Agent.Upstream = ""
				c.RedisDefaultAddr = ""
				c.NetworkState.Source = "http://node.example.com:10003"
			},
			wantErr: false,
		},
		{
			name: "agent auto-discovery via https rpc",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.Agent.Upstream = ""
				c.RedisDefaultAddr = ""
				c.NetworkState.Source = "https://node.example.com:10003"
			},
			wantErr: false,
		},
		{
			name: "server rejects rpc network_state",
			cfg:  baseServer, mutate: func(c *Config) {
				c.RedisDefaultAddr = ""
				c.NetworkState.Source = "http://node.example.com:10003"
			},
			wantErr: true, wantContain: []string{"RPC backend", "agent-mode only"},
		},
		{
			name: "router-only server missing redis",
			cfg:  baseRouterOnly, mutate: func(c *Config) { c.RedisDefaultAddr = "" },
			wantErr: true, wantContain: []string{"network_state.source"},
		},
		{
			name: "missing listen",
			cfg:  baseServer, mutate: func(c *Config) { c.Listen = "" },
			wantErr: true, wantContain: []string{"listen address is required"},
		},
		{
			name: "negative buf size",
			cfg:  baseServer, mutate: func(c *Config) { c.StreamingBufSize = -1 },
			wantErr: true, wantContain: []string{"streaming_buf_size"},
		},
		{
			name:    "server reports all missing fields at once",
			cfg:     Config{Listen: ":9001", Upstream: "ch:9000"},
			wantErr: true, wantContain: []string{"network_state.source", "ckh_manager_config_path"},
		},
		{
			name: "agent ignores server-mode requirements",
			cfg:  baseAgent, mutate: func(c *Config) {
				c.RedisDefaultAddr = ""
				c.CkhManagerConfigPath = ""
			},
			wantErr: false,
		},
		{
			name: "concurrency_limit.enabled requires redis",
			cfg:  baseServer, mutate: func(c *Config) {
				c.ConcurrencyLimit.Enabled = true
				c.RedisDefaultAddr = "" // remove fallback too
				c.NetworkState.Source = "ns:6379"
			},
			wantErr: true, wantContain: []string{"concurrency_limit.redis_addr"},
		},
		{
			name: "usage.enabled requires redis",
			cfg:  baseServer, mutate: func(c *Config) {
				c.Usage.Enabled = true
				c.RedisDefaultAddr = ""
				c.NetworkState.Source = "ns:6379"
			},
			wantErr: true, wantContain: []string{"usage.redis_addr"},
		},
		{
			name: "concurrency_limit resolves via section addr",
			cfg:  baseServer, mutate: func(c *Config) {
				c.ConcurrencyLimit.Enabled = true
				c.ConcurrencyLimit.RedisAddr = "limiter:6380"
			},
			wantErr: false,
		},
		{
			name:        "rewriter_engine_invalid",
			cfg:         baseServer,
			mutate:      func(c *Config) { c.Rewriter.Engine = "carrier-pigeon" },
			wantErr:     true,
			wantContain: []string{"rewriter.engine"},
		},
		{
			name:    "rewriter_engine_grpc_explicit_ok",
			cfg:     baseServer,
			mutate:  func(c *Config) { c.Rewriter.Engine = "grpc" },
			wantErr: false,
		},
		{
			name:    "rewriter_engine_native_ok",
			cfg:     baseServer,
			mutate:  func(c *Config) { c.Rewriter.Engine = "native" },
			wantErr: false,
		},
		{
			name:    "rewriter_engine_empty_ok",
			cfg:     baseServer,
			mutate:  func(c *Config) { c.Rewriter.Engine = "" },
			wantErr: false,
		},
		{
			name:        "rewriter_engine_invalid_agent_mode",
			cfg:         baseAgent,
			mutate:      func(c *Config) { c.Rewriter.Engine = "carrier-pigeon" },
			wantErr:     true,
			wantContain: []string{"rewriter.engine"},
		},
		{
			name:    "rewriter_engine_native_ok_agent_mode",
			cfg:     baseAgent,
			mutate:  func(c *Config) { c.Rewriter.Engine = "native" },
			wantErr: false,
		},
		{
			name:        "rewriter_native_sha256_invalid",
			cfg:         baseServer,
			mutate:      func(c *Config) { c.Rewriter.NativeLibrarySHA256 = "not-hex" },
			wantErr:     true,
			wantContain: []string{"native_library_sha256"},
		},
		{
			name:    "rewriter_native_sha256_ok",
			cfg:     baseServer,
			mutate:  func(c *Config) { c.Rewriter.NativeLibrarySHA256 = strings.Repeat("ab", 32) },
			wantErr: false,
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

func TestValidate_Materialize(t *testing.T) {
	base := func() *Config {
		c := Default()
		c.Agent.Mode = true
		c.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		c.Agent.Upstream = "127.0.0.1:9000"
		return &c
	}
	t.Run("enabled without engine is rejected", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		if err := c.Validate(); err == nil {
			t.Fatal("want error: engine required when enabled")
		}
	})
	t.Run("grpc without service_addr is rejected", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		c.Materialize.Engine = "grpc"
		c.Materialize.ServiceAddr = ""
		if err := c.Validate(); err == nil {
			t.Fatal("want error: service_addr required for grpc")
		}
	})
	t.Run("native enabled validates", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		c.Materialize.Engine = "native"
		if err := c.Validate(); err != nil {
			t.Fatalf("native enabled should validate: %v", err)
		}
	})
	t.Run("disabled block ignored", func(t *testing.T) {
		c := base()
		c.Materialize = materializeplugin.Config{} // all zero, Enabled=false
		if err := c.Validate(); err != nil {
			t.Fatalf("disabled materialize must not affect validation: %v", err)
		}
	})
	t.Run("default pool size is 16", func(t *testing.T) {
		d := Default()
		if got := d.Materialize.RandomPoolSize; got != 16 {
			t.Fatalf("default RandomPoolSize = %d, want 16", got)
		}
		if got := d.Materialize.Enabled; got != false {
			t.Fatalf("default Materialize.Enabled = %v, want false", got)
		}
		if got := d.Materialize.Timeout.Duration; got != 5*time.Second {
			t.Fatalf("default Materialize.Timeout = %v, want 5s", got)
		}
	})
	t.Run("negative pool size is rejected", func(t *testing.T) {
		c := base()
		c.Materialize.Enabled = true
		c.Materialize.Engine = "native"
		c.Materialize.RandomPoolSize = -1
		if err := c.Validate(); err == nil {
			t.Fatal("want error: random_pool_size must be >= 0")
		}
	})
}
