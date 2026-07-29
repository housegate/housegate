// Package config defines the runtime configuration for housegate and the
// validation/loading logic operators invoke at startup.
//
// Each plugin owns its own Config struct in its plugin package; this
// file composes them into a single root Config and adds top-level
// fields (proxy lifecycle, cross-cutting credentials, dispatch
// switches). Adding a new plugin therefore costs one import and one
// field here — the type itself stays next to the plugin code.
//
// Redis is per-plugin: any section that needs Redis has its own
// RedisAddr field, and RedisDefaultAddr at the top level is the
// fallback for sections that leave theirs blank.
package config

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/log"

	"go.yaml.in/yaml/v3"

	"github.com/housegate/housegate/pkg/cfgtypes"
	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/plugins/agent"
	authplugin "github.com/housegate/housegate/pkg/plugins/auth"
	"github.com/housegate/housegate/pkg/plugins/concurrency"
	indexingusage "github.com/housegate/housegate/pkg/plugins/indexing_usage"
	lthashplugin "github.com/housegate/housegate/pkg/plugins/lthash"
	materializeplugin "github.com/housegate/housegate/pkg/plugins/materialize"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sessionstate"
	"github.com/housegate/housegate/pkg/plugins/usage"
	"github.com/housegate/housegate/pkg/rewriter"
)

// Duration is re-exported from cfgtypes so existing callers that
// reference config.Duration keep working.
type Duration = cfgtypes.Duration

// Config is the root configuration loaded from JSON. All fields have
// sane defaults so the binary can boot without a config file.
type Config struct {
	// --- Network & lifecycle (top-level) ---

	Listen string `json:"listen"                   yaml:"listen"`
	// InternalListen is the bind address for peer-only traffic
	// (__peer__-authenticated handshakes from other server-mode proxies).
	// Empty = disabled (back-compat); set to a host:port to enable.
	InternalListen        string   `json:"internal_listen"          yaml:"internal_listen"`
	Upstream              string   `json:"upstream"                 yaml:"upstream"`
	CallbackUrl           string   `json:"callback_url"             yaml:"callback_url"`
	MetricsListen         string   `json:"metrics_listen"           yaml:"metrics_listen"`
	DialTimeout           Duration `json:"dial_timeout"             yaml:"dial_timeout"`
	IdleTimeout           Duration `json:"idle_timeout"             yaml:"idle_timeout"`
	ShutdownTimeout       Duration `json:"shutdown_timeout"         yaml:"shutdown_timeout"`
	StatsInterval         Duration `json:"stats_interval"           yaml:"stats_interval"`
	MaxConnectionLifetime Duration `json:"max_connection_lifetime"  yaml:"max_connection_lifetime"`
	StreamingBufSize      int      `json:"streaming_buf_size"       yaml:"streaming_buf_size"`
	ValidateChecksum      bool     `json:"validate_checksum"        yaml:"validate_checksum"`

	// LogLevel is the package-default log level applied by cmd/housegate
	// at startup via log.SetLevel. Case-insensitive; accepts "debug",
	// "info", "warn", "error", "fatal", as well as slog's offset syntax
	// ("DEBUG+1"). Empty = "info". Library hosts that inject their own
	// *slog.Logger via Options.Logger should manage level on their own
	// handler — this field only governs the default Logger.
	LogLevel string `json:"log_level"                yaml:"log_level"`

	// LogFile redirects cmd/housegate's own log output (housegate code +
	// any transitive deps that honour the -log-file flag) to the
	// given path. Empty = stderr. ANSI color is disabled when writing to
	// a file. Operators should manage rotation externally (logrotate /
	// copy-truncate); the file is opened O_APPEND|O_CREATE. Like
	// LogLevel, this only governs the cmd binary's default Logger —
	// library hosts manage their own destination via Options.Logger.
	LogFile string `json:"log_file"                 yaml:"log_file"`

	// --- Cross-cutting credentials & integrations (top-level) ---

	// IndexerID identifies this proxy instance in NetworkState. Used
	// by the rewriter to decide whether a logical-database-keyed
	// remote upstream is "this proxy" (local pass-through) or a peer
	// (remote() emission), and as the audience claim of inbound
	// peer-relay JWS tokens. Optional; library hosts can supply a
	// dynamic getter via Options.GetIndexerId instead.
	IndexerID uint64 `json:"indexer_id"                yaml:"indexer_id"`

	// RelayPrivateKeyHex is the proxy-level secp256k1 signing key
	// used both for the existing relay/agent SQL-binding JWS and
	// for the peer-relay JWS injected into rewritten remote() calls.
	// Hex with or without a leading 0x. Empty disables both signers.
	RelayPrivateKeyHex string `json:"relay_private_key_hex"     yaml:"relay_private_key_hex"`

	// RelayPublicKeyHex is the corresponding uncompressed secp256k1
	// public key. Optional and informational — the signing path
	// derives the address from RelayPrivateKeyHex on its own. Stored
	// here so library hosts that publish peer identity into a
	// registry / NetworkState have a single configured source.
	RelayPublicKeyHex string `json:"relay_public_key_hex"      yaml:"relay_public_key_hex"`

	// PeerTokenTTL overrides the default validity window of the
	// peer-relay JWS the rewriter mints for outbound remote() calls.
	// The TTL must outlive ClickHouse's longest expected re-handshake
	// against a stale cluster connection (pool eviction, idle close,
	// MV refresh holding the original SQL, etc.); empty / zero falls
	// back to the rewriter package default (1h).
	PeerTokenTTL Duration `json:"peer_token_ttl"            yaml:"peer_token_ttl"`

	CkhManagerConfigPath     string `json:"ckh_manager_config_path"   yaml:"ckh_manager_config_path"`
	CredentialReplaceEnabled bool   `json:"credential_replace_enabled" yaml:"credential_replace_enabled"`

	// --- Dispatch switches (top-level) ---
	//
	// --- Redis default (top-level fallback) ---

	RedisDefaultAddr string `json:"redis_default_addr" yaml:"redis_default_addr"`

	// --- Cluster topology (already hierarchical; owned by pkg/cluster) ---

	Shard       *cluster.ShardConfig       `json:"shard,omitempty"        yaml:"shard,omitempty"`
	HealthCheck *cluster.HealthCheckConfig `json:"health_check,omitempty" yaml:"health_check,omitempty"`
	Pool        *cluster.PoolConfig        `json:"pool,omitempty"         yaml:"pool,omitempty"`
	Routing     *cluster.RoutingConfig     `json:"routing,omitempty"      yaml:"routing,omitempty"`

	// --- Feature sections (each owned by its plugin package) ---

	Auth             authplugin.Config        `json:"auth"              yaml:"auth"`
	Rewriter         rewrite.Config           `json:"rewriter"          yaml:"rewriter"`
	Materialize      materializeplugin.Config `json:"materialize"       yaml:"materialize"`
	Agent            agent.Config             `json:"agent"             yaml:"agent"`
	Usage            usage.Config             `json:"usage"             yaml:"usage"`
	IndexingUsage    indexingusage.Config     `json:"indexing_usage"    yaml:"indexing_usage"`
	ConcurrencyLimit concurrency.Config       `json:"concurrency_limit" yaml:"concurrency_limit"`
	State            sessionstate.Config      `json:"state"             yaml:"state"`
	LtHash           lthashplugin.Config      `json:"lthash"            yaml:"lthash"`
	StorageIntegrity StorageIntegrityConfig   `json:"storage_integrity" yaml:"storage_integrity"`

	// --- Plumbing sections owned by pkg/config ---

	Logging          LoggingConfig          `json:"logging"          yaml:"logging"`
	NetworkState     NetworkStateConfig     `json:"network_state"    yaml:"network_state"`
	ReplicationProxy ReplicationProxyConfig `json:"replication_proxy" yaml:"replication_proxy"`

	// Observability owns the metrics Collector + pprof config. Read by
	// buildServer to construct the pkg/metrics Collector; no plugin owns it.
	Observability ObservabilityConfig `json:"observability" yaml:"observability"`
}

// LoggingConfig controls what the proxy writes to operator logs. Owned
// here because no single plugin "owns" logging.
type LoggingConfig struct {
	Queries       bool `json:"queries"          yaml:"queries"`
	Data          bool `json:"data"             yaml:"data"`
	MaxQueryBytes int  `json:"max_query_bytes"  yaml:"max_query_bytes"`
	MaxDataBytes  int  `json:"max_data_bytes"   yaml:"max_data_bytes"`
}

// ObservabilityConfig configures downstream metrics collection and the
// authenticated pprof endpoint. Owned by pkg/config (plumbing) and read by
// buildServer; the pkg/metrics Collector takes plain values, so there is no
// config -> metrics import.
type ObservabilityConfig struct {
	Collector CollectorConfig `json:"collector" yaml:"collector"`
	Pprof     PprofConfig     `json:"pprof"     yaml:"pprof"`
}

// CollectorConfig controls the background metrics Collector. CHAddr optionally
// overrides the ClickHouse poll target (default: the shard replicas / upstream).
// The collector's ClickHouse credentials always come from the ckh_manager config
// (ckh_manager_config_path) — there are no separate collector credential fields.
type CollectorConfig struct {
	Enabled     bool     `json:"enabled"      yaml:"enabled"`
	Interval    Duration `json:"interval"     yaml:"interval"`
	PollTimeout Duration `json:"poll_timeout" yaml:"poll_timeout"`
	CHAddr      string   `json:"ch_addr"      yaml:"ch_addr"`
}

// PprofConfig gates the /debug/pprof endpoint. Disabled by default; when
// enabled, Token is required (bearer auth) — operators are encouraged to keep
// it in an age-encrypted config rather than plaintext.
type PprofConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Token   string `json:"token"   yaml:"token"`
}

// NetworkStateConfig configures the NetworkState consumer (proxy
// infrastructure, not a plugin).
//
// One polymorphic field, auto-detected by suffix / scheme:
//   - `.yaml` / `.yml` → load the file into InMemoryNetworkState at
//     startup (local development, integration tests, no Redis
//     dependency). Schema is documented in pkg/network/yaml.go and an
//     example sits at configs/local.network_state.yaml.
//   - `http://` / `https://` → treat as a sentio storage-node JSON-RPC
//     endpoint and back the State with RpcNetworkState. Agent-only;
//     server mode rejects this scheme.
//   - anything else → treat as a Redis address for the statemirror
//     consumer. Empty falls back to top-level `redis_default_addr`.
//
// Operators don't need to declare which backend they want; the value
// itself signals it.
type NetworkStateConfig struct {
	// Source is either a YAML file path, an http(s) RPC URL, or a
	// Redis address. See the type-level comment for the dispatch rules.
	Source string `json:"source" yaml:"source"`
}

// IsYAMLSource reports whether Source points to a YAML fixture file.
func (c NetworkStateConfig) IsYAMLSource() bool {
	return strings.HasSuffix(c.Source, ".yaml") || strings.HasSuffix(c.Source, ".yml")
}

// IsRpcSource reports whether Source is an http(s) URL pointing at a
// sentio storage-node JSON-RPC endpoint.
func (c NetworkStateConfig) IsRpcSource() bool {
	return strings.HasPrefix(c.Source, "http://") || strings.HasPrefix(c.Source, "https://")
}

// ResolveRedisAddr returns sectionAddr if non-empty, otherwise the
// top-level RedisDefaultAddr.
func (c *Config) ResolveRedisAddr(sectionAddr string) string {
	if sectionAddr != "" {
		return sectionAddr
	}
	return c.RedisDefaultAddr
}

// Mode returns the active runtime mode implied by the config.
//
// Two runtime modes:
//   - ModeAgent — agent.mode = true. Signs queries with an Ethereum
//     key, forwards them to a remote server-mode proxy.
//   - ModeServer — everything else. Hosts client connections, runs the
//     full plugin chain, and routes to local replicas (when shard or
//     upstream is configured) or peers (when neither is — "router-only"
//     deployment, equivalent to the legacy forwarding-only mode).
func (c *Config) Mode() Mode {
	if c.Agent.Mode {
		return ModeAgent
	}
	return ModeServer
}

// Mode enumerates the proxy's runtime modes.
type Mode int

const (
	ModeServer Mode = iota
	ModeAgent
)

func (m Mode) String() string {
	switch m {
	case ModeServer:
		return "server"
	case ModeAgent:
		return "agent"
	default:
		return "unknown"
	}
}

// Validate performs startup-time consistency checks on the config and
// aggregates every problem it finds into a single error so operators
// can fix them in one pass.
func (c *Config) Validate() error {
	var errs []error

	if c.Listen == "" {
		errs = append(errs, errors.New("listen address is required"))
	}
	if c.StreamingBufSize < 0 {
		errs = append(errs, fmt.Errorf("streaming_buf_size must be >= 0 (got %d)", c.StreamingBufSize))
	}
	if _, err := log.ParseLevel(c.LogLevel); err != nil {
		errs = append(errs, fmt.Errorf("log_level: %w", err))
	}

	switch c.Mode() {
	case ModeAgent:
		if err := c.Agent.Validate(); err != nil {
			errs = append(errs, err)
		}
		// The agent's upstream may come from either an explicit address
		// or an auto-discovery NetworkState lookup. At least one must be
		// configured.
		if c.Agent.Upstream == "" &&
			!c.NetworkState.IsYAMLSource() &&
			!c.NetworkState.IsRpcSource() &&
			c.ResolveRedisAddr(c.NetworkState.Source) == "" {
			errs = append(errs, errors.New("agent.upstream is required, or set network_state.source to a YAML path (.yaml/.yml), an RPC URL (http(s)://), or a Redis address (or top-level redis_default_addr) for upstream auto-discovery"))
		}
		// Materialize is an agent-mode-only feature (Phase-1 non-determinism
		// materialization). Gating validation here means a stray
		// materialize block in a server-mode config is silently ignored
		// rather than rejected.
		if err := c.Materialize.Validate(); err != nil {
			errs = append(errs, err)
		}
	case ModeServer:
		// NetworkState is needed both for cross-shard rewriter routing
		// (when shard/upstream is set) and for router-only fallback
		// dialing (when neither is set). It is therefore always
		// required in server mode. The rpc backend exposes only the
		// methods agent.Selector needs and would silently misbehave
		// for server-mode lookups, so reject it explicitly.
		switch {
		case c.NetworkState.IsYAMLSource():
			// ok
		case c.NetworkState.IsRpcSource():
			errs = append(errs, errors.New("network_state.source: RPC backend (http(s)://) is agent-mode only — use a YAML path (.yaml/.yml) or a Redis address in server mode"))
		case c.ResolveRedisAddr(c.NetworkState.Source) != "":
			// ok
		default:
			errs = append(errs, errors.New("network_state.source must be a YAML path (.yaml/.yml) or a Redis address (or set top-level redis_default_addr) in server mode"))
		}
		// CkhManagerConfigPath is only needed when this server hosts a
		// local shard or has an explicit upstream — i.e. when the SQL
		// rewriter runs. A router-only server (no shard, no upstream)
		// never invokes the rewriter and so doesn't need a manager.
		if (c.Shard != nil || c.Upstream != "") && c.CkhManagerConfigPath == "" {
			errs = append(errs, errors.New("ckh_manager_config_path is required when upstream or shard is configured (needed for SQL rewriter table mapping)"))
		}
	}

	// Cross-section Redis requirements.
	if c.ConcurrencyLimit.Enabled && c.ResolveRedisAddr(c.ConcurrencyLimit.RedisAddr) == "" {
		errs = append(errs, errors.New("concurrency_limit.redis_addr (or top-level redis_default_addr) is required when concurrency_limit.enabled"))
	}
	if c.Usage.Enabled && c.ResolveRedisAddr(c.Usage.RedisAddr) == "" {
		errs = append(errs, errors.New("usage.redis_addr (or top-level redis_default_addr) is required when usage.enabled"))
	}

	if c.InternalListen != "" {
		if _, _, err := net.SplitHostPort(c.InternalListen); err != nil {
			errs = append(errs, fmt.Errorf("internal_listen: invalid host:port %q: %w", c.InternalListen, err))
		}
	}
	if err := c.StorageIntegrity.validate(c.Mode()); err != nil {
		errs = append(errs, err)
	}
	if err := c.ReplicationProxy.validate(*c, c.Mode()); err != nil {
		errs = append(errs, err)
	}

	// Observability.
	if c.Observability.Pprof.Enabled && c.Observability.Pprof.Token == "" {
		errs = append(errs, errors.New("observability.pprof.token is required when observability.pprof.enabled is true"))
	}
	if c.Observability.Collector.Enabled {
		if c.Observability.Collector.Interval.Duration <= 0 {
			errs = append(errs, errors.New("observability.collector.interval must be > 0 when the collector is enabled"))
		}
		if c.Observability.Collector.PollTimeout.Duration <= 0 {
			errs = append(errs, errors.New("observability.collector.poll_timeout must be > 0 when the collector is enabled"))
		}
	}

	// Engine validity is mode-independent — both server and agent can
	// configure a non-default engine for test or migration purposes.
	switch c.Rewriter.Engine {
	case "", rewriter.EngineGRPC, rewriter.EngineNative:
		// ok
	default:
		errs = append(errs, fmt.Errorf("rewriter.engine %q is invalid (want %q or %q)",
			c.Rewriter.Engine, rewriter.EngineGRPC, rewriter.EngineNative))
	}

	if s := c.Rewriter.NativeLibrarySHA256; s != "" {
		if raw, err := hex.DecodeString(s); err != nil || len(raw) != 32 {
			errs = append(errs, fmt.Errorf("rewriter.native_library_sha256 must be 64 hex chars, got %q", s))
		}
	}

	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("invalid config (mode=%s):\n%w", c.Mode(), joined)
	}
	return nil
}

// ValidateExceptNetworkStateSource runs Validate() but treats an empty
// NetworkState.Source as valid in server / forwarding-only mode. Used
// by housegate.New when the embedding host injects Options.NetworkState
// — sentio-node is the canonical case, where the embedded library
// receives a chain-backed network.State directly and the operator-
// visible source field is irrelevant. Hosts that don't inject a
// NetworkState must use Validate().
//
// Implementation note: this temporarily mutates c.NetworkState.Source
// to a sentinel during the Validate call and restores the original
// value before returning, so callers cannot observe the mutation.
func (c *Config) ValidateExceptNetworkStateSource() error {
	saved := c.NetworkState.Source
	defer func() { c.NetworkState.Source = saved }()
	if saved == "" && c.ResolveRedisAddr("") == "" {
		// Inject a dummy redis-style value (NOT a yaml suffix) so the
		// Source-required rule passes without changing dispatch in
		// loadNetworkState — which we won't call anyway when injection
		// is provided.
		c.NetworkState.Source = "injected:0"
	}
	return c.Validate()
}

// Default returns a Config populated from environment variables where
// applicable. Env vars stay flat (HOUSEGATE_*) because deployment tooling
// doesn't reflect JSON nesting well.
func Default() Config {
	return Config{
		// Network & lifecycle
		Listen:                EnvOrDefault("HOUSEGATE_LISTEN", ":9001"),
		Upstream:              os.Getenv("HOUSEGATE_UPSTREAM"),
		MetricsListen:         EnvOrDefault("HOUSEGATE_METRICS_LISTEN", ":9091"),
		DialTimeout:           Duration{5 * time.Second},
		IdleTimeout:           Duration{5 * time.Minute},
		ShutdownTimeout:       Duration{30 * time.Second},
		StatsInterval:         Duration{10 * time.Second},
		MaxConnectionLifetime: Duration{24 * time.Hour},
		StreamingBufSize:      131072,
		ValidateChecksum:      false,

		// Cross-cutting
		IndexerID:                envUint64("HOUSEGATE_INDEXER_ID"),
		RelayPrivateKeyHex:       EnvOrDefault("HOUSEGATE_PRIVATE_KEY", ""),
		RelayPublicKeyHex:        EnvOrDefault("HOUSEGATE_PUBLIC_KEY", ""),
		CkhManagerConfigPath:     EnvOrDefault("CKH_MANAGER_CONFIG", ""),
		CredentialReplaceEnabled: true,

		// Redis
		RedisDefaultAddr: EnvOrDefault("HOUSEGATE_REDIS_ADDR", ""),

		// Feature sections
		Auth: authplugin.Config{
			MaxTokenAge: Duration{1 * time.Minute},
		},
		Rewriter: rewrite.Config{
			ServiceAddr: EnvOrDefault("HOUSEGATE_REWRITER_ADDR", "localhost:50051"),
			Timeout:     Duration{5 * time.Second},
			Engine:      EnvOrDefault("HOUSEGATE_REWRITER_ENGINE", ""),
		},
		Materialize: materializeplugin.Config{
			Timeout:        Duration{5 * time.Second},
			RandomPoolSize: 16,
		},
		Agent: agent.Config{
			Mode:          EnvOrDefault("HOUSEGATE_AGENT", "") == "true",
			Upstream:      EnvOrDefault("HOUSEGATE_AGENT_UPSTREAM", ""),
			PrivateKeyHex: EnvOrDefault("HOUSEGATE_AGENT_KEY", ""),
			Owner:         EnvOrDefault("HOUSEGATE_AGENT_OWNER", ""),
			Driver:        EnvOrDefault("HOUSEGATE_AGENT_DRIVER", "") == "true",
		},
		Usage: usage.Config{
			Enabled: false,
		},
		ConcurrencyLimit: concurrency.Config{
			Enabled:  false,
			PerUser:  0,
			Timeout:  Duration{60 * time.Second},
			FailOpen: true,
		},
		StorageIntegrity: defaultStorageIntegrityConfig(),

		// Plumbing sections
		Logging: LoggingConfig{
			Queries:       true,
			Data:          false,
			MaxQueryBytes: 300,
			MaxDataBytes:  200,
		},
		NetworkState: NetworkStateConfig{
			// HOUSEGATE_NETWORK_STATE_REDIS is the legacy spelling
			// (Redis address only); auto-detect treats anything
			// non-yaml-suffixed as a Redis address so it still works.
			// HOUSEGATE_NETWORK_STATE_SOURCE is the modern spelling
			// and additionally accepts a YAML path.
			Source: EnvOrDefault("HOUSEGATE_NETWORK_STATE_SOURCE", EnvOrDefault("HOUSEGATE_NETWORK_STATE_REDIS", "")),
		},
		ReplicationProxy: defaultReplicationProxyConfig(),
		Observability: ObservabilityConfig{
			Collector: CollectorConfig{
				Enabled:     EnvOrDefault("HOUSEGATE_COLLECTOR_ENABLED", "true") != "false",
				Interval:    Duration{15 * time.Second},
				PollTimeout: Duration{5 * time.Second},
				CHAddr:      EnvOrDefault("HOUSEGATE_COLLECTOR_CH_ADDR", ""),
			},
			Pprof: PprofConfig{
				Enabled: EnvOrDefault("HOUSEGATE_PPROF_ENABLED", "") == "true",
				Token:   EnvOrDefault("HOUSEGATE_PPROF_TOKEN", ""),
			},
		},
	}
}

func Load(path string) Config {
	cfg := Default()
	if path == "" {
		if _, err := os.Stat("config.json"); err == nil {
			path = "config.json"
		} else {
			log.Info("no config file provided, using defaults and env overrides")
			return cfg
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Infow("config file not found, using defaults", "path", path)
			return cfg
		}
		log.Fatalw("read config file", "path", path, "err", err)
	}
	if err := unmarshalByExt(path, raw, &cfg); err != nil {
		log.Fatalw("parse config file", "path", path, "err", err)
	}
	log.Infow("config loaded",
		"path", path,
		"listen", cfg.Listen,
		"upstream", cfg.Upstream)
	return cfg
}

// unmarshalByExt decodes raw into cfg using a codec selected from the
// file extension: .yaml / .yml use YAML, everything else (including
// .json and no extension) uses JSON.
func unmarshalByExt(path string, raw []byte, cfg *Config) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return yaml.Unmarshal(raw, cfg)
	default:
		return json.Unmarshal(raw, cfg)
	}
}

// envUint64 returns the env var key parsed as a base-10 unsigned
// integer, or 0 when unset or unparseable. Used for fields that admit
// the zero value as "not configured" (see e.g. IndexerID).
func envUint64(key string) uint64 {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		log.Warnw("invalid uint64 env var, ignoring", "key", key, "value", v, "err", err)
		return 0
	}
	return n
}

// EnvOrDefault returns the value of env var key, or fallback if unset
// or empty.
func EnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
