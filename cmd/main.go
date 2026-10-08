// Package main is the housegate ClickHouse-proxy standalone binary.
//
// Library callers should import "github.com/housegate/housegate" instead and
// call housegate.New(opts).Run(ctx) — see proxy.go.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/metricshttp"
	"github.com/housegate/housegate/pkg/secretsload"
	"github.com/housegate/housegate/pkg/version"
)

func main() {
	if handled, exit := secretSubcommand(); handled {
		os.Exit(exit)
	}
	if handled, exit := fetchSubcommand(); handled {
		os.Exit(exit)
	}

	// Install the console handler before anything logs so our slog-based
	// pkg/log records share the zap-development format with any
	// transitive-dep logs that still flow through. Level is driven by
	// pkg/log's LevelVar so log.SetLevel still applies.
	// loadConfigWithOverrides may swap the writer to a file later.
	log.SetDefault(log.New(newConsoleHandler(os.Stderr, log.DefaultLevelVar(), true)))

	cfg := loadConfigWithOverrides()
	if err := validateStandaloneRuntimeConfig(&cfg); err != nil {
		log.Fatale(err, "standalone config validation failed")
	}
	logStartupBanner(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p, err := housegate.New(housegate.Options{Config: &cfg})
	if err != nil {
		log.Fatale(err, "init housegate")
	}
	// Construct the proxy first so the metrics server can gather its dedicated
	// collector registry alongside the default registry's init() globals.
	metricshttp.Start(ctx, cfg.MetricsListen, p.MetricsRegistry(), cfg.Observability.Pprof)
	if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatale(err, "housegate stopped")
	}
}

// loadConfigWithOverrides parses CLI flags and applies them on top of
// the file/env config. Override precedence: CLI flag > env var >
// config file > built-in default.
func loadConfigWithOverrides() config.Config {
	showVersion := flag.Bool("version", false, "print version information and exit")
	configPath := flag.String("config", config.EnvOrDefault("HOUSEGATE_CONFIG", ""), "path to JSON config file (optional)")

	agentMode := flag.Bool("agent", false, "enable agent mode (token-signing pass-through proxy)")
	agentUpstream := flag.String("agent-upstream", "", "server-side proxy address, e.g. 10.0.0.8:9001 (required in agent mode)")
	agentKey := flag.String("agent-key", "", "agent Ethereum private key hex for JWS signing; implies agent mode without a config file unless -agent is given (prefer env var HOUSEGATE_AGENT_KEY)")
	agentOwner := flag.String("agent-owner", "", "billed Ethereum address (owner) when -agent-key is an operator key (overrides config/env HOUSEGATE_AGENT_OWNER)")
	agentDriver := flag.Bool("agent-driver", false, "mark outgoing queries as indexer-driver traffic (injects SQL_sentio_driver=1; upstream still gates on signer == indexer)")
	var quick agentQuickstartFlags
	flag.StringVar(&quick.network, "network", "", `agent network preset (default "devnet2" without a config file; also HOUSEGATE_NETWORK)`)
	flag.StringVar(&quick.si, "si", "", "storage-integrity signing: auto|on|off (default auto without a config file; also HOUSEGATE_SI)")
	flag.StringVar(&quick.siStateDir, "si-state-dir", "", "storage-integrity agent state directory (default per OS; also HOUSEGATE_SI_STATE_DIR)")
	flag.StringVar(&quick.siLanes, "si-lanes", "", "client_seq lanes: auto|off (also HOUSEGATE_SI_LANES)")
	flag.StringVar(&quick.siReadMode, "si-read-mode", "", "inject SQL_x_read_mode on SELECTs: safe|unsafe_latest (also HOUSEGATE_SI_READ_MODE)")
	flag.StringVar(&quick.siInlineValues, "si-inline-values", "", "signed inline INSERT ... VALUES: auto|on|off (default auto without a config file; also HOUSEGATE_SI_INLINE_VALUES)")

	stateSource := flag.String("state", "", "NetworkState source: yaml path, redis addr, or RPC URL e.g. http://node:10003 (overrides config/env HOUSEGATE_NETWORK_STATE_SOURCE)")
	listenAddr := flag.String("listen", "", `proxy listen address, e.g. :9001 (overrides config/env; agent default "127.0.0.1:9000" without a config file)`)
	metricsAddr := flag.String("metrics-listen", "", "Prometheus metrics listen address, e.g. :9091 (overrides config/env)")
	dialTimeout := flag.String("dial-timeout", "", "upstream dial timeout, e.g. 5s (overrides config/env)")
	idleTimeout := flag.String("idle-timeout", "", "connection idle timeout, e.g. 5m (overrides config/env)")
	logQueries := flag.Bool("log-queries", true, "log SQL query content")
	logLevel := flag.String("log-level", "", `package-default log level: "debug" / "info" / "warn" / "error" / "fatal" (overrides config/env HOUSEGATE_LOG_LEVEL)`)
	// -log-file may be registered transitively by another logging package
	// when something in the dependency graph still imports it. We pick up
	// its value via flag.Lookup inside maybeSwapLogFile (nil-safe — falls
	// through to HOUSEGATE_LOG_FILE / cfg.LogFile when the flag is absent).

	flag.Parse()

	if *showVersion {
		fmt.Println(version.Info())
		os.Exit(0)
	}

	// Stage-1 log-destination resolve: take effect immediately so any
	// log emitted by config.Load (e.g. "no config file provided") already
	// lands in the right place. Stage-2 below covers yaml-only configs.
	if swapped := maybeSwapLogFile(""); swapped {
		// already pointed at file via flag/env
	}

	explicitFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })

	cfgPath := *configPath
	cfgCleanup := func() {}
	if cfgPath != "" {
		resolved, err := secretsload.Resolve(cfgPath)
		if err != nil {
			log.Fatale(err, "resolve config file")
		}
		cfgPath = resolved.Path
		cfgCleanup = resolved.Cleanup
	}
	cfg, cfgLoaded, err := config.LoadFile(cfgPath)
	cfgCleanup()
	if err != nil {
		log.Fatale(err, "load config file")
	}

	if explicitFlags["agent"] {
		cfg.Agent.Mode = *agentMode
	}
	if explicitFlags["agent-upstream"] {
		cfg.Agent.Upstream = *agentUpstream
	}
	if explicitFlags["agent-key"] {
		cfg.Agent.PrivateKeyHex = *agentKey
	}
	if explicitFlags["agent-owner"] {
		cfg.Agent.Owner = *agentOwner
	}
	if explicitFlags["agent-driver"] {
		cfg.Agent.Driver = *agentDriver
	}
	if explicitFlags["state"] {
		cfg.NetworkState.Source = *stateSource
	}
	if explicitFlags["listen"] {
		cfg.Listen = *listenAddr
	}
	if explicitFlags["metrics-listen"] {
		cfg.MetricsListen = *metricsAddr
	}
	if explicitFlags["dial-timeout"] {
		var d config.Duration
		if err := d.UnmarshalText([]byte(*dialTimeout)); err != nil {
			log.Fatale(err, "invalid -dial-timeout")
		}
		cfg.DialTimeout = d
	}
	if explicitFlags["idle-timeout"] {
		var d config.Duration
		if err := d.UnmarshalText([]byte(*idleTimeout)); err != nil {
			log.Fatale(err, "invalid -idle-timeout")
		}
		cfg.IdleTimeout = d
	}
	if explicitFlags["log-queries"] {
		cfg.Logging.Queries = *logQueries
	}
	if explicitFlags["log-level"] {
		cfg.LogLevel = *logLevel
	} else if env := config.EnvOrDefault("HOUSEGATE_LOG_LEVEL", ""); env != "" && cfg.LogLevel == "" {
		cfg.LogLevel = env
	}
	// After every override: the quickstart reads the effective key,
	// upstream, network source and listen address.
	if err := config.ApplyAgentQuickstart(&cfg, agentQuickstartInputs(cfgLoaded, explicitFlags, quick, os.Getenv)); err != nil {
		log.Fatale(err, "agent options")
	}

	if err := cfg.Validate(); err != nil {
		log.Fatale(err, "config validation failed")
	}

	// Apply the resolved level before anything else logs. Validate already
	// confirmed parseability; ignore the error.
	if lv, err := log.ParseLevel(cfg.LogLevel); err == nil {
		log.SetLevel(lv)
	}

	// Stage-2 log-destination resolve: covers the yaml-only case (no
	// flag, no env). If stage-1 already swapped, maybeSwapLogFile no-ops.
	maybeSwapLogFile(cfg.LogFile)
	return cfg
}

// agentQuickstartFlags holds the agent-UX value flags of spec 2026-10-09 §6.4.
type agentQuickstartFlags struct {
	network, si, siStateDir, siLanes, siReadMode, siInlineValues string
}

// agentQuickstartInputs resolves each agent-UX value as an explicitly passed
// flag, else its env var (empty means not given), and records whether a
// config file was read and whether the operator chose the mode or the listen
// address, which the quickstart defaults must not override (plan decision
// P5). An empty env var counts as unset, matching config.Default.
func agentQuickstartInputs(configLoaded bool, explicit map[string]bool, f agentQuickstartFlags, getenv func(string) string) config.AgentQuickstart {
	flagOrEnv := func(name, value, env string) string {
		if explicit[name] {
			return value
		}
		return getenv(env)
	}
	return config.AgentQuickstart{
		ConfigFileLoaded: configLoaded,
		AgentModeSet:     explicit["agent"] || getenv("HOUSEGATE_AGENT") != "",
		ListenSet:        explicit["listen"] || getenv("HOUSEGATE_LISTEN") != "",
		Network:          flagOrEnv("network", f.network, "HOUSEGATE_NETWORK"),
		SI:               flagOrEnv("si", f.si, "HOUSEGATE_SI"),
		SIStateDir:       flagOrEnv("si-state-dir", f.siStateDir, "HOUSEGATE_SI_STATE_DIR"),
		SILanes:          flagOrEnv("si-lanes", f.siLanes, "HOUSEGATE_SI_LANES"),
		SIReadMode:       flagOrEnv("si-read-mode", f.siReadMode, "HOUSEGATE_SI_READ_MODE"),
		SIInlineValues:   flagOrEnv("si-inline-values", f.siInlineValues, "HOUSEGATE_SI_INLINE_VALUES"),
	}
}

func validateStandaloneRuntimeConfig(cfg *config.Config) error {
	if cfg.StorageIntegrity.Ingress.Enabled {
		return fmt.Errorf("storage_integrity.ingress.enabled is library-host-only for standalone cmd/housegate: embedders must provide housegate.Options.StorageIntegrityAdmissionConsumer")
	}
	return nil
}

// logFileSwapped is set once maybeSwapLogFile has redirected pkg/log to a
// file, so a later call with a yaml-only path doesn't override an explicit
// flag/env destination.
var logFileSwapped bool

// maybeSwapLogFile redirects pkg/log to a file when one is configured.
// Precedence inside this call: -log-file flag > HOUSEGATE_LOG_FILE env >
// the provided fallback (cfg.LogFile from yaml/json).
//
// Returns true if a swap was performed (this call or any earlier one).
func maybeSwapLogFile(fallback string) bool {
	if logFileSwapped {
		return true
	}
	logFile := fallback
	if lf := flag.Lookup("log-file"); lf != nil {
		if v := lf.Value.String(); v != "" {
			logFile = v
		}
	}
	if logFile == "" {
		logFile = config.EnvOrDefault("HOUSEGATE_LOG_FILE", "")
	}
	if logFile == "" {
		return false
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalfe(err, "open log file %q", logFile)
	}
	// color = false: ANSI escapes don't belong in a file.
	log.SetDefault(log.New(newConsoleHandler(f, log.DefaultLevelVar(), false)))
	logFileSwapped = true
	return true
}

func logStartupBanner(cfg *config.Config) {
	log.Infow("housegate starting",
		"mode", cfg.Mode(), "listen", cfg.Listen, "upstream", cfg.Upstream,
		"dial_timeout", cfg.DialTimeout, "idle_timeout", cfg.IdleTimeout,
		"stats_interval", cfg.StatsInterval,
		"log_queries", cfg.Logging.Queries, "log_data", cfg.Logging.Data,
		"auth_enabled", cfg.Auth.Enabled,
	)
	if cfg.Mode() == config.ModeServer && cfg.Shard == nil && cfg.Upstream == "" {
		log.Info("router-only server: no upstream/shard configured, requests will be forwarded to bound proxies via NetworkState")
	}
	if cfg.Shard != nil && cfg.Upstream != "" {
		log.Warn("both 'shard' and 'upstream' configured; 'shard' takes priority, 'upstream' will be ignored for routing")
	}
}
