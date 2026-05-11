// Package main is the housegate ClickHouse-proxy standalone binary.
//
// Library callers should import "housegate/housegate" instead and
// call housegate.New(opts).Run(ctx) — see proxy.go.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"housegate/housegate"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/secretsload"
)

func main() {
	if handled, exit := secretSubcommand(); handled {
		os.Exit(exit)
	}

	// Install the console handler before anything logs so our slog-based
	// pkg/log records share the zap-development format with sentio-core's
	// own transitive-dep logs (envconf, clickhousemanager, ...). Level is
	// driven by pkg/log's LevelVar so log.SetLevel still applies.
	// loadConfigWithOverrides may swap the writer to a file later.
	log.SetDefault(log.New(newConsoleHandler(os.Stderr, log.DefaultLevelVar(), true)))

	cfg := loadConfigWithOverrides()
	logStartupBanner(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startMetricsServer(cfg.MetricsListen)

	p, err := housegate.New(housegate.Options{Config: &cfg})
	if err != nil {
		log.Fatale(err, "init housegate")
	}
	if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatale(err, "housegate stopped")
	}
}

// loadConfigWithOverrides parses CLI flags and applies them on top of
// the file/env config. Override precedence: CLI flag > env var >
// config file > built-in default.
func loadConfigWithOverrides() config.Config {
	configPath := flag.String("config", config.EnvOrDefault("HOUSEGATE_CONFIG", ""), "path to JSON config file (optional)")

	sidecarMode := flag.Bool("sidecar", false, "enable sidecar mode (token-signing pass-through proxy)")
	sidecarUpstream := flag.String("sidecar-upstream", "", "server-side proxy address, e.g. 10.0.0.8:9001 (required in sidecar mode)")
	sidecarKey := flag.String("sidecar-key", "", "sidecar Ethereum private key hex for JWS signing (prefer env var HOUSEGATE_SIDECAR_KEY)")
	sidecarOwner := flag.String("sidecar-owner", "", "billed Ethereum address (owner) when -sidecar-key is an operator key")

	stateSource := flag.String("state", "", "NetworkState source: yaml path, redis addr, or RPC URL e.g. http://node:10003 (overrides config/env HOUSEGATE_NETWORK_STATE_SOURCE)")
	listenAddr := flag.String("listen", "", "proxy listen address, e.g. :9001 (overrides config/env)")
	metricsAddr := flag.String("metrics-listen", "", "Prometheus metrics listen address, e.g. :9091 (overrides config/env)")
	dialTimeout := flag.String("dial-timeout", "", "upstream dial timeout, e.g. 5s (overrides config/env)")
	idleTimeout := flag.String("idle-timeout", "", "connection idle timeout, e.g. 5m (overrides config/env)")
	logQueries := flag.Bool("log-queries", true, "log SQL query content")
	logLevel := flag.String("log-level", "", `package-default log level: "debug" / "info" / "warn" / "error" / "fatal" (overrides config/env HOUSEGATE_LOG_LEVEL)`)
	// -log-file is registered transitively by sentio-core/common/log (still
	// imported via clickhousemanager in cmd/sentio_adapter.go). We pick up
	// its value via flag.Lookup inside maybeSwapLogFile, so a single flag
	// controls both sentio-core's writer and ours.

	flag.Parse()

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
	cfg := config.Load(cfgPath)
	cfgCleanup()

	if explicitFlags["sidecar"] {
		cfg.Sidecar.Mode = *sidecarMode
	}
	if explicitFlags["sidecar-upstream"] {
		cfg.Sidecar.Upstream = *sidecarUpstream
	}
	if explicitFlags["sidecar-key"] {
		cfg.Sidecar.PrivateKeyHex = *sidecarKey
	}
	if explicitFlags["sidecar-owner"] {
		cfg.Sidecar.Owner = *sidecarOwner
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

func startMetricsServer(addr string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorw("metrics server panic recovered", "panic", r)
			}
		}()
		log.Infow("metrics listening", "addr", addr)
		if err := http.ListenAndServe(addr, promhttp.Handler()); err != nil {
			log.Infoe(err, "metrics server error")
		}
	}()
}
