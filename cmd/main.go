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

	"sentioxyz/sentio-core/common/flags"
	"sentioxyz/sentio-core/common/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"housegate/housegate"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/secretsload"
)

func main() {
	if handled, exit := secretSubcommand(); handled {
		os.Exit(exit)
	}

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

	flags.ParseAndInitLogFlag()

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

	if err := cfg.Validate(); err != nil {
		log.Fatale(err, "config validation failed")
	}
	return cfg
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
