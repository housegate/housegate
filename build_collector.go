package housegate

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/credentials"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/metrics"
)

// buildCollector constructs the downstream-metrics Collector and its dedicated
// Prometheus registry from config, or returns (nil, nil, nil) when the collector
// is disabled. The returned cleanup closes the CH poller connections and must be
// registered on the teardown stack; the Collector itself stops when the run
// context passed to Start is cancelled.
//
// CH poll targets resolve by precedence: observability.collector.ch_addr if set,
// else the configured shard replicas, else the single upstream. CH credentials
// come from the ckh_manager credential provider (the proxy's when built, else
// loaded directly from ckh_manager_config_path) — there are no separate
// collector credential fields. A node with no CH target still gets a Collector
// that publishes host + runtime metrics only.
func buildCollector(cfg config.Config, credProvider credentials.CredentialProvider, selfIndexerID uint64) (*metrics.Collector, *prometheus.Registry, func()) {
	cc := cfg.Observability.Collector
	if !cc.Enabled {
		return nil, nil, nil
	}

	store := &metrics.Store{}
	reg := metrics.NewRegistry(store, strconv.FormatUint(selfIndexerID, 10))

	// Credentials for the local ClickHouse come from the ckh_manager credential
	// provider's default credential (GetDefaultCredential, not the per-peer
	// GetCredentialForIndexer). Prefer the proxy's already-built provider; if it
	// is nil (credential_replace_enabled=false) load ckh_manager directly, since
	// the collector needs local-CH creds regardless of that data-path flag. With
	// no provider at all the collector connects without credentials and fails
	// open (ch_up=0).
	if credProvider == nil && cfg.CkhManagerConfigPath != "" {
		if cp, err := credentials.LoadCkhManagerYAMLProvider(cfg.CkhManagerConfigPath); err == nil {
			credProvider = cp
		} else {
			log.Warnw("metrics collector: failed to load ckh_manager credentials", "path", cfg.CkhManagerConfigPath, "err", err)
		}
	}
	var user, password string
	if credProvider != nil {
		if u, p, err := credProvider.GetDefaultCredential(); err == nil {
			user, password = u, p
		}
	}

	// Targets: explicit override, else shard replicas, else single upstream.
	var addrs []string
	switch {
	case cc.CHAddr != "":
		addrs = []string{cc.CHAddr}
	case cfg.Shard != nil && len(cfg.Shard.Replicas) > 0:
		for _, r := range cfg.Shard.Replicas {
			addrs = append(addrs, r.Addr())
		}
	case cfg.Upstream != "":
		addrs = []string{cfg.Upstream}
	}

	var pollers []*metrics.CHPoller
	for _, addr := range addrs {
		p, err := metrics.NewCHPoller(addr, "default", user, password, cc.PollTimeout.Duration)
		if err != nil {
			log.Warnw("metrics collector: skipping CH poller", "addr", addr, "err", err)
			continue
		}
		pollers = append(pollers, p)
	}
	if len(addrs) == 0 {
		log.Infow("metrics collector: no ClickHouse target configured, host+runtime metrics only", "indexer_id", selfIndexerID)
	}

	collector := metrics.NewCollector(store, pollers, cc.Interval.Duration)
	cleanup := func() {
		for _, p := range pollers {
			_ = p.Close()
		}
	}
	return collector, reg, cleanup
}
