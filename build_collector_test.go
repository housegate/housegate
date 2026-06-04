package housegate

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"housegate/housegate/pkg/config"
)

func obsTestConfig(enabled bool, chAddr string) config.Config {
	cfg := config.Default()
	cfg.Observability.Collector.Enabled = enabled
	cfg.Observability.Collector.Interval = config.Duration{15 * time.Second}
	cfg.Observability.Collector.PollTimeout = config.Duration{2 * time.Second}
	cfg.Observability.Collector.CHAddr = chAddr
	return cfg
}

func TestBuildCollectorDisabled(t *testing.T) {
	c, reg, cleanup := buildCollector(obsTestConfig(false, ""), nil, 7)
	if c != nil || reg != nil {
		t.Errorf("disabled collector should yield nil collector + registry; got %v %v", c, reg)
	}
	if cleanup != nil {
		cleanup()
	}
}

func TestBuildCollectorEnabledExposesRegistry(t *testing.T) {
	c, reg, cleanup := buildCollector(obsTestConfig(true, "127.0.0.1:9000"), nil, 7)
	if c == nil || reg == nil {
		t.Fatal("enabled collector should yield non-nil collector + registry")
	}
	defer cleanup()
	// Before any poll the exporter still gathers collector_up (=0) from the
	// dedicated registry — proves NewRegistry was wired to the same store the
	// collector publishes to.
	if n, err := testutil.GatherAndCount(reg, "clickhouse_proxy_collector_up"); err != nil || n != 1 {
		t.Errorf("registry collector_up count = %d err=%v, want 1", n, err)
	}
}

func TestBuildCollectorNoTargetsStillRuns(t *testing.T) {
	// Enabled but no CH addr / shard / upstream → host+runtime only, no CH
	// pollers, but still a valid collector + registry.
	c, reg, cleanup := buildCollector(obsTestConfig(true, ""), nil, 7)
	if c == nil || reg == nil {
		t.Fatal("collector with no CH targets should still run (host+runtime)")
	}
	defer cleanup()
}

func TestProxyImplMetricsRegistryAccessor(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := &proxyImpl{built: &builtServer{metricsRegistry: reg}}
	if p.MetricsRegistry() != reg {
		t.Error("MetricsRegistry() did not return the built registry")
	}
	// nil built → nil registry, no panic.
	p2 := &proxyImpl{}
	if p2.MetricsRegistry() != nil {
		t.Error("MetricsRegistry() with nil built should return nil")
	}
}
