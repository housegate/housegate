package housegate

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
)

type fakeCredProvider struct {
	user, pass string
	err        error
}

func (f fakeCredProvider) GetDefaultCredential() (string, string, error) {
	return f.user, f.pass, f.err
}

func (f fakeCredProvider) GetCredentialForIndexer(uint64) (string, string, error) {
	return f.user, f.pass, f.err
}

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

func TestBuildCollectorCredProviderAndShardTargets(t *testing.T) {
	// No CHAddr → fall through to shard replicas; credProvider supplies creds.
	cfg := obsTestConfig(true, "")
	cfg.Shard = &cluster.ShardConfig{Replicas: []cluster.ReplicaConfig{
		{Host: "10.0.0.1", Port: 9000},
		{Host: "10.0.0.2", Port: 9000},
	}}
	c, reg, cleanup := buildCollector(cfg, fakeCredProvider{user: "u", pass: "p"}, 7)
	if c == nil || reg == nil {
		t.Fatal("expected collector + registry from shard replicas")
	}
	defer cleanup()
}

func TestBuildCollectorUpstreamTarget(t *testing.T) {
	// No CHAddr, no shard → fall through to single upstream.
	cfg := obsTestConfig(true, "")
	cfg.Upstream = "ch.local:9000"
	c, _, cleanup := buildCollector(cfg, nil, 7)
	if c == nil {
		t.Fatal("expected collector for upstream target")
	}
	defer cleanup()
}

func TestBuildCollectorCredProviderErrorStillBuilds(t *testing.T) {
	// credProvider's GetDefaultCredential errors → no creds resolved, but the
	// collector is still constructed (it fails open at poll time).
	cfg := obsTestConfig(true, "127.0.0.1:9000")
	c, _, cleanup := buildCollector(cfg, fakeCredProvider{err: errCredUnavailable}, 7)
	if c == nil {
		t.Fatal("expected collector even when credProvider errors")
	}
	defer cleanup()
}

func TestBuildCollectorLoadsCkhManagerWhenNoProvider(t *testing.T) {
	// credProvider nil + a ckh_manager path that fails to load → warn, no creds,
	// collector still built (fail-open). Exercises the decoupled ckh_manager load
	// (the collector resolves local-CH creds from ckh_manager regardless of
	// credential_replace_enabled).
	cfg := obsTestConfig(true, "127.0.0.1:9000")
	cfg.CkhManagerConfigPath = "/nonexistent/ckh_manager.yaml"
	c, _, cleanup := buildCollector(cfg, nil, 7)
	if c == nil {
		t.Fatal("expected collector even when ckh_manager load fails")
	}
	defer cleanup()
}

var errCredUnavailable = errorString("cred unavailable")

type errorString string

func (e errorString) Error() string { return string(e) }
