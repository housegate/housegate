package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
)

// scrapeMerged renders the proxy's dedicated collector registry merged with the
// default registry (the init() globals), exactly as the binary's /metrics
// handler does, and returns the text exposition.
func scrapeMerged(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	h := promhttp.HandlerFor(prometheus.Gatherers{prometheus.DefaultGatherer, reg}, promhttp.HandlerOpts{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d", rec.Code)
	}
	return rec.Body.String()
}

// metricValue returns the value of the first exposition line for the given
// metric name (matching "name{" or "name ").
func metricValue(body, name string) (string, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				return f[len(f)-1], true
			}
		}
	}
	return "", false
}

// TestMetricsCollectorEndToEnd is the Wiring-CP check: a server built via the
// real housegate.New/buildServer path starts the Collector in preServe, which
// polls the live testcontainers ClickHouse; the dedicated registry exposed by
// Proxy.MetricsRegistry() then carries the ch/host/runtime families, merged with
// the default registry's init globals.
func TestMetricsCollectorEndToEnd(t *testing.T) {
	tp := testenv.StartServerProxy(t, chEnv.Addr, testenv.WithConfigMutator(func(cfg *config.Config) {
		cfg.Observability.Collector.Enabled = true
		cfg.Observability.Collector.Interval = config.Duration{Duration: 500 * time.Millisecond}
		cfg.Observability.Collector.PollTimeout = config.Duration{Duration: 3 * time.Second}
		cfg.Observability.Collector.CHAddr = chEnv.Addr
		// Credentials come from the testenv ckh_manager credential provider
		// (GetDefaultCredential) — the collector has no separate cred fields.
	}))

	reg := tp.Proxy.MetricsRegistry()
	if reg == nil {
		t.Fatal("Proxy.MetricsRegistry() is nil — collector not wired by buildServer")
	}

	// Wait for the first successful CH poll (ch_up == 1).
	var body, up string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		body = scrapeMerged(t, reg)
		if v, ok := metricValue(body, "clickhouse_proxy_ch_up"); ok && v == "1" {
			up = v
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if up != "1" {
		t.Fatalf("clickhouse_proxy_ch_up never reached 1 (collector did not poll the live CH). body:\n%s", body)
	}

	// CH value family + host + runtime families all present.
	for _, name := range []string{
		"clickhouse_proxy_ch_memory_tracking_bytes",
		"clickhouse_proxy_ch_mutations_pending",
		"clickhouse_proxy_ch_mutations_running",
		"clickhouse_proxy_ch_tables",
		"clickhouse_proxy_host_cpu_percent",
		"clickhouse_proxy_runtime_goroutines",
		"clickhouse_proxy_collector_up",
	} {
		if _, ok := metricValue(body, name); !ok {
			t.Errorf("merged /metrics missing family %s", name)
		}
	}
	// indexer_id label is present on the collector series.
	if !strings.Contains(body, "indexer_id=") {
		t.Error("indexer_id label missing from collector series")
	}
	// Merge proof: a default-registry init global appears in the same scrape.
	if !strings.Contains(body, "clickhouse_proxy_active_connections") {
		t.Error("default-registry init global missing from merged scrape")
	}
}

// TestMetricsCollectorFailOpen points the collector at a dead ClickHouse address
// and asserts the fail-open contract end to end: the server stays up, ch_up is 0
// (configured but unreachable), and host + runtime metrics still publish.
func TestMetricsCollectorFailOpen(t *testing.T) {
	tp := testenv.StartServerProxy(t, chEnv.Addr, testenv.WithConfigMutator(func(cfg *config.Config) {
		cfg.Observability.Collector.Enabled = true
		cfg.Observability.Collector.Interval = config.Duration{Duration: 500 * time.Millisecond}
		cfg.Observability.Collector.PollTimeout = config.Duration{Duration: 1 * time.Second}
		cfg.Observability.Collector.CHAddr = "127.0.0.1:1" // reserved port, nothing listens
	}))

	reg := tp.Proxy.MetricsRegistry()
	if reg == nil {
		t.Fatal("Proxy.MetricsRegistry() is nil")
	}

	// Wait until a snapshot has been published (runtime always publishes).
	var body string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		body = scrapeMerged(t, reg)
		if _, ok := metricValue(body, "clickhouse_proxy_runtime_goroutines"); ok {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// ch_up == 0 for the configured-but-unreachable replica.
	if v, ok := metricValue(body, "clickhouse_proxy_ch_up"); !ok || v != "0" {
		t.Errorf("clickhouse_proxy_ch_up = %q (present=%v), want 0 for a dead CH. body:\n%s", v, ok, body)
	}
	// Host + runtime metrics still present under CH fail-open.
	if _, ok := metricValue(body, "clickhouse_proxy_host_cpu_percent"); !ok {
		t.Error("host metrics missing under CH fail-open")
	}
	if _, ok := metricValue(body, "clickhouse_proxy_runtime_goroutines"); !ok {
		t.Error("runtime metrics missing under CH fail-open")
	}
}
