package integration

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/integration/testenv"
)

// metricCounterValue returns the current value of the named counter
// registered against the global prometheus registry, summed across all
// label values for label-vec metrics. Returns 0 if the metric is not
// registered (which itself is a regression for the housegate package
// — the proxy/observer.go init() should have registered it).
func metricCounterValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("prometheus.Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return sum
	}
	return 0
}

// TestMetrics_QueriesForwardedCounter pins the metrics plugin's
// QueryForwarded → clickhouse_proxy_queries_forwarded_total path: a
// successful signed query through the full chain must increment the
// counter. The metric is registered as a global by pkg/proxy/observer.go's
// init(), so we read it back from prometheus.DefaultGatherer.
//
// We compute a *delta* across the query because the metric is a process-
// wide counter shared with any other test that ran before us in the
// same Bazel test binary — the absolute value is unstable, the delta
// is not.
//
// Coverage: ensures the metrics plugin is actually wired into the
// chain. Without it, no QueryForwarded fires and the delta stays 0.
func TestMetrics_QueriesForwardedCounter(t *testing.T) {
	const metric = "clickhouse_proxy_queries_forwarded_total"

	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
	)
	conn := openSignedConn(t, proxy.Addr, signer)

	before := metricCounterValue(t, metric)

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("signed SELECT 1: %v", err)
	}

	after := metricCounterValue(t, metric)
	delta := after - before
	if delta < 1 {
		t.Errorf("%s delta = %v, want ≥ 1 (before=%v after=%v)", metric, delta, before, after)
	}
}

