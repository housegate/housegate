package proxy

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/plugins/sistatement"
)

// The metric globals are init()-registered, so this asserts deltas rather than
// absolute values and never re-registers a collector.
func TestMetricsObserver_InlineValuesCounters(t *testing.T) {
	var obs sistatement.Observer = NewMetricsObserver()
	results := []string{"synthesized", "evaluation_failed", "closure_refused"}
	before := map[string]float64{}
	for _, result := range results {
		before[result] = testutil.ToFloat64(agentInlineValuesTotal.WithLabelValues(result))
	}
	obs.InlineValuesSynthesized()
	obs.InlineValuesEvaluationFailed()
	obs.InlineValuesClosureRefused()
	for _, result := range results {
		if delta := testutil.ToFloat64(agentInlineValuesTotal.WithLabelValues(result)) - before[result]; delta != 1 {
			t.Fatalf("result=%q counter moved by %v, want 1", result, delta)
		}
	}
	const want = `
# HELP clickhouse_proxy_agent_inline_values_total Agent-mode signed inline INSERT ... VALUES outcomes
# TYPE clickhouse_proxy_agent_inline_values_total counter
clickhouse_proxy_agent_inline_values_total{result="closure_refused"} 1
clickhouse_proxy_agent_inline_values_total{result="evaluation_failed"} 1
clickhouse_proxy_agent_inline_values_total{result="metric_name_probe"} 1
clickhouse_proxy_agent_inline_values_total{result="synthesized"} 1
`
	agentInlineValuesTotal.WithLabelValues("metric_name_probe").Inc()
	if err := testutil.CollectAndCompare(agentInlineValuesTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
