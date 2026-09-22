package proxy

import (
	"fmt"
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
	beforeProbe := testutil.ToFloat64(agentInlineValuesTotal.WithLabelValues("metric_name_probe"))
	want := fmt.Sprintf(`
# HELP clickhouse_proxy_agent_inline_values_total Agent-mode signed inline INSERT ... VALUES outcomes
# TYPE clickhouse_proxy_agent_inline_values_total counter
clickhouse_proxy_agent_inline_values_total{result="closure_refused"} %g
clickhouse_proxy_agent_inline_values_total{result="evaluation_failed"} %g
clickhouse_proxy_agent_inline_values_total{result="metric_name_probe"} %g
clickhouse_proxy_agent_inline_values_total{result="synthesized"} %g
`, before["closure_refused"]+1, before["evaluation_failed"]+1, beforeProbe+1, before["synthesized"]+1)
	agentInlineValuesTotal.WithLabelValues("metric_name_probe").Inc()
	if err := testutil.CollectAndCompare(agentInlineValuesTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
