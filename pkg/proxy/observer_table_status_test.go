package proxy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/plugins/sistatement"
)

func TestMetricsObserver_TableStatusLookupFailures(t *testing.T) {
	var obs sistatement.StatusObserver = NewMetricsObserver()
	before := testutil.ToFloat64(agentSITableStatusFailuresTotal)
	obs.TableStatusLookupFailed()
	want := fmt.Sprintf(`
# HELP clickhouse_proxy_agent_si_table_status_failures_total Agent-mode storage-integrity table status lookups that failed; the INSERT passed through unsigned
# TYPE clickhouse_proxy_agent_si_table_status_failures_total counter
clickhouse_proxy_agent_si_table_status_failures_total %g
`, before+1)
	if err := testutil.CollectAndCompare(agentSITableStatusFailuresTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsObserver_SIDiscoveryFailures(t *testing.T) {
	var obs sistatement.DiscoveryObserver = NewMetricsObserver()
	before := testutil.ToFloat64(agentSIDiscoveryFailuresTotal.WithLabelValues("precheck"))
	obs.SIDiscoveryFailed("precheck")
	if got := testutil.ToFloat64(agentSIDiscoveryFailuresTotal.WithLabelValues("precheck")); got != before+1 {
		t.Fatalf("clickhouse_proxy_agent_si_discovery_failures_total{step=\"precheck\"} = %g, want %g", got, before+1)
	}
}

func TestMetricsObserver_SIUpstreamSwitches(t *testing.T) {
	var obs sistatement.SwitchObserver = NewMetricsObserver()
	for _, result := range []string{"switched", "refused_state", "refused_database", "refused_revision", "dial_failed"} {
		before := testutil.ToFloat64(agentSIUpstreamSwitchesTotal.WithLabelValues(result))
		obs.SIUpstreamSwitch(result)
		if got := testutil.ToFloat64(agentSIUpstreamSwitchesTotal.WithLabelValues(result)); got != before+1 {
			t.Fatalf("clickhouse_proxy_agent_si_upstream_switches_total{result=%q} = %g, want %g", result, got, before+1)
		}
	}
}

func TestMetricsObserver_SILanes(t *testing.T) {
	var obs sistatement.LaneObserver = NewMetricsObserver()
	for _, reason := range []string{"gap_budget", "lost_state", "new_process"} {
		before := testutil.ToFloat64(agentSILaneRotationsTotal.WithLabelValues(reason))
		obs.LaneRotated(reason)
		if got := testutil.ToFloat64(agentSILaneRotationsTotal.WithLabelValues(reason)); got != before+1 {
			t.Fatalf("clickhouse_proxy_agent_si_lane_rotations_total{reason=%q} = %g, want %g", reason, got, before+1)
		}
	}
	before := testutil.ToFloat64(agentSIInflight)
	obs.SIInflight(1)
	obs.SIInflight(1)
	obs.SIInflight(-1)
	if got := testutil.ToFloat64(agentSIInflight); got != before+1 {
		t.Fatalf("clickhouse_proxy_agent_si_inflight = %g, want %g", got, before+1)
	}
	obs.SIInflight(-1)
}
