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
