package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func sampleSnapshot() *Snapshot {
	return &Snapshot{
		CapturedAt: time.Unix(1000, 0),
		CH: []CHReplicaMetrics{
			{Replica: "ch-a:9000", Reachable: true, QueryTotal: 100, InsertTotal: 20, MemoryTrackingBytes: 2048, PartsActive: 7, ReplicationQueue: 3, MutationsPending: 12, MutationsRunning: 4, TablesTotal: 256, OSCPUSeconds: 12.5},
			{Replica: "ch-b:9000", Reachable: false},
		},
		Host: HostMetrics{
			CPUPercent: 42.5, MemAvailableBytes: 8 << 30, MemTotalBytes: 16 << 30,
			DiskReadBytes: map[string]uint64{"disk0": 1000}, DiskWriteBytes: map[string]uint64{"disk0": 500},
			NetRxBytes: 9000, NetTxBytes: 4000,
		},
		Runtime:     RuntimeMetrics{Goroutines: 25, HeapAllocBytes: 1 << 20, GCPauseSeconds: 0.5},
		LastSuccess: map[string]time.Time{"host": time.Unix(1000, 0), "runtime": time.Unix(1000, 0), "ch:ch-a:9000": time.Unix(1000, 0)},
	}
}

func TestExporterNilSnapshotEmitsOnlyUpZero(t *testing.T) {
	exp := NewExporter(&Store{}, "idx-1")
	const expected = `
# HELP clickhouse_proxy_collector_up 1 if the collector has published at least one snapshot, else 0.
# TYPE clickhouse_proxy_collector_up gauge
clickhouse_proxy_collector_up{indexer_id="idx-1"} 0
`
	if err := testutil.CollectAndCompare(exp, strings.NewReader(expected), "clickhouse_proxy_collector_up"); err != nil {
		t.Error(err)
	}
	if n := testutil.CollectAndCount(exp); n != 1 {
		t.Errorf("nil-snapshot series count = %d, want 1 (only collector_up)", n)
	}
}

func TestExporterChUpReachableAndUnreachable(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	exp := NewExporter(store, "idx-1")
	const expected = `
# HELP clickhouse_proxy_ch_up 1 if the ClickHouse replica answered the last scrape, else 0.
# TYPE clickhouse_proxy_ch_up gauge
clickhouse_proxy_ch_up{indexer_id="idx-1",replica="ch-a:9000"} 1
clickhouse_proxy_ch_up{indexer_id="idx-1",replica="ch-b:9000"} 0
`
	if err := testutil.CollectAndCompare(exp, strings.NewReader(expected), "clickhouse_proxy_ch_up"); err != nil {
		t.Error(err)
	}
}

func TestExporterUnreachableReplicaEmitsOnlyUp(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	exp := NewExporter(store, "idx-1")
	// ch-b is unreachable: only ch_up is emitted for it, no value series.
	const expected = `
# HELP clickhouse_proxy_ch_query_total Cumulative queries run by the ClickHouse replica.
# TYPE clickhouse_proxy_ch_query_total counter
clickhouse_proxy_ch_query_total{indexer_id="idx-1",replica="ch-a:9000"} 100
`
	if err := testutil.CollectAndCompare(exp, strings.NewReader(expected), "clickhouse_proxy_ch_query_total"); err != nil {
		t.Error(err)
	}
}

func TestExporterScalarValues(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	exp := NewExporter(store, "idx-1")
	const expected = `
# HELP clickhouse_proxy_host_cpu_percent Host CPU utilization percent (0..100, summed across cores).
# TYPE clickhouse_proxy_host_cpu_percent gauge
clickhouse_proxy_host_cpu_percent{indexer_id="idx-1"} 42.5
# HELP clickhouse_proxy_runtime_goroutines Current goroutine count in the housegate process.
# TYPE clickhouse_proxy_runtime_goroutines gauge
clickhouse_proxy_runtime_goroutines{indexer_id="idx-1"} 25
`
	if err := testutil.CollectAndCompare(exp, strings.NewReader(expected),
		"clickhouse_proxy_host_cpu_percent", "clickhouse_proxy_runtime_goroutines"); err != nil {
		t.Error(err)
	}
}

func TestExporterMutationsAndTables(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	exp := NewExporter(store, "idx-1")
	const expected = `
# HELP clickhouse_proxy_ch_mutations_pending Unfinished mutations (system.mutations is_done = 0) across all tables on the replica.
# TYPE clickhouse_proxy_ch_mutations_pending gauge
clickhouse_proxy_ch_mutations_pending{indexer_id="idx-1",replica="ch-a:9000"} 12
# HELP clickhouse_proxy_ch_mutations_running Mutations currently in progress on the replica (from the PartMutation system.metrics gauge).
# TYPE clickhouse_proxy_ch_mutations_running gauge
clickhouse_proxy_ch_mutations_running{indexer_id="idx-1",replica="ch-a:9000"} 4
# HELP clickhouse_proxy_ch_tables Total number of tables across all databases on the replica (from system.asynchronous_metrics NumberOfTables).
# TYPE clickhouse_proxy_ch_tables gauge
clickhouse_proxy_ch_tables{indexer_id="idx-1",replica="ch-a:9000"} 256
`
	if err := testutil.CollectAndCompare(exp, strings.NewReader(expected),
		"clickhouse_proxy_ch_mutations_pending", "clickhouse_proxy_ch_mutations_running", "clickhouse_proxy_ch_tables"); err != nil {
		t.Error(err)
	}
}

func TestExporterNoCHReplicasEmitsNoCHSeries(t *testing.T) {
	store := &Store{}
	snap := sampleSnapshot()
	snap.CH = nil
	store.Store(snap)
	exp := NewExporter(store, "idx-1")
	if n := testutil.CollectAndCount(exp, "clickhouse_proxy_ch_up"); n != 0 {
		t.Errorf("ch_up series with no replicas = %d, want 0 (series absent, not 0-valued)", n)
	}
	if n := testutil.CollectAndCount(exp, "clickhouse_proxy_host_cpu_percent"); n != 1 {
		t.Errorf("host_cpu_percent count = %d, want 1", n)
	}
}

func TestNewRegistryNoDoubleRegisterPanic(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	// Two independent registries must each register cleanly — proves the
	// Exporter does not touch the global default registry (the documented
	// double-registration hazard).
	r1 := NewRegistry(store, "idx-1")
	r2 := NewRegistry(store, "idx-2")
	if n, err := testutil.GatherAndCount(r1); err != nil || n == 0 {
		t.Errorf("r1 gather: n=%d err=%v", n, err)
	}
	if n, err := testutil.GatherAndCount(r2); err != nil || n == 0 {
		t.Errorf("r2 gather: n=%d err=%v", n, err)
	}
}

func TestExporterMetricNamesPrefixedAndPresent(t *testing.T) {
	store := &Store{}
	store.Store(sampleSnapshot())
	exp := NewExporter(store, "idx-1")
	names := []string{
		"clickhouse_proxy_ch_up", "clickhouse_proxy_ch_query_total", "clickhouse_proxy_ch_insert_total",
		"clickhouse_proxy_ch_memory_tracking_bytes", "clickhouse_proxy_ch_parts_active",
		"clickhouse_proxy_ch_replication_queue", "clickhouse_proxy_ch_os_cpu_seconds",
		"clickhouse_proxy_ch_mutations_pending", "clickhouse_proxy_ch_mutations_running",
		"clickhouse_proxy_ch_tables",
		"clickhouse_proxy_host_cpu_percent", "clickhouse_proxy_host_mem_available_bytes",
		"clickhouse_proxy_host_mem_total_bytes", "clickhouse_proxy_host_disk_read_bytes_total",
		"clickhouse_proxy_host_disk_write_bytes_total", "clickhouse_proxy_host_net_rx_bytes_total",
		"clickhouse_proxy_host_net_tx_bytes_total", "clickhouse_proxy_runtime_goroutines",
		"clickhouse_proxy_runtime_heap_alloc_bytes", "clickhouse_proxy_runtime_gc_pause_seconds",
		"clickhouse_proxy_collector_last_success_timestamp_seconds", "clickhouse_proxy_collector_up",
	}
	for _, n := range names {
		if !strings.HasPrefix(n, "clickhouse_proxy_") {
			t.Errorf("%s missing prefix", n)
		}
		if c := testutil.CollectAndCount(exp, n); c == 0 {
			t.Errorf("metric %s emitted 0 series, expected >=1", n)
		}
	}
}
