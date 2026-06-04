package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metricNamespace is the shared prefix for every series this exporter emits,
// matching the existing init()-registered globals in pkg/proxy/observer.go so
// dashboards and scrape annotations key off one consistent namespace.
const metricNamespace = "clickhouse_proxy"

// Exporter is a prometheus.Collector that publishes the latest collected
// Snapshot at scrape time. Each Collect reads the Store (a single lock-free
// atomic load), so the exposition always reflects the most recent collection
// cycle without holding any scrape-time lock and without the exporter owning a
// goroutine. It is registered on a dedicated registry (see NewRegistry) so it
// never touches the global default registry that the init()-registered counter
// globals own — avoiding the documented double-registration panic.
type Exporter struct {
	store *Store

	chUp               *prometheus.Desc
	chQueryTotal       *prometheus.Desc
	chInsertTotal      *prometheus.Desc
	chMemoryTracking   *prometheus.Desc
	chPartsActive      *prometheus.Desc
	chReplicationQueue *prometheus.Desc
	chOSCPUSeconds     *prometheus.Desc

	hostCPUPercent   *prometheus.Desc
	hostMemAvailable *prometheus.Desc
	hostMemTotal     *prometheus.Desc
	hostDiskRead     *prometheus.Desc
	hostDiskWrite    *prometheus.Desc
	hostNetRx        *prometheus.Desc
	hostNetTx        *prometheus.Desc

	runtimeGoroutines *prometheus.Desc
	runtimeHeapAlloc  *prometheus.Desc
	runtimeGCPause    *prometheus.Desc

	lastSuccess *prometheus.Desc
	collectorUp *prometheus.Desc
}

// NewExporter builds an Exporter reading from store, with indexerID baked into
// every series as the constant indexer_id label.
func NewExporter(store *Store, indexerID string) *Exporter {
	cl := prometheus.Labels{"indexer_id": indexerID}
	fqn := func(s string) string { return metricNamespace + "_" + s }
	return &Exporter{
		store: store,

		chUp:               prometheus.NewDesc(fqn("ch_up"), "1 if the ClickHouse replica answered the last scrape, else 0.", []string{"replica"}, cl),
		chQueryTotal:       prometheus.NewDesc(fqn("ch_query_total"), "Cumulative queries run by the ClickHouse replica.", []string{"replica"}, cl),
		chInsertTotal:      prometheus.NewDesc(fqn("ch_insert_total"), "Cumulative insert queries run by the ClickHouse replica.", []string{"replica"}, cl),
		chMemoryTracking:   prometheus.NewDesc(fqn("ch_memory_tracking_bytes"), "Bytes currently tracked by the ClickHouse memory tracker.", []string{"replica"}, cl),
		chPartsActive:      prometheus.NewDesc(fqn("ch_parts_active"), "Number of active data parts on the ClickHouse replica.", []string{"replica"}, cl),
		chReplicationQueue: prometheus.NewDesc(fqn("ch_replication_queue"), "Tasks waiting in the ClickHouse replication queue.", []string{"replica"}, cl),
		chOSCPUSeconds:     prometheus.NewDesc(fqn("ch_os_cpu_seconds"), "Cumulative OS CPU seconds used by the ClickHouse server (lags CH's async-metrics update period).", []string{"replica"}, cl),

		hostCPUPercent:   prometheus.NewDesc(fqn("host_cpu_percent"), "Host CPU utilization percent (0..100, summed across cores).", nil, cl),
		hostMemAvailable: prometheus.NewDesc(fqn("host_mem_available_bytes"), "Host memory available to userspace, in bytes.", nil, cl),
		hostMemTotal:     prometheus.NewDesc(fqn("host_mem_total_bytes"), "Host total physical memory, in bytes.", nil, cl),
		hostDiskRead:     prometheus.NewDesc(fqn("host_disk_read_bytes_total"), "Cumulative bytes read from a host block device.", []string{"device"}, cl),
		hostDiskWrite:    prometheus.NewDesc(fqn("host_disk_write_bytes_total"), "Cumulative bytes written to a host block device.", []string{"device"}, cl),
		hostNetRx:        prometheus.NewDesc(fqn("host_net_rx_bytes_total"), "Cumulative bytes received across host network interfaces.", nil, cl),
		hostNetTx:        prometheus.NewDesc(fqn("host_net_tx_bytes_total"), "Cumulative bytes transmitted across host network interfaces.", nil, cl),

		runtimeGoroutines: prometheus.NewDesc(fqn("runtime_goroutines"), "Current goroutine count in the housegate process.", nil, cl),
		runtimeHeapAlloc:  prometheus.NewDesc(fqn("runtime_heap_alloc_bytes"), "Allocated heap object bytes in the housegate process.", nil, cl),
		runtimeGCPause:    prometheus.NewDesc(fqn("runtime_gc_pause_seconds"), "Cumulative stop-the-world GC pause seconds.", nil, cl),

		lastSuccess: prometheus.NewDesc(fqn("collector_last_success_timestamp_seconds"), "Unix timestamp of the last successful poll, per collection source.", []string{"source"}, cl),
		collectorUp: prometheus.NewDesc(fqn("collector_up"), "1 if the collector has published at least one snapshot, else 0.", nil, cl),
	}
}

func (e *Exporter) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		e.chUp, e.chQueryTotal, e.chInsertTotal, e.chMemoryTracking, e.chPartsActive, e.chReplicationQueue, e.chOSCPUSeconds,
		e.hostCPUPercent, e.hostMemAvailable, e.hostMemTotal, e.hostDiskRead, e.hostDiskWrite, e.hostNetRx, e.hostNetTx,
		e.runtimeGoroutines, e.runtimeHeapAlloc, e.runtimeGCPause,
		e.lastSuccess, e.collectorUp,
	}
}

// Describe sends every metric descriptor to ch (implements prometheus.Collector).
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range e.descs() {
		ch <- d
	}
}

// Collect reads the latest snapshot and emits all series (implements
// prometheus.Collector). Before the first snapshot is published it emits only
// collector_up=0, so a scraper can distinguish "not yet collected" from a real
// all-zero reading. An unreachable replica emits only ch_up=0 (its other
// values are unknown, not zero); a node with no configured replicas emits no
// ch_* series at all.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}

	snap := e.store.Load()
	if snap == nil {
		gauge(e.collectorUp, 0)
		return
	}
	gauge(e.collectorUp, 1)

	for _, r := range snap.CH {
		if !r.Reachable {
			// Unreachable: only the liveness gauge; the value series are
			// unknown, so they are omitted rather than reported as zero.
			gauge(e.chUp, 0, r.Replica)
			continue
		}
		gauge(e.chUp, 1, r.Replica)
		counter(e.chQueryTotal, float64(r.QueryTotal), r.Replica)
		counter(e.chInsertTotal, float64(r.InsertTotal), r.Replica)
		gauge(e.chMemoryTracking, float64(r.MemoryTrackingBytes), r.Replica)
		gauge(e.chPartsActive, float64(r.PartsActive), r.Replica)
		gauge(e.chReplicationQueue, float64(r.ReplicationQueue), r.Replica)
		counter(e.chOSCPUSeconds, r.OSCPUSeconds, r.Replica)
	}

	h := snap.Host
	gauge(e.hostCPUPercent, h.CPUPercent)
	gauge(e.hostMemAvailable, float64(h.MemAvailableBytes))
	gauge(e.hostMemTotal, float64(h.MemTotalBytes))
	for dev, b := range h.DiskReadBytes {
		counter(e.hostDiskRead, float64(b), dev)
	}
	for dev, b := range h.DiskWriteBytes {
		counter(e.hostDiskWrite, float64(b), dev)
	}
	counter(e.hostNetRx, float64(h.NetRxBytes))
	counter(e.hostNetTx, float64(h.NetTxBytes))

	rt := snap.Runtime
	gauge(e.runtimeGoroutines, float64(rt.Goroutines))
	gauge(e.runtimeHeapAlloc, float64(rt.HeapAllocBytes))
	counter(e.runtimeGCPause, rt.GCPauseSeconds)

	for source, ts := range snap.LastSuccess {
		gauge(e.lastSuccess, float64(ts.Unix()), source)
	}
}

// NewRegistry builds a dedicated *prometheus.Registry with an Exporter reading
// from store registered on it. Using a dedicated registry (rather than the
// global default) keeps these scrape-time metrics independent of the
// init()-registered counter globals and avoids the documented
// double-registration panic. The HTTP layer gathers this registry merged with
// the default one via prometheus.Gatherers.
func NewRegistry(store *Store, indexerID string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewExporter(store, indexerID))
	return reg
}
