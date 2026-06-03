// Package metrics defines the in-process metrics snapshot that housegate
// publishes once per collection cycle and the lock-free store every consumer
// reads from.
//
// The snapshot is the single seam between the background collector (which
// scrapes ClickHouse, the host, and the Go runtime) and its consumers. Today
// the only consumer is the Prometheus exporter; the router and the request
// signer attach to the same Store later. Consumers read through
// Store.Load, which is a single atomic pointer load — no locks on the read
// path, so a slow scrape never blocks a scrape-time export.
//
// This file owns the data-model types. Every field is a plain holder so that
// downstream code only ever populates or reads these structs; it does not
// reshape them. A Snapshot is treated as immutable once published: the
// collector builds a fresh *Snapshot each cycle and swaps it in with
// Store.Store, rather than mutating a live one. That immutability is what makes
// the lock-free read safe — a reader either sees the whole old snapshot or the
// whole new one, never a half-written mix.
package metrics

import (
	"sync/atomic"
	"time"
)

// Snapshot is one complete capture of all metrics at a single instant. It is
// built fresh each collection cycle and published atomically; readers treat it
// as read-only.
type Snapshot struct {
	// CapturedAt is when this snapshot's collection cycle started.
	CapturedAt time.Time

	// CH holds one entry per ClickHouse replica that was scraped this cycle.
	CH []CHReplicaMetrics

	// Host holds machine-level metrics (CPU, memory, disk, network) for the
	// host running this housegate.
	Host HostMetrics

	// Runtime holds Go runtime metrics for this housegate process.
	Runtime RuntimeMetrics

	// LastSuccess records, per collection source (for example "ch", "host",
	// "runtime"), the last time that source was scraped successfully. It lets
	// the exporter publish a per-source freshness gauge so a stale source is
	// visible even when the rest of the snapshot updates normally.
	LastSuccess map[string]time.Time
}

// CHReplicaMetrics holds the curated set of ClickHouse metrics for one replica.
// Counter-style fields (query/insert totals, OS CPU seconds) are cumulative
// values read straight from ClickHouse; the exporter presents them as Prometheus
// counters. Reachable reports whether this replica answered the scrape at all;
// when it is false the value fields are not meaningful for this cycle.
type CHReplicaMetrics struct {
	// Replica is the label identifying this replica (typically its address).
	Replica string

	// Reachable is true when the replica answered this cycle's scrape.
	Reachable bool

	// QueryTotal is the cumulative number of queries the replica has run
	// (from ClickHouse's Query event counter).
	QueryTotal uint64

	// InsertTotal is the cumulative number of insert queries the replica has
	// run (from ClickHouse's InsertQuery event counter).
	InsertTotal uint64

	// MemoryTrackingBytes is the bytes currently tracked by ClickHouse's
	// memory tracker (from the MemoryTracking metric).
	MemoryTrackingBytes uint64

	// PartsActive is the number of active data parts across tables (from the
	// PartsActive / asynchronous_metrics).
	PartsActive uint64

	// ReplicationQueue is the number of tasks waiting in the replication queue
	// (from the ReplicasMaxQueueSize / asynchronous_metrics).
	ReplicationQueue uint64

	// OSCPUSeconds is the cumulative OS CPU time the ClickHouse server has used,
	// in seconds (from the OSUserTime+OSSystemTime asynchronous_metrics).
	OSCPUSeconds float64
}

// HostMetrics holds machine-level metrics for the host running this housegate.
// Disk byte counters are keyed by device name because a host may have several
// block devices and the exporter labels each series by device.
type HostMetrics struct {
	// CPUPercent is the host CPU utilization over the last cycle, 0..100
	// (summed across cores, as gopsutil reports it).
	CPUPercent float64

	// MemAvailableBytes is the memory available to userspace, in bytes.
	MemAvailableBytes uint64

	// MemTotalBytes is the total physical memory, in bytes.
	MemTotalBytes uint64

	// DiskReadBytes maps device name to its cumulative bytes read.
	DiskReadBytes map[string]uint64

	// DiskWriteBytes maps device name to its cumulative bytes written.
	DiskWriteBytes map[string]uint64

	// NetRxBytes is the cumulative bytes received across network interfaces.
	NetRxBytes uint64

	// NetTxBytes is the cumulative bytes transmitted across network interfaces.
	NetTxBytes uint64
}

// RuntimeMetrics holds Go runtime metrics for this housegate process.
type RuntimeMetrics struct {
	// Goroutines is the current number of goroutines.
	Goroutines int

	// HeapAllocBytes is the bytes of allocated heap objects (runtime
	// MemStats.HeapAlloc).
	HeapAllocBytes uint64

	// GCPauseSeconds is the cumulative stop-the-world GC pause time, in seconds
	// (runtime MemStats.PauseTotalNs converted to seconds).
	GCPauseSeconds float64
}

// Store is the lock-free holder for the latest published Snapshot. The zero
// value is ready to use: Load returns nil until the first Store call. It is
// safe for concurrent use by any number of readers and writers — Load and Store
// are each a single atomic pointer operation.
type Store struct {
	ptr atomic.Pointer[Snapshot]
}

// Load returns the most recently published snapshot, or nil if nothing has been
// published yet. It never panics and never blocks. Callers must treat the
// returned snapshot as read-only.
func (s *Store) Load() *Snapshot {
	return s.ptr.Load()
}

// Store publishes snap as the latest snapshot, atomically replacing any previous
// one. Subsequent Load calls observe snap (until the next Store). The caller
// must not mutate snap after publishing it.
func (s *Store) Store(snap *Snapshot) {
	s.ptr.Store(snap)
}
