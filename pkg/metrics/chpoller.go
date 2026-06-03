package metrics

import (
	"context"
	"fmt"
	"math"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// defaultCHPollTimeout bounds each individual system-table SELECT when the
// caller supplies a non-positive timeout.
const defaultCHPollTimeout = 5 * time.Second

// chRows is the minimal row-cursor surface CHPoller needs from a query result.
// The real clickhouse-go driver.Rows satisfies it; tests substitute a fake to
// exercise the scrape/Poll IO paths without a live ClickHouse.
type chRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// CHPoller reads ClickHouse server metrics from a single replica's system
// tables (system.metrics / system.events / system.asynchronous_metrics) over a
// native clickhouse-go/v2 connection and maps a curated subset into a
// CHReplicaMetrics. One CHPoller owns one long-lived connection to one replica;
// the collector holds one per replica it polls.
type CHPoller struct {
	addr    string
	conn    clickhouse.Conn
	timeout time.Duration

	// queryFn fetches rows for a query. It defaults to a thin wrapper over
	// conn.Query; tests reassign it to inject fake rows and drive the scrape /
	// Poll IO paths (success, query error, scan error) without a live server.
	queryFn func(ctx context.Context, query string) (chRows, error)
}

// NewCHPoller opens a native ClickHouse connection to addr and returns a poller
// bound to it. clickhouse-go dials lazily (on first query), so NewCHPoller
// against a currently-unreachable replica still succeeds and the unreachability
// surfaces from Poll instead — exactly what the fail-open collector wants.
// A non-positive timeout falls back to defaultCHPollTimeout.
func NewCHPoller(addr, database, user, password string, timeout time.Duration) (*CHPoller, error) {
	if timeout <= 0 {
		timeout = defaultCHPollTimeout
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return nil, fmt.Errorf("ch poller %s: open: %w", addr, err)
	}
	p := &CHPoller{addr: addr, conn: conn, timeout: timeout}
	p.queryFn = func(ctx context.Context, query string) (chRows, error) {
		return conn.Query(ctx, query)
	}
	return p, nil
}

// Close releases the underlying connection.
func (p *CHPoller) Close() error {
	if p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

// Poll scrapes the three system tables and returns the mapped CHReplicaMetrics.
//
// Fail-open contract: any query error returns a CHReplicaMetrics with
// Reachable=false (and the replica label set) plus a wrapped error, so the
// collector can record this replica as down without dropping the host/runtime
// portion of the snapshot. Poll never panics.
func (p *CHPoller) Poll(ctx context.Context) (CHReplicaMetrics, error) {
	metrics, err := p.scrape(ctx, "SELECT metric, toFloat64(value) FROM system.metrics")
	if err != nil {
		return p.unreachable(), fmt.Errorf("ch poller %s: system.metrics: %w", p.addr, err)
	}
	events, err := p.scrape(ctx, "SELECT event, toFloat64(value) FROM system.events")
	if err != nil {
		return p.unreachable(), fmt.Errorf("ch poller %s: system.events: %w", p.addr, err)
	}
	async, err := p.scrape(ctx, "SELECT metric, toFloat64(value) FROM system.asynchronous_metrics")
	if err != nil {
		return p.unreachable(), fmt.Errorf("ch poller %s: system.asynchronous_metrics: %w", p.addr, err)
	}
	return mapCHMetrics(p.addr, metrics, events, async), nil
}

// unreachable is the CHReplicaMetrics returned when a scrape fails: only the
// replica label is set and Reachable is false.
func (p *CHPoller) unreachable() CHReplicaMetrics {
	return CHReplicaMetrics{Replica: p.addr, Reachable: false}
}

// scrape runs a two-column (name, float64 value) query under a per-query
// timeout and collects the rows into a name→value map. The value column is cast
// to Float64 in SQL (toFloat64) so a single float64 scan target works uniformly
// across system.metrics (Int64), system.events (UInt64) and
// system.asynchronous_metrics (Float64).
func (p *CHPoller) scrape(ctx context.Context, query string) (map[string]float64, error) {
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	rows, err := p.queryFn(cctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]float64)
	for rows.Next() {
		var (
			name  string
			value float64
		)
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, rows.Err()
}

// mapCHMetrics maps the curated ClickHouse system-table keys into a
// CHReplicaMetrics. It is pure (no IO) so it is unit-testable without a live
// server, and it runs only after a successful scrape, so Reachable is always
// true here (the unreachable case is built directly by Poll on error).
//
// Resolution rules:
//   - MemoryTrackingBytes ← system.metrics["MemoryTracking"]
//   - QueryTotal/InsertTotal ← system.events["Query"]/["InsertQuery"] (cumulative)
//   - ReplicationQueue ← system.metrics["ReplicasMaxQueueSize"]
//   - PartsActive ← system.metrics["PartsActive"], else async["NumberOfActiveParts"]
//   - OSCPUSeconds ← async["OSUserTimeNormalized"], else async["OSCPUVirtualTimeMicroseconds"]/1e6
//
// Missing keys yield zero and nil source maps are tolerated (a nil-map read is a
// zero-value read in Go). Negative readings (asynchronous_metrics values are
// float64 and a transient negative is possible) are clamped to 0 for the
// unsigned counter fields rather than wrapping to a huge uint64.
func mapCHMetrics(replica string, metrics, events, async map[string]float64) CHReplicaMetrics {
	m := CHReplicaMetrics{
		Replica:   replica,
		Reachable: true,
	}

	m.MemoryTrackingBytes = clampUint64(metrics["MemoryTracking"])
	m.QueryTotal = clampUint64(events["Query"])
	m.InsertTotal = clampUint64(events["InsertQuery"])
	m.ReplicationQueue = clampUint64(metrics["ReplicasMaxQueueSize"])

	// PartsActive: prefer the system.metrics key; fall back to the
	// asynchronous_metrics name used on some versions. The primary key wins
	// even when both are present.
	if v, ok := metrics["PartsActive"]; ok {
		m.PartsActive = clampUint64(v)
	} else if v, ok := async["NumberOfActiveParts"]; ok {
		m.PartsActive = clampUint64(v)
	}

	// OSCPUSeconds: prefer the normalized per-second key; fall back to the
	// cumulative virtual-time microseconds key converted to seconds.
	if v, ok := async["OSUserTimeNormalized"]; ok {
		m.OSCPUSeconds = v
	} else if v, ok := async["OSCPUVirtualTimeMicroseconds"]; ok {
		m.OSCPUSeconds = v / 1e6
	}

	return m
}

// clampUint64 converts a float64 metric reading to uint64, clamping negatives
// and NaN to 0 so a transient bad reading never wraps to a huge counter.
func clampUint64(f float64) uint64 {
	if f < 0 || math.IsNaN(f) {
		return 0
	}
	return uint64(f)
}
