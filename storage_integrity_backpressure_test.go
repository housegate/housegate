package housegate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type rootPartsRow struct {
	db, table, partition string
	n                    uint64
}

type rootPartsConn struct {
	mu      sync.Mutex
	rows    []rootPartsRow
	queries atomic.Int64
	err     error
}

func (c *rootPartsConn) setRows(rows []rootPartsRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = rows
}

func (c *rootPartsConn) Exec(context.Context, string, ...any) error { return nil }
func (c *rootPartsConn) Query(context.Context, string, ...any) (sicore.MergeRows, error) {
	c.queries.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	return &rootPartsRows{rows: append([]rootPartsRow(nil), c.rows...)}, nil
}

type rootPartsRows struct {
	rows []rootPartsRow
	i    int
}

func (r *rootPartsRows) Next() bool { return r.i < len(r.rows) }
func (r *rootPartsRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	*(dest[0].(*string)) = row.db
	*(dest[1].(*string)) = row.table
	*(dest[2].(*string)) = row.partition
	*(dest[3].(*uint64)) = row.n
	return nil
}
func (r *rootPartsRows) Err() error   { return nil }
func (r *rootPartsRows) Close() error { return nil }

func TestPartsPressureSupervisor_RefreshSetsGauges(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "db__t", "a", 5},
		{"hg_safe", "db__t", "a", 9},
	}}
	guard := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe", SoftPartsPerPartition: 3, HardPartsPerPartition: 4})
	sup := NewStorageIntegrityPartsPressureSupervisor(guard, time.Second, "hg_unsafe", "hg_safe")
	if err := sup.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := testutil.ToFloat64(storageIntegrityUnsafeParts.WithLabelValues("db__t", "p_a")); got != 5 {
		t.Fatalf("unsafe gauge = %v want 5", got)
	}
	if got := testutil.ToFloat64(storageIntegritySafeParts.WithLabelValues("db__t", "p_a")); got != 9 {
		t.Fatalf("safe gauge = %v want 9", got)
	}
	if err := sup.Allow("db__t", "p_a"); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("supervisor must delegate Allow: %v", err)
	}

	conn.setRows([]rootPartsRow{{"hg_safe", "db__t", "a", 9}})
	if err := sup.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := testutil.ToFloat64(storageIntegrityUnsafeParts.WithLabelValues("db__t", "p_a")); got != 0 {
		t.Fatalf("stale unsafe gauge = %v want 0 after reset", got)
	}
}

func TestPartsPressureSupervisor_RunRefreshesOnTickAndInvalidate(t *testing.T) {
	conn := &rootPartsConn{}
	guard := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	sup := NewStorageIntegrityPartsPressureSupervisor(guard, 20*time.Millisecond, "hg_unsafe", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for conn.queries.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if conn.queries.Load() < 2 {
		t.Fatalf("ticker refresh count = %d, want >= 2", conn.queries.Load())
	}
	before := conn.queries.Load()
	sup.Invalidate()
	deadline = time.Now().Add(2 * time.Second)
	for conn.queries.Load() == before && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if conn.queries.Load() == before {
		t.Fatal("Invalidate must trigger a prompt refresh")
	}
}

func TestStorageIntegrityBackpressureMetricsRegisteredOnce(t *testing.T) {
	storageIntegrityUnsafeParts.WithLabelValues("t", "p").Set(0)
	storageIntegritySafeParts.WithLabelValues("t", "p").Set(0)
	storageIntegrityBackpressureTotal.WithLabelValues("t").Add(0)
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := map[string]bool{}
	for _, mf := range mfs {
		found[mf.GetName()] = true
	}
	for _, name := range []string{"storage_integrity_unsafe_parts", "storage_integrity_safe_parts", "storage_integrity_backpressure_total"} {
		if !found[name] {
			t.Fatalf("metric %s not registered on the default registry", name)
		}
	}
}
