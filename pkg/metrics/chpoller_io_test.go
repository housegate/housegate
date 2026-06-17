package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeRows is an in-memory chRows that drives scrape/Poll without a live
// ClickHouse, so the IO paths (success loop, query error, scan error, rows.Err)
// are covered by docker-free unit tests.
type fakeRows struct {
	names    []string
	values   []float64
	idx      int
	scanErr  error // returned from Scan when non-nil
	errAfter error // returned from Err() after iteration
}

func (f *fakeRows) Next() bool {
	if f.idx >= len(f.names) {
		return false
	}
	f.idx++
	return true
}

func (f *fakeRows) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	*(dest[0].(*string)) = f.names[f.idx-1]
	*(dest[1].(*float64)) = f.values[f.idx-1]
	return nil
}

func (f *fakeRows) Close() error { return nil }
func (f *fakeRows) Err() error   { return f.errAfter }

// newTestPoller builds a CHPoller with an injected queryFn and no real conn.
func newTestPoller(addr string, qf func(ctx context.Context, query string) (chRows, error)) *CHPoller {
	return &CHPoller{addr: addr, timeout: time.Second, queryFn: qf}
}

// curatedQueryFn returns fake rows keyed by which system table is queried.
func curatedQueryFn(ctx context.Context, query string) (chRows, error) {
	switch {
	case strings.Contains(query, "system.metrics"):
		return &fakeRows{
			names:  []string{"MemoryTracking", "ReplicasMaxQueueSize", "PartsActive", "PartMutation"},
			values: []float64{2048, 4, 9, 2},
		}, nil
	case strings.Contains(query, "system.events"):
		return &fakeRows{names: []string{"Query", "InsertQuery"}, values: []float64{200, 50}}, nil
	case strings.Contains(query, "system.asynchronous_metrics"):
		return &fakeRows{names: []string{"OSUserTimeNormalized", "NumberOfTables"}, values: []float64{3.0, 128}}, nil
	case strings.Contains(query, "system.mutations"):
		return &fakeRows{names: []string{"pending"}, values: []float64{5}}, nil
	default:
		return &fakeRows{}, nil
	}
}

func TestPollSuccess(t *testing.T) {
	p := newTestPoller("ch-1:9000", curatedQueryFn)
	got, err := p.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll error: %v", err)
	}
	if !got.Reachable || got.Replica != "ch-1:9000" {
		t.Errorf("Reachable/Replica = %v/%q", got.Reachable, got.Replica)
	}
	if got.MemoryTrackingBytes != 2048 || got.ReplicationQueue != 4 || got.PartsActive != 9 {
		t.Errorf("metrics mismatch: %+v", got)
	}
	if got.QueryTotal != 200 || got.InsertTotal != 50 {
		t.Errorf("events mismatch: %+v", got)
	}
	if got.OSCPUSeconds != 3.0 {
		t.Errorf("OSCPUSeconds = %v, want 3.0", got.OSCPUSeconds)
	}
	if got.MutationsPending != 5 {
		t.Errorf("MutationsPending = %d, want 5", got.MutationsPending)
	}
	if got.MutationsRunning != 2 {
		t.Errorf("MutationsRunning = %d, want 2", got.MutationsRunning)
	}
	if got.TablesTotal != 128 {
		t.Errorf("TablesTotal = %d, want 128", got.TablesTotal)
	}
}

// errOnTable returns a queryFn that errors when the queried table contains
// `table`, and otherwise returns empty rows. Exercises each of Poll's three
// per-table error returns.
func errOnTable(table string) func(ctx context.Context, query string) (chRows, error) {
	return func(ctx context.Context, query string) (chRows, error) {
		if strings.Contains(query, table) {
			return nil, errors.New("query failed")
		}
		return &fakeRows{}, nil
	}
}

func TestPollUnreachableOnEachTable(t *testing.T) {
	for _, table := range []string{"system.metrics", "system.events", "system.asynchronous_metrics", "system.mutations"} {
		t.Run(table, func(t *testing.T) {
			p := newTestPoller("ch-down:9000", errOnTable(table))
			got, err := p.Poll(context.Background())
			if err == nil {
				t.Fatalf("expected error when %s fails", table)
			}
			if got.Reachable {
				t.Error("Reachable = true, want false on query failure")
			}
			if got.Replica != "ch-down:9000" {
				t.Errorf("Replica = %q, want ch-down:9000 even when unreachable", got.Replica)
			}
		})
	}
}

func TestScrapeScanError(t *testing.T) {
	qf := func(ctx context.Context, query string) (chRows, error) {
		return &fakeRows{names: []string{"X"}, values: []float64{1}, scanErr: errors.New("bad scan")}, nil
	}
	p := newTestPoller("ch-1:9000", qf)
	if _, err := p.Poll(context.Background()); err == nil {
		t.Fatal("expected scan error to propagate")
	}
}

func TestScrapeRowsErr(t *testing.T) {
	qf := func(ctx context.Context, query string) (chRows, error) {
		return &fakeRows{errAfter: errors.New("rows iteration failed")}, nil
	}
	p := newTestPoller("ch-1:9000", qf)
	if _, err := p.Poll(context.Background()); err == nil {
		t.Fatal("expected rows.Err() to propagate")
	}
}

func TestNewCHPollerBuildsConnAndDefaults(t *testing.T) {
	// clickhouse.Open dials lazily, so this succeeds without a live server and
	// exercises the constructor + default-timeout fallback + Close.
	p, err := NewCHPoller("127.0.0.1:9000", "db", "user", "pw", 0)
	if err != nil {
		t.Fatalf("NewCHPoller: %v", err)
	}
	if p.timeout != defaultCHPollTimeout {
		t.Errorf("timeout = %v, want default %v", p.timeout, defaultCHPollTimeout)
	}
	if p.queryFn == nil {
		t.Error("queryFn not wired")
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestCloseNilConn(t *testing.T) {
	p := &CHPoller{addr: "x"}
	if err := p.Close(); err != nil {
		t.Errorf("Close with nil conn: %v", err)
	}
}
