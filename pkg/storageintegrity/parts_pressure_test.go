package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakePartsRow struct {
	database, table, partition string
	number                     uint64
}

type fakePartsConn struct {
	rows     []fakePartsRow
	queryErr error
	queries  []string
}

func (c *fakePartsConn) Exec(context.Context, string, ...any) error { return nil }

func (c *fakePartsConn) Query(_ context.Context, query string, _ ...any) (MergeRows, error) {
	c.queries = append(c.queries, query)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakePartsRows{rows: c.rows}, nil
}

type fakePartsRows struct {
	rows []fakePartsRow
	i    int
}

func (r *fakePartsRows) Next() bool { return r.i < len(r.rows) }
func (r *fakePartsRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	*(dest[0].(*string)) = row.database
	*(dest[1].(*string)) = row.table
	*(dest[2].(*string)) = row.partition
	*(dest[3].(*uint64)) = row.number
	return nil
}
func (r *fakePartsRows) Err() error   { return nil }
func (r *fakePartsRows) Close() error { return nil }

func pressureFixture(rows ...fakePartsRow) (*PartsPressureGuard, *fakePartsConn) {
	conn := &fakePartsConn{rows: rows}
	guard := NewPartsPressureGuard(conn, PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	return guard, conn
}

func TestPartsPressureGuard_BuildSnapshotQuery(t *testing.T) {
	guard, _ := pressureFixture()
	query := guard.BuildSnapshotQuery()
	for _, want := range []string{"system.parts", "active", "GROUP BY database, table, partition", "'hg_unsafe'", "'hg_safe'"} {
		if !strings.Contains(query, want) {
			t.Fatalf("query %q missing %q", query, want)
		}
	}
	if strings.Contains(query, "partition_id") {
		t.Fatal("must group by partition text, not partition_id (SipHash for String keys)")
	}
}

func TestPartsPressureGuard_RefreshMapsPartitionsToLogicalIDs(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "a", 3},
		fakePartsRow{"hg_unsafe", "db__u", "tuple()", 1},
		fakePartsRow{"hg_safe", "db__t", "a", 7},
	)
	snapshot, err := guard.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snapshot[PartsKey{"hg_unsafe", "db__t", "p_a"}] != 3 || snapshot[PartsKey{"hg_unsafe", "db__u", "all"}] != 1 || snapshot[PartsKey{"hg_safe", "db__t", "p_a"}] != 7 {
		t.Fatalf("snapshot = %v", snapshot)
	}
	if got, ok := guard.Snapshot(); !ok || len(got) != 3 {
		t.Fatalf("Snapshot() = %v %v", got, ok)
	}
}

func TestPartsPressureGuard_AllowBelowAtAboveSoftAndHard(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "below", 2},
		fakePartsRow{"hg_unsafe", "db__t", "at_soft", 3},
		fakePartsRow{"hg_unsafe", "db__t", "above_soft", 4},
		fakePartsRow{"hg_unsafe", "db__t", "at_hard", 5},
	)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := guard.Allow("db__t", "p_below"); err != nil {
		t.Fatalf("below soft must be allowed: %v", err)
	}
	if err := guard.Allow("db__t", "p_never_seen"); err != nil {
		t.Fatalf("unknown partition (0 parts) must be allowed: %v", err)
	}
	for partition, kind := range map[string]string{"p_at_soft": "soft", "p_above_soft": "soft", "p_at_hard": "hard"} {
		err := guard.Allow("db__t", partition)
		var backpressure *BackpressureError
		if !errors.As(err, &backpressure) || !errors.Is(err, ErrBackpressure) {
			t.Fatalf("%s: err = %v want BackpressureError", partition, err)
		}
		if backpressure.Kind != kind || backpressure.Table != "db__t" || backpressure.Partition != partition {
			t.Fatalf("%s: %+v", partition, backpressure)
		}
		if !strings.HasPrefix(err.Error(), "storage_integrity: back-pressure") {
			t.Fatalf("message prefix: %q", err.Error())
		}
	}
	err := guard.Allow("db__t", "p_at_soft")
	if !strings.Contains(err.Error(), "hg_unsafe.db__t") || !strings.Contains(err.Error(), "3 active parts") || !strings.Contains(err.Error(), "soft limit 3") {
		t.Fatalf("message must name table, count and limit: %q", err.Error())
	}
}

func TestPartsPressureGuard_AllowWithoutSnapshotIsUnavailable(t *testing.T) {
	guard, _ := pressureFixture()
	err := guard.Allow("db__t", "p_a")
	var backpressure *BackpressureError
	if !errors.As(err, &backpressure) || backpressure.Kind != "unavailable" {
		t.Fatalf("err = %v want unavailable BackpressureError", err)
	}
}

func TestPartsPressureGuard_RefreshErrorKeepsLastGoodSnapshot(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", 1})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	conn.queryErr = errors.New("connection reset")
	if _, err := guard.Refresh(context.Background()); err == nil {
		t.Fatal("refresh error must surface")
	}
	if err := guard.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("last good snapshot must remain usable: %v", err)
	}
}

func TestPartsPressureGuard_InvalidateSignalsOnce(t *testing.T) {
	guard, _ := pressureFixture()
	guard.Invalidate()
	guard.Invalidate()
	select {
	case <-guard.Invalidated():
	default:
		t.Fatal("Invalidate must signal the poller")
	}
	select {
	case <-guard.Invalidated():
		t.Fatal("second signal must be coalesced")
	default:
	}
}

func TestLogicalPartitionID(t *testing.T) {
	if LogicalPartitionID("tuple()") != "all" || LogicalPartitionID("2026") != "p_2026" || LogicalPartitionID("") != "p_" {
		t.Fatal("LogicalPartitionID mapping")
	}
}
