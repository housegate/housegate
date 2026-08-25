package storageintegrity

import (
	"context"
	"strings"
	"testing"
)

func TestBuildAggregateSnapshotQuery_IsGroupedAndBoundToBothDatabases(t *testing.T) {
	g := NewPartsPressureGuard(&fakePartsConn{}, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	query, args := g.BuildAggregateSnapshotQuery()
	for _, want := range []string{"count()", "GROUP BY", "parts.database IN (?, ?)", "parts.active"} {
		if !strings.Contains(query, want) {
			t.Fatalf("aggregate query %q missing %q", query, want)
		}
	}
	if strings.Contains(query, "parts.name") {
		t.Fatalf("aggregate query must not read part names: %q", query)
	}
	if len(args) != 2 || args[0] != "hg_unsafe" || args[1] != "hg_safe" {
		t.Fatalf("aggregate args = %v", args)
	}
}

func TestRefreshCounts_NeverReadsSafeDatabasePartNames(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 2},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	snapshot, err := g.RefreshCounts(context.Background())
	if err != nil {
		t.Fatalf("RefreshCounts: %v", err)
	}
	if snapshot[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}] != 5 {
		t.Fatalf("safe count missing: %v", snapshot)
	}
	g.mu.RLock()
	names := g.activeParts[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	g.mu.RUnlock()
	if len(names) != 0 {
		t.Fatalf("safe database part names were read: %v", names)
	}
	for _, query := range conn.recordedQueries() {
		if strings.Contains(query, "parts.name") && strings.Contains(query, "hg_safe") {
			t.Fatalf("a query read safe-database part names: %q", query)
		}
	}
}

func TestRefreshLiveKeys_ReadsOnlyKeysWithLiveOwnership(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__a", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__b", partition: "p0", partitionKey: "p", number: 1},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	reservation, err := g.Reserve(context.Background(), "db__a", []string{"p_p0"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	defer reservation.Release()
	conn.resetQueries()
	if err := g.RefreshLiveKeys(context.Background()); err != nil {
		t.Fatalf("RefreshLiveKeys: %v", err)
	}
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("RefreshLiveKeys issued %d queries, want 1", len(queries))
	}
	args := conn.lastArgs()
	if len(args) != 3 || args[1] != "db__a" || args[2] != "p0" {
		t.Fatalf("live-key read args = %v, want the reserved table and partition only", args)
	}
}
