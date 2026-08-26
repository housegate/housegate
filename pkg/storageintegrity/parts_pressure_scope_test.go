package storageintegrity

import (
	"context"
	"errors"
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

func TestReserve_ReadsOnlyTheStatementsPartitions(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p1", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__other", partition: "p0", partitionKey: "p", number: 900},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5000},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("seed Refresh: %v", err)
	}
	// Spec P D4: the exact read is hg_unsafe-only, so the safe count this test
	// asserts the hot path must not clobber is seeded by the bounded aggregate.
	if _, err := g.RefreshCounts(context.Background()); err != nil {
		t.Fatalf("seed RefreshCounts: %v", err)
	}
	conn.resetQueries()
	reservation, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"p_p0"})
	if err != nil {
		t.Fatalf("ReserveStatement: %v", err)
	}
	defer reservation.Release()
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("Reserve issued %d queries, want 1", len(queries))
	}
	if strings.Contains(queries[0], "hg_safe") {
		t.Fatalf("hot-path query touched the safe database: %q", queries[0])
	}
	args := conn.lastArgs()
	if len(args) != 3 || args[0] != "hg_unsafe" || args[1] != "db__t" || args[2] != "p0" {
		t.Fatalf("hot-path args = %v, want [hg_unsafe db__t p0]", args)
	}
	g.mu.RLock()
	other := g.snapshot[PartsKey{Database: "hg_unsafe", Table: "db__other", Partition: "p_p0"}]
	safe := g.snapshot[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	g.mu.RUnlock()
	if other != 900 || safe != 5000 {
		t.Fatalf("scoped read clobbered untouched keys: other=%d safe=%d", other, safe)
	}
}

func TestReserve_RejectsMalformedPartitionIDs(t *testing.T) {
	conn := &fakePartsConn{}
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	if _, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"p_p0", "all"}); err == nil {
		t.Fatal("mixing all with partitioned ids must fail closed")
	}
	if _, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"bogus"}); err == nil {
		t.Fatal("a partition id that is neither all nor p_-prefixed must fail closed")
	}
}

// TestRestoreBatch_NeverReadsSafeDatabasePartNames pins the one production
// caller of the exact full read. RestoreBatch is the startup recovery
// boundary; before Spec P D4 it scanned every active part name in BOTH
// databases, which is where Spec L D3(b)'s growth cliff moved to.
func TestRestoreBatch_NeverReadsSafeDatabasePartNames(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 2},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.RestoreBatch(context.Background(), nil); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("RestoreBatch issued %d queries, want 1: %v", len(queries), queries)
	}
	if !strings.Contains(queries[0], "parts.name") {
		t.Fatalf("RestoreBatch must still read exact names: %s", queries[0])
	}
	if strings.Contains(queries[0], "IN (?, ?)") {
		t.Fatalf("RestoreBatch must bind only the unsafe database: %s", queries[0])
	}
	if args := conn.lastArgs(); len(args) != 1 || args[0] != "hg_unsafe" {
		t.Fatalf("RestoreBatch args = %v, want [hg_unsafe]", args)
	}
	g.mu.RLock()
	names := g.activeParts[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	ok := g.lastFullOK
	g.mu.RUnlock()
	if len(names) != 0 {
		t.Fatalf("safe-database part names were installed: %v", names)
	}
	if !ok {
		t.Fatal("an unsafe-only exact read must still latch lastFullOK; IsFull now describes the capacity surface")
	}
}

// TestExactScope_PreservesTheThreeIsFullConsequences proves Spec P D4's scope
// split is not merely mechanical. IsFull was relaxed from "both databases" to
// "the whole unsafe database", and it drives exactly three things; each is
// asserted here against the unsafe-only exact scope rather than assumed.
//
//  1. lastFullOK, which gates Snapshot().
//  2. refreshGate.Lock() versus RLock() in refreshScope - the branch is
//     literally `if scope.IsFull(g.cfg)`, so the predicate is the selector.
//  3. RestoreBatch's restoreBlocked latch, which fails admissions closed until
//     a complete exact pass rebuilds the durable projection.
func TestExactScope_PreservesTheThreeIsFullConsequences(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 9},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 10, HardPartsPerPartition: 20,
	})

	// (1) and (2): an unsafe-only exact read is still a full read.
	if !g.exactScope().IsFull(g.cfg) {
		t.Fatal("exactScope must satisfy IsFull, or refreshScope drops to RLock and Snapshot() never latches")
	}
	if !g.countScope().IsFull(g.cfg) {
		t.Fatal("countScope must satisfy IsFull, or RefreshCounts stops maintaining lastFullOK")
	}
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, ok := g.Snapshot(); !ok {
		t.Fatal("an unsafe-only exact read must latch lastFullOK so Snapshot() is usable")
	}

	// (3): the latch still closes on a failed exact pass and reopens on a
	// successful one.
	conn.setQueryError(errors.New("boom"))
	if _, err := g.RestoreBatch(context.Background(), nil); err == nil {
		t.Fatal("a failed exact pass must fail RestoreBatch")
	}
	if _, ok := g.Snapshot(); ok {
		t.Fatal("a latched restoreBlocked must make Snapshot() unavailable")
	}
	if err := g.Allow("db__t", "p_p0"); err == nil {
		t.Fatal("a latched restoreBlocked must fail admissions closed")
	}
	conn.setQueryError(nil)
	if _, err := g.RestoreBatch(context.Background(), nil); err != nil {
		t.Fatalf("RestoreBatch after recovery: %v", err)
	}
	if err := g.Allow("db__t", "p_p0"); err != nil {
		t.Fatalf("a successful unsafe-only exact pass must clear restoreBlocked: %v", err)
	}
	// RestoreBatch clears restoreBlocked only AFTER its own exact pass has
	// already computed lastFullOK = !restoreBlocked, so Snapshot() reopens on
	// the next full pass rather than on this one. That ordering is unchanged by
	// the scope split - it is identical on the pre-split code - and the D4
	// property under test is that an hg_unsafe-only pass is still a full one.
	if _, ok := g.Snapshot(); ok {
		t.Fatal("RestoreBatch computes lastFullOK before clearing its own latch; this documents that ordering")
	}
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh after recovery: %v", err)
	}
	if _, ok := g.Snapshot(); !ok {
		t.Fatal("an hg_unsafe-only full pass must re-latch lastFullOK once restoreBlocked is clear")
	}
}
