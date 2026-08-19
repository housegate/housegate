package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakePartsRow struct {
	database, table, partition, partitionKey string
	number                                   uint64
}

type fakePartsConn struct {
	mu                sync.Mutex
	rows              []fakePartsRow
	inventory         []fakePartInventoryRow
	queryErr          error
	queries           []string
	blockUntilContext bool
	queryStarted      chan struct{}
}

type fakePartInventoryRow struct {
	database, table, partition, partitionKey, partName string
}

func (c *fakePartsConn) Exec(context.Context, string, ...any) error { return nil }

func (c *fakePartsConn) Query(ctx context.Context, query string, _ ...any) (MergeRows, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	queryErr := c.queryErr
	rows := append([]fakePartsRow(nil), c.rows...)
	inventory := append([]fakePartInventoryRow(nil), c.inventory...)
	blockUntilContext := c.blockUntilContext
	queryStarted := c.queryStarted
	c.mu.Unlock()
	if queryStarted != nil {
		select {
		case queryStarted <- struct{}{}:
		default:
		}
	}
	if blockUntilContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if queryErr != nil {
		return nil, queryErr
	}
	if inventory == nil {
		for _, row := range rows {
			for idx := uint64(1); idx <= row.number; idx++ {
				inventory = append(inventory, fakePartInventoryRow{
					database: row.database, table: row.table, partition: row.partition,
					partitionKey: row.partitionKey, partName: fmt.Sprintf("%s_part_%d", row.partition, idx),
				})
			}
		}
	}
	return &fakePartsRows{rows: inventory}, nil
}

func (c *fakePartsConn) setRows(rows ...fakePartsRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append([]fakePartsRow(nil), rows...)
	c.inventory = nil
}

func (c *fakePartsConn) setInventory(rows ...fakePartInventoryRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = nil
	c.inventory = append([]fakePartInventoryRow{}, rows...)
}

func (c *fakePartsConn) setQueryError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryErr = err
}

type fakePartsRows struct {
	rows []fakePartInventoryRow
	i    int
}

func (r *fakePartsRows) Next() bool { return r.i < len(r.rows) }
func (r *fakePartsRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	*(dest[0].(*string)) = row.database
	*(dest[1].(*string)) = row.table
	*(dest[2].(*string)) = row.partition
	*(dest[3].(*string)) = row.partitionKey
	*(dest[4].(*string)) = row.partName
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
	for _, want := range []string{"system.parts", "system.tables", "partition_key", "parts.name", "active", "ORDER BY", "'hg_unsafe'", "'hg_safe'"} {
		if !strings.Contains(query, want) {
			t.Fatalf("query %q missing %q", query, want)
		}
	}
	if strings.Contains(query, "partition_id") {
		t.Fatal("must read partition text, not partition_id (SipHash for String keys)")
	}
	if strings.Contains(query, "count()") || strings.Contains(query, "GROUP BY") {
		t.Fatal("cleanup proof requires exact active part names, not only aggregate counts")
	}
}

func TestPartsPressureGuard_RefreshRetainsExactPartInventory(t *testing.T) {
	guard, conn := pressureFixture()
	conn.setInventory(
		fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_1_1_0"},
		fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_2_2_0"},
	)
	snapshot, err := guard.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := snapshot[PartsKey{"hg_unsafe", "db__t", "p_a"}]; got != 2 {
		t.Fatalf("snapshot count=%d want 2 exact inventory rows", got)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if _, ok := guard.activeParts[key]["a_2_2_0"]; !ok {
		t.Fatalf("exact inventory=%v missing candidate", guard.activeParts[key])
	}
}

func TestPartsPressureGuard_RefreshMapsPartitionsToLogicalIDs(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3},
		fakePartsRow{"hg_unsafe", "db__u", "tuple()", "", 1},
		fakePartsRow{"hg_safe", "db__t", "a", "p", 7},
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

func TestPartsPressureGuard_TupleTextUsesTablePartitionMetadata(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__partitioned", "tuple()", "p", 3},
		fakePartsRow{"hg_unsafe", "db__unpartitioned", "tuple()", "", 1},
	)
	snapshot, err := guard.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := snapshot[PartsKey{"hg_unsafe", "db__partitioned", "p_tuple()"}]; got != 3 {
		t.Fatalf("partitioned tuple() value = %d parts, want 3", got)
	}
	if got := snapshot[PartsKey{"hg_unsafe", "db__unpartitioned", "all"}]; got != 1 {
		t.Fatalf("unpartitioned table = %d parts, want 1", got)
	}
	if err := guard.Allow("db__partitioned", "p_tuple()"); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("partitioned tuple() at soft limit must be refused: %v", err)
	}
	if err := guard.Allow("db__unpartitioned", "all"); err != nil {
		t.Fatalf("unpartitioned below soft must be allowed: %v", err)
	}
}

func TestPartsPressureGuard_AllowBelowAtAboveSoftAndHard(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "below", "p", 2},
		fakePartsRow{"hg_unsafe", "db__t", "at_soft", "p", 3},
		fakePartsRow{"hg_unsafe", "db__t", "above_soft", "p", 4},
		fakePartsRow{"hg_unsafe", "db__t", "at_hard", "p", 5},
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
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	conn.setQueryError(errors.New("connection reset"))
	if _, err := guard.Refresh(context.Background()); err == nil {
		t.Fatal("refresh error must surface")
	}
	if err := guard.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("last good snapshot must remain usable: %v", err)
	}
}

func TestPartsPressureGuard_ExpiredSnapshotFailsClosedAndRecovers(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	now := time.Unix(100, 0)
	guard.now = func() time.Time { return now }
	guard.cfg.SnapshotTTL = time.Second
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := guard.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("fresh snapshot must allow: %v", err)
	}

	now = now.Add(2 * time.Second)
	conn.setQueryError(errors.New("connection reset"))
	if _, err := guard.Refresh(context.Background()); err == nil {
		t.Fatal("failed refresh must surface")
	}
	var unavailable *BackpressureError
	if err := guard.Allow("db__t", "p_a"); !errors.As(err, &unavailable) || unavailable.Kind != "unavailable" {
		t.Fatalf("expired snapshot must fail closed: %v", err)
	}

	conn.setQueryError(nil)
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh: %v", err)
	}
	if err := guard.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("successful refresh must recover admission: %v", err)
	}
}

func TestPartsPressureGuard_RefreshTimesOut(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.RefreshTimeout = 20 * time.Millisecond
	conn.mu.Lock()
	conn.blockUntilContext = true
	conn.queryStarted = make(chan struct{}, 1)
	conn.mu.Unlock()
	started := time.Now()
	if _, err := guard.Refresh(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Refresh error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Refresh took %s, want bounded by refresh timeout", elapsed)
	}
}

type partsTransportError struct{ message string }

func (e *partsTransportError) Error() string { return e.message }

func TestPartsPressureGuard_ReservePreservesRefreshCauses(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		guard, conn := pressureFixture()
		cause := &partsTransportError{message: "connection reset"}
		conn.setQueryError(cause)
		_, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
		var got *partsTransportError
		if !errors.Is(err, ErrBackpressure) || !errors.Is(err, cause) || !errors.As(err, &got) {
			t.Fatalf("Reserve error = %v, want ErrBackpressure and transport cause", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		guard, conn := pressureFixture()
		guard.cfg.RefreshTimeout = 20 * time.Millisecond
		conn.mu.Lock()
		conn.blockUntilContext = true
		conn.mu.Unlock()
		_, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
		if !errors.Is(err, ErrBackpressure) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Reserve error = %v, want ErrBackpressure and deadline cause", err)
		}
	})
}

func TestPartsPressureGuard_ReservePreventsConcurrentOversubscription(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	const callers = 16
	start := make(chan struct{})
	type result struct {
		reservation PartsReservation
		err         error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
			results <- result{reservation: reservation, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var winner PartsReservation
	refused := 0
	for got := range results {
		if got.err == nil {
			if winner != nil {
				t.Fatal("more than one reservation passed at soft-1")
			}
			winner = got.reservation
			continue
		}
		if !errors.Is(got.err, ErrBackpressure) {
			t.Fatalf("reservation error = %v, want ErrBackpressure", got.err)
		}
		refused++
	}
	if winner == nil || refused != callers-1 {
		t.Fatalf("winner=%v refused=%d want one winner and %d refusals", winner != nil, refused, callers-1)
	}

	winner.Release()
	replacement, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("released capacity must be reusable: %v", err)
	}
	replacement.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3})
	if _, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("committed part at soft limit must refuse: %v", err)
	}
}

func TestPartsPressureGuard_DelayedVisibilityKeepsCommittedCapacityReserved(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("initial Reserve: %v", err)
	}
	reservation.Commit()

	// ClickHouse still reports the pre-write count. A successful refresh must
	// not discard the committed slot until system.parts exposes its growth.
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("delayed Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("committed capacity after delayed snapshot = %d, want 1", got)
	}

	const callers = 8
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, reserveErr := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
			results <- reserveErr
		}()
	}
	wg.Wait()
	close(results)
	for reserveErr := range results {
		if !errors.Is(reserveErr, ErrBackpressure) {
			t.Fatalf("concurrent delayed-visibility Reserve = %v, want ErrBackpressure", reserveErr)
		}
	}

	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("visible Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("committed capacity after visible growth = %d, want 0", got)
	}
}

func TestPartsPressureGuard_RefreshBeforeCommitAbsorbsOriginalReservation(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	guard.cfg.SoftPartsPerPartition = 4
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// The source write becomes visible while the caller still holds the
	// reservation. Commit must retain the original count/generation so the
	// already-visible N+1 snapshot absorbs it instead of creating a phantom slot.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("pre-commit Refresh: %v", err)
	}
	reservation.Commit()
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable post-commit Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("committed capacity after pre-commit visibility = %d, want 0", got)
	}
	if replacement, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); err != nil {
		t.Fatalf("stable N+1 snapshot must leave one soft-limit slot: %v", err)
	} else {
		replacement.Release()
	}
}

func TestPartsPressureGuard_ReleaseAfterCommitCancelsPendingReservation(t *testing.T) {
	guard, _ := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	reservation.Commit()
	reservation.Release()
	reservation.Release()
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("committed capacity after cancel = %d, want 0", got)
	}
	if replacement, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); err != nil {
		t.Fatalf("canceled committed capacity must be reusable: %v", err)
	} else {
		replacement.Release()
	}
}

func TestPartsPressureGuard_CancelOneOfAmbiguousCohortRecomputesAggregateDebt(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	second.Commit()

	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("one visible increment must leave one aggregate debt, committed = %d", got)
	}
	first.Release()
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("canceling either ambiguous identity must recompute aggregate debt, committed = %d", got)
	}
	second.Release()
}

func TestPartsPressureGuard_FinalizedCohortCapacityCannotBeReused(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	second.Commit()

	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 3})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("first visible Refresh: %v", err)
	}
	first.Finalize()
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("finalized first credit must not cover second identity, committed = %d", got)
	}
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 4})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("second visible Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("second distinct growth must cover second identity, committed = %d", got)
	}
	second.Release()
}

func TestPartsPressureGuard_CleanupReplacementAtSameCountDoesNotLeaveDebt(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 3
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()

	// The first candidate is visible at soft-1 when the second statement takes
	// its baseline, then exact cleanup removes it while the second is queued.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, first)

	// The second write restores the same count (N -> N-1 -> N). A stable N
	// snapshot is sufficient; requiring N+1 would leave a permanent phantom debt.
	second.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("same-count Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("committed capacity after cleanup/replacement = %d, want 0", got)
	}
	if replacement, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); err != nil {
		t.Fatalf("stable soft-1 replacement must leave one slot: %v", err)
	} else {
		replacement.Release()
	}
}

func TestPartsPressureGuard_ExactCleanupDistinguishesOffsettingVisibleWrite(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 4
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	cleaned := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_cleaned"}
	replacement := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_replacement"}
	conn.setInventory(base)

	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	if err := first.Commit(CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: cleaned.partName}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	conn.setInventory(base, cleaned)
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	if err := second.Commit(CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: replacement.partName}); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	if err := first.PrepareCleanupProof(context.Background(), []CandidatePart{{TableID: "db.t", PartitionID: "p_a", PartName: cleaned.partName}}); err != nil {
		t.Fatalf("PrepareCleanupProof: %v", err)
	}
	// A was really removed, while B became visible before the post-cleanup
	// inventory. The total stays at two; exact names must clear B's debt without
	// treating an absent/no-op A as a decrement.
	conn.setInventory(base, replacement)
	if err := first.ReleaseCleaned(context.Background()); err != nil {
		t.Fatalf("ReleaseCleaned: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("offsetting visible replacement debt=%d want 0", got)
	}
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("stable same-count inventory recreated debt=%d", got)
	}
}

func TestPartsPressureGuard_CandidateMustMatchReservedTableAndPartition(t *testing.T) {
	guard, _ := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	for _, candidate := range []CandidatePart{
		{TableID: "other.t", PartitionID: "p_a", PartName: "a_new"},
		{TableID: "db.t", PartitionID: "p_other", PartName: "a_new"},
		{TableID: "", PartitionID: "p_a", PartName: "a_new"},
	} {
		reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		if err := reservation.Commit(candidate); err == nil {
			t.Fatalf("Commit accepted mismatched candidate %+v", candidate)
		}
		key := PartsKey{"hg_unsafe", "db__t", "p_a"}
		if got := guard.committed[key]; got != 1 {
			t.Fatalf("mismatched candidate must remain charged, committed=%d", got)
		}
		reservation.Release()
	}
}

func TestPartsPressureGuard_ExactCoverageDoesNotDoubleCountAggregateGrowth(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 6
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	exact := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_exact"}
	unknown := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_unknown"}
	conn.setInventory(base)
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	if err := first.Commit(CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: exact.partName}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	conn.setInventory(base, exact)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("exact-only Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("one exact part covered two slots: debt=%d want 1", got)
	}

	conn.setInventory(base, exact, unknown)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("second-growth Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("distinct exact+aggregate growth debt=%d want 0", got)
	}
}

func TestPartsPressureGuard_DuplicateExactCandidateCannotCoverTwoReservations(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 5
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base)
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	if err := first.Commit(shared); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := second.Commit(shared); err == nil {
		t.Fatal("second reservation claimed an already-owned exact candidate")
	}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("one exact part covered duplicate reservations: debt=%d want 1", got)
	}
}

func TestPartsPressureGuard_InFlightCleanupRebasesQueuedReplacement(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}

	// In production the next statement reserves before it waits on the source
	// frontier. The first part can therefore be visible while its ingress call
	// still owns an uncommitted reservation.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, first)
	second.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("replacement Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("in-flight cleanup/replacement debt = %d, want 0", got)
	}
}

func TestPartsPressureGuard_CleanupRebasesOlderReservationQueuedBehind(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	guard.cfg.SoftPartsPerPartition = 8
	queued, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("queued Reserve: %v", err)
	}
	cleaner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("cleaner Reserve: %v", err)
	}
	// The newer reservation reaches the source frontier first and cleans the
	// part represented in the older reservation's baseline.
	prepareExactCleanup(t, cleaner, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, cleaner)
	queued.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("older queued replacement debt=%d, want 0", got)
	}
}

func TestPartsPressureGuard_CleanupDoesNotRebaseReservationBeforeCandidateVisibility(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 6
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	cleaned := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_cleaned"}
	replacement := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_replacement"}
	conn.setInventory(base)
	queued, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("queued Reserve: %v", err)
	}
	cleaner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("cleaner Reserve: %v", err)
	}
	conn.setInventory(base, cleaned)
	prepareExactCleanup(t, cleaner, CandidatePart{PartitionID: "p_a", PartName: cleaned.partName})
	conn.setInventory(base)
	releaseAfterExactCleanup(t, cleaner)

	queued.Commit()
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable pre-write Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("cleanup of part absent from queued baseline cleared debt=%d want 1", got)
	}
	conn.setInventory(base, replacement)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("replacement Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("visible replacement debt=%d want 0", got)
	}
}

func TestPartsPressureGuard_CleanupReplacementRequiresPostCleanupSnapshot(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 4
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()

	// The second reservation's baseline includes the first part. Exact cleanup
	// rebases that dependency, but the cached count=2 snapshot predates cleanup
	// and therefore cannot prove that the second part is visible.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, first)
	second.Commit()

	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("pre-cleanup snapshot cleared replacement debt: committed = %d, want 1", got)
	}

	// Only a successful refresh after cleanup that observes the replacement may
	// absorb the reservation.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("post-cleanup Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("post-cleanup visible replacement debt = %d, want 0", got)
	}
}

func TestPartsPressureGuard_CleanupRebasesFinalizedReplacementUntilVisible(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	second.Commit()
	second.Finalize()
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, first)

	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("finalized replacement used stale snapshot: committed = %d, want 1", got)
	}
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("replacement Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("visible finalized replacement debt = %d, want 0", got)
	}
	if got := len(guard.liveReservations); got != 0 {
		t.Fatalf("finalized visible reservation handles = %d, want 0", got)
	}
}

func TestPartsPressureGuard_PartialCohortCleanupReplacementConverges(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 8
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()
	other, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("other Reserve: %v", err)
	}
	other.Commit()
	// Only one member of the two-reservation cohort is visible.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	replacement, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("replacement Reserve: %v", err)
	}
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_part_2"})
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	releaseAfterExactCleanup(t, first)
	replacement.Commit()
	replacement.Finalize()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	// The other candidate was absent, so its cleanup is a no-op; it releases its
	// own debt without inventing another decrement.
	prepareExactCleanup(t, other, CandidatePart{PartitionID: "p_a", PartName: "a_missing"})
	releaseAfterExactCleanup(t, other)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("partial cohort replacement debt=%d, want 0", got)
	}
}

func TestPartsPressureGuard_NoOpCleanupDoesNotRebaseDescendant(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	// Empty/absent-candidate cleanup leaves the physical count unchanged.
	prepareExactCleanup(t, first, CandidatePart{PartitionID: "p_a", PartName: "a_missing"})
	releaseAfterExactCleanup(t, first)
	second.Commit()
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("no-op cleanup undercharged descendant: committed=%d want 1", got)
	}
	second.Release()
}

func TestPartsPressureGuard_PreexistingCandidateMustDisappearAfterCleanup(t *testing.T) {
	guard, conn := pressureFixture()
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	candidate := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_candidate"}
	conn.setInventory(base)
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	conn.setInventory(base, candidate)
	prepareExactCleanup(t, reservation, CandidatePart{PartitionID: "p_a", PartName: candidate.partName})
	if err := reservation.ReleaseCleaned(context.Background()); !errors.Is(err, ErrCleanupProofPending) {
		t.Fatalf("still-active cleanup proof=%v want ErrCleanupProofPending", err)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("still-active exact candidate must make inventory unavailable")
	}
	conn.setInventory(base)
	if err := reservation.ReleaseCleaned(context.Background()); err != nil {
		t.Fatalf("proof retry after exact removal: %v", err)
	}
}

func TestPartsPressureGuard_CleanupRefreshFailureKeepsDebtAndInvalidatesSnapshot(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	first.Commit()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	if err := first.PrepareCleanupProof(context.Background(), []CandidatePart{{TableID: "db.t", PartitionID: "p_a", PartName: "a_part_2"}}); err != nil {
		t.Fatalf("PrepareCleanupProof: %v", err)
	}
	guard.cfg.RefreshTimeout = 20 * time.Millisecond
	conn.mu.Lock()
	conn.blockUntilContext = true
	conn.mu.Unlock()
	err = first.ReleaseCleaned(context.Background())
	if !errors.Is(err, ErrCleanupProofPending) || !errors.Is(err, ErrBackpressure) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReleaseCleaned error=%v, want pressure deadline", err)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("failed cleanup proof must make snapshot unavailable")
	}
	conn.mu.Lock()
	conn.blockUntilContext = false
	conn.mu.Unlock()
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	pendingSnapshot, err := guard.Refresh(context.Background())
	if err != nil {
		t.Fatalf("background recovery Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := pendingSnapshot[key]; got != 1 {
		t.Fatalf("successful fenced Refresh snapshot=%v, want physical count 1 for metrics", pendingSnapshot)
	}
	if _, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("new admission before proof retry=%v, want unavailable pressure", err)
	}
	if err := first.ReleaseCleaned(context.Background()); err != nil {
		t.Fatalf("cleanup proof retry: %v", err)
	}
	second.Commit()
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("cleanup proof retry descendant debt=%d, want 1 before replacement visibility", got)
	}
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("replacement Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("visible replacement debt=%d, want 0", got)
	}
	second.Release()
}

func TestPartsPressureGuard_NoWriteReleaseDoesNotRebaseDescendant(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	guard.cfg.SoftPartsPerPartition = 5
	first, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	first.Commit()

	// The count growth may be unrelated to the first statement. A no-write proof
	// cancels its slot but must not attribute that ambiguous growth to its ID or
	// lower a descendant's baseline; only exact part cleanup has that meaning.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	second, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("second Reserve: %v", err)
	}
	first.Release()
	second.Commit()
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("stable Refresh: %v", err)
	}
	key := PartsKey{Database: "hg_unsafe", Table: "db__t", Partition: "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("no-write release rebased descendant: committed = %d, want 1", got)
	}
	second.Release()
}

func prepareExactCleanup(t *testing.T, reservation PartsReservation, candidates ...CandidatePart) {
	t.Helper()
	for idx := range candidates {
		if candidates[idx].TableID == "" {
			candidates[idx].TableID = "db.t"
		}
	}
	if err := reservation.PrepareCleanupProof(context.Background(), candidates); err != nil {
		t.Fatalf("PrepareCleanupProof: %v", err)
	}
}

func releaseAfterExactCleanup(t *testing.T, reservation PartsReservation) {
	t.Helper()
	if err := reservation.ReleaseCleaned(context.Background()); err != nil {
		t.Fatalf("ReleaseCleaned: %v", err)
	}
}

func TestPartsPressureGuard_ReserveIsAllOrNothing(t *testing.T) {
	guard, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2},
		fakePartsRow{"hg_unsafe", "db__t", "b", "p", 3},
	)
	if _, err := guard.Reserve(context.Background(), "db__t", []string{"p_a", "p_b"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("mixed reservation must refuse: %v", err)
	}
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("failed multi-partition reservation leaked capacity: %v", err)
	}
	reservation.Release()
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
	if LogicalPartitionID("tuple()", false) != "p_tuple()" || LogicalPartitionID("tuple()", true) != "all" || LogicalPartitionID("2026", false) != "p_2026" || LogicalPartitionID("", false) != "p_" {
		t.Fatal("LogicalPartitionID mapping")
	}
}
