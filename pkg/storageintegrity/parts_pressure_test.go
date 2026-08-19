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

func TestPartsPressureGuard_UnboundCommittedSlotCannotBeCoveredByUnrelatedGrowth(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	reservation.CommitIndeterminate()

	// A lost Prepare response has no exact candidate identity yet. Unrelated
	// aggregate growth cannot prove that statement's write visible or surrender
	// its source-frontier capacity slot to a queued statement.
	conn.setRows(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 2})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if competing, err := guard.ReserveStatement(context.Background(), "competing", "db__t", []string{"p_a"}); err == nil {
		competing.Release()
		t.Fatal("unrelated growth covered an unbound committed reservation")
	} else if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("competing Reserve=%v, want ErrBackpressure", err)
	}
	reservation.Release()
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
	if err := reservation.Commit(CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_part_3"}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
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

func TestPartsPressureGuard_CleanupProofRejectsCandidateClaimedByAnotherReservation(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 5
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base)
	owner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("owner Reserve: %v", err)
	}
	cleaner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("cleaner Reserve: %v", err)
	}
	if err := owner.Commit(shared); err != nil {
		t.Fatalf("owner Commit: %v", err)
	}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if err := cleaner.PrepareCleanupProof(context.Background(), []CandidatePart{shared}); err == nil {
		t.Fatal("cleanup proof accepted a candidate owned by another reservation")
	} else if !errors.Is(err, ErrCleanupProofPending) || !errors.Is(err, ErrBackpressure) {
		t.Fatalf("cleanup proof error=%v, want pending back-pressure", err)
	}
	owner.Release()
	cleaner.Release()
}

func TestPartsPressureGuard_RestoreCandidateConflictDoesNotLeakCapacity(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 3
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})

	if _, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "finalized-owner", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{shared}, Finalized: true,
	}}); err != nil {
		t.Fatalf("restore finalized owner: %v", err)
	}
	restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "pending-conflict", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{shared},
	}})
	if err == nil {
		t.Fatal("restored pending statement claimed a finalized owner's candidate")
	}
	if restored != nil {
		t.Fatal("failed Restore returned a live reservation")
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.reserved[key] + guard.committed[key]; got != 0 {
		t.Fatalf("failed Restore leaked capacity debt=%d", got)
	}
	if got := len(guard.liveReservations); got != 0 {
		t.Fatalf("failed Restore leaked %d live reservations", got)
	}
}

func TestPartsPressureGuard_RestoreBatchConflictRollsBackEarlierRecords(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 5
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if _, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "finalized-owner", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{shared}, Finalized: true,
	}}); err != nil {
		t.Fatalf("restore owner: %v", err)
	}

	if restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{
		{StatementID: "applied-first", Table: "db__t", PartitionIDs: []string{"p_a"}},
		{StatementID: "conflicts-second", Table: "db__t", PartitionIDs: []string{"p_a"}, Candidates: []CandidatePart{shared}},
	}); err == nil {
		t.Fatal("RestoreBatch accepted a candidate owned by finalized history")
	} else if restored != nil {
		t.Fatalf("failed RestoreBatch returned handles=%v", restored)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.reserved[key] + guard.committed[key]; got != 0 {
		t.Fatalf("failed batch retained capacity debt=%d", got)
	}
	if got := len(guard.liveReservations); got != 0 {
		t.Fatalf("failed batch retained %d live reservations", got)
	}
	if got := len(guard.candidateClaims[key]); got != 1 {
		t.Fatalf("failed batch changed finalized owner claims=%d, want 1", got)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("claim-conflict restore left inventory available")
	}
	if _, err := guard.ReserveStatement(context.Background(), "after-conflict", "db__t", []string{"p_a"}); err == nil {
		t.Fatal("claim-conflict restore allowed a later reservation")
	} else {
		var unavailable *BackpressureError
		if !errors.As(err, &unavailable) || unavailable.Kind != "unavailable" {
			t.Fatalf("post-conflict reservation error=%v, want unavailable backpressure", err)
		}
	}
}

func TestPartsPressureGuard_RestoreBatchUsesOneSnapshotAndDropsObservedAbsentHistory(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	records := make([]PartsRestoreRecord, 0, 128)
	for idx := range 128 {
		candidate := CandidatePart{
			TableID: "db.t", PartitionID: "p_a", PartName: fmt.Sprintf("a_historical_%d", idx),
		}
		records = append(records, PartsRestoreRecord{
			StatementID: fmt.Sprintf("stmt-%d", idx), Table: "db__t",
			PartitionIDs: []string{"p_a"}, Candidates: []CandidatePart{candidate},
			ObservedCandidates: []CandidatePart{candidate}, Finalized: true,
		})
	}

	if _, err := guard.RestoreBatch(context.Background(), records); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	conn.mu.Lock()
	queries := len(conn.queries)
	conn.mu.Unlock()
	if queries != 1 {
		t.Fatalf("batch restore ran %d inventory queries, want exactly one", queries)
	}
	if got := len(guard.candidateClaims); got != 0 {
		t.Fatalf("observed-then-absent history retained %d candidate claim groups", got)
	}
	if got := len(guard.liveReservations); got != 0 {
		t.Fatalf("observed-then-absent history retained %d reservation handles", got)
	}
	if got := len(guard.committed) + len(guard.reserved); got != 0 {
		t.Fatalf("observed-then-absent history retained capacity debt: committed=%v reserved=%v", guard.committed, guard.reserved)
	}
}

func TestPartsPressureGuard_RestoreBatchFinalizedZeroCandidateIsNoop(t *testing.T) {
	guard, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", "p", 1})
	if restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "stmt-empty-finalized", Table: "db__t", Finalized: true,
	}}); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	} else if len(restored) != 0 {
		t.Fatalf("finalized zero-candidate restore returned handles=%v", restored)
	}
	if got := len(conn.queries); got != 1 {
		t.Fatalf("zero-candidate batch inventory queries=%d, want one", got)
	}
	if len(guard.liveReservations) != 0 || len(guard.candidateClaims) != 0 || len(guard.committed) != 0 || len(guard.reserved) != 0 {
		t.Fatalf("zero-candidate finalized restore retained state: live=%d claims=%d committed=%v reserved=%v",
			len(guard.liveReservations), len(guard.candidateClaims), guard.committed, guard.reserved)
	}
}

func TestPartsPressureGuard_RestoreBatchChargesUnseenFinalizedCandidateUntilVisible(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 2
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_delayed"}
	conn.setInventory(base)

	if _, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "stmt-finalized", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{shared}, Finalized: true,
	}}); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("unseen finalized candidate debt=%d, want 1", got)
	}
	if _, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("unseen finalized candidate did not reserve the last soft slot: %v", err)
	}

	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("visible Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("visible finalized candidate left debt=%d", got)
	}
	if got := len(guard.candidateClaims[key]); got != 1 {
		t.Fatalf("visible finalized candidate claims=%d want 1 until cleanup", got)
	}
	conn.setInventory(base)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("post-cleanup Refresh: %v", err)
	}
	if got := len(guard.candidateClaims[key]); got != 0 {
		t.Fatalf("observed-then-absent finalized candidate retained %d claims", got)
	}
}

func TestPartsPressureGuard_UnrelatedGrowthCannotCoverUnseenFinalizedCandidate(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 3
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	delayed := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_delayed"}
	conn.setInventory(base)
	if _, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "stmt-finalized", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{delayed}, Finalized: true,
	}}); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}

	// Another source's part becomes visible first. Aggregate N+1 growth cannot
	// prove that this statement's exact delayed candidate exists, so its debt
	// must remain and consume the final soft-limit slot.
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_unrelated"})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("unrelated Refresh: %v", err)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.committed[key]; got != 1 {
		t.Fatalf("unrelated growth covered exact delayed debt=%d, want 1", got)
	}
	if _, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("unrelated growth reopened delayed candidate capacity: %v", err)
	}

	conn.setInventory(base,
		fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_unrelated"},
		fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", delayed.PartName},
	)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("exact visibility Refresh: %v", err)
	}
	if got := guard.committed[key]; got != 0 {
		t.Fatalf("exact delayed visibility left debt=%d", got)
	}
}

func TestPartsPressureGuard_ObservationPersistenceFailureFailsClosedAndRetries(t *testing.T) {
	guard, conn := pressureFixture()
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_delayed"}
	conn.setInventory(base)
	if _, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "stmt-finalized", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{shared}, Finalized: true,
	}}); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}

	persistErr := errors.New("journal fsync failed")
	guard.SetCandidateObservedHook(func(context.Context, string, CandidatePart) error { return persistErr })
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if _, err := guard.Refresh(context.Background()); !errors.Is(err, persistErr) {
		t.Fatalf("Refresh error=%v want observation persistence cause", err)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("failed observation persistence left snapshot available")
	}

	var observedStatement string
	guard.SetCandidateObservedHook(func(_ context.Context, statementID string, candidate CandidatePart) error {
		observedStatement = statementID
		if candidate.PartName != shared.PartName {
			t.Fatalf("observed candidate=%+v want %+v", candidate, shared)
		}
		return nil
	})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("retry Refresh: %v", err)
	}
	if observedStatement != "stmt-finalized" {
		t.Fatalf("observed statement=%q want stmt-finalized", observedStatement)
	}
	if _, ok := guard.Snapshot(); !ok {
		t.Fatal("successful observation retry did not recover snapshot")
	}
}

func TestPartsPressureGuard_ObservationHookRequiresStatementAddressedReservation(t *testing.T) {
	guard, _ := pressureFixture()
	guard.SetCandidateObservedHook(func(context.Context, string, CandidatePart) error { return nil })
	reservation, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	defer reservation.Release()
	if err := reservation.Commit(CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_new"}); err == nil {
		t.Fatal("exact candidate bound without statement-addressed reservation")
	}
}

func TestPartsPressureGuard_RestoreBatchObservationFailureRollsBack(t *testing.T) {
	guard, conn := pressureFixture()
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	candidate := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_restored"}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", candidate.PartName})
	persistErr := errors.New("journal fsync failed")
	guard.SetCandidateObservedHook(func(context.Context, string, CandidatePart) error { return persistErr })
	if restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "stmt-finalized", Table: "db__t", PartitionIDs: []string{"p_a"},
		Candidates: []CandidatePart{candidate}, Finalized: true,
	}}); !errors.Is(err, persistErr) {
		t.Fatalf("RestoreBatch error=%v, want persistence cause", err)
	} else if restored != nil {
		t.Fatalf("failed RestoreBatch returned handles=%v", restored)
	}
	key := PartsKey{"hg_unsafe", "db__t", "p_a"}
	if got := guard.reserved[key] + guard.committed[key]; got != 0 {
		t.Fatalf("observation failure retained capacity debt=%d", got)
	}
	if len(guard.liveReservations) != 0 || len(guard.candidateClaims) != 0 || len(guard.pendingObserved) != 0 {
		t.Fatalf("observation failure retained state: live=%d claims=%d pending=%d",
			len(guard.liveReservations), len(guard.candidateClaims), len(guard.pendingObserved))
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("observation failure left inventory available")
	}
}

func TestPartsPressureGuard_RestoreBatchValidationFailureLatchesUnavailableUntilSuccessfulBatch(t *testing.T) {
	guard, conn := pressureFixture()
	conn.setInventory(fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"})
	if restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		Table: "db__t", PartitionIDs: []string{"p_a"},
	}}); err == nil {
		t.Fatal("RestoreBatch accepted a record without statement id")
	} else if restored != nil {
		t.Fatalf("failed RestoreBatch returned handles=%v", restored)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("validation failure left the inventory snapshot available")
	}
	if _, err := guard.ReserveStatement(context.Background(), "blocked", "db__t", []string{"p_a"}); err == nil {
		t.Fatal("ReserveStatement refreshed past a failed durable restore")
	} else {
		var unavailable *BackpressureError
		if !errors.As(err, &unavailable) || unavailable.Kind != "unavailable" {
			t.Fatalf("ReserveStatement error=%v, want unavailable backpressure", err)
		}
	}
	if reservation, err := guard.ReserveStatement(context.Background(), "blocked-zero", "db__t", []string{}); err == nil {
		if reservation != nil {
			reservation.Release()
		}
		t.Fatal("zero-capacity ReserveStatement bypassed a failed durable restore")
	} else {
		var unavailable *BackpressureError
		if !errors.As(err, &unavailable) || unavailable.Kind != "unavailable" {
			t.Fatalf("zero-capacity ReserveStatement error=%v, want unavailable backpressure", err)
		}
	}
	if err := guard.Allow("db__t", "p_a"); err == nil {
		t.Fatal("Allow passed after a failed durable restore")
	} else {
		var unavailable *BackpressureError
		if !errors.As(err, &unavailable) || unavailable.Kind != "unavailable" {
			t.Fatalf("Allow error=%v, want unavailable backpressure", err)
		}
	}
	if restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "unknown-partitions", Table: "db__t", PartitionIDs: nil,
	}}); err == nil {
		t.Fatalf("RestoreBatch accepted unknown partition set with handles=%v", restored)
	}
	if _, ok := guard.Snapshot(); ok {
		t.Fatal("unknown partition set cleared the failed-restore latch")
	}

	restored, err := guard.RestoreBatch(context.Background(), []PartsRestoreRecord{{
		StatementID: "restored", Table: "db__t", PartitionIDs: []string{},
	}})
	if err != nil {
		t.Fatalf("successful RestoreBatch retry: %v", err)
	}
	reservation := restored["restored"]
	if reservation == nil {
		t.Fatal("successful RestoreBatch retry returned no reservation")
	}
	defer reservation.Release()
	if err := guard.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("successful full RestoreBatch did not release unavailable latch: %v", err)
	}
	zero, err := guard.ReserveStatement(context.Background(), "restored-zero", "db__t", []string{})
	if err != nil {
		t.Fatalf("successful full RestoreBatch kept zero-capacity admission unavailable: %v", err)
	}
	zero.Release()
}

func TestPartsPressureGuard_FinalizedCandidateClaimProtectsAlreadyQueuedReservation(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 6
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base)
	owner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("owner Reserve: %v", err)
	}
	queued, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("queued Reserve: %v", err)
	}
	if err := owner.Commit(shared); err != nil {
		t.Fatalf("owner Commit: %v", err)
	}
	owner.Finalize()
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("owner visibility Refresh: %v", err)
	}
	if err := queued.PrepareCleanupProof(context.Background(), []CandidatePart{shared}); err == nil {
		t.Fatal("queued cleanup proof reused a finalized candidate's active part")
	}
	queued.Release()

	// Once a later exact inventory proves the finalized part absent, retaining
	// its old name would only leak an in-memory claim. A new no-op cleanup may
	// bind that absent name without gaining any deletion proof.
	conn.setInventory(base)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("part removal Refresh: %v", err)
	}
	reuse, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("reuse Reserve: %v", err)
	}
	if err := reuse.PrepareCleanupProof(context.Background(), []CandidatePart{shared}); err != nil {
		t.Fatalf("absent finalized candidate name remained claimed: %v", err)
	}
	reuse.Release()
}

func TestPartsPressureGuard_FinalizedClaimSurvivesDelayedExactVisibility(t *testing.T) {
	guard, conn := pressureFixture()
	guard.cfg.SoftPartsPerPartition = 7
	base := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_base"}
	unrelated := fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", "a_unrelated"}
	shared := CandidatePart{TableID: "db.t", PartitionID: "p_a", PartName: "a_shared"}
	conn.setInventory(base)
	owner, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("owner Reserve: %v", err)
	}
	queued, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("queued Reserve: %v", err)
	}
	if err := owner.Commit(shared); err != nil {
		t.Fatalf("owner Commit: %v", err)
	}
	owner.Finalize()

	// Aggregate growth may cover and compact the finalized reservation before
	// ClickHouse exposes its exact candidate name. That must not discard the
	// single-owner identity; the delayed exact part can still appear afterward.
	conn.setInventory(base, unrelated)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("unrelated growth Refresh: %v", err)
	}
	conn.setInventory(base, fakePartInventoryRow{"hg_unsafe", "db__t", "a", "p", shared.PartName})
	if err := queued.PrepareCleanupProof(context.Background(), []CandidatePart{shared}); err == nil {
		t.Fatal("queued cleanup reused a finalized candidate after delayed exact visibility")
	}
	queued.Release()

	conn.setInventory(base)
	if _, err := guard.Refresh(context.Background()); err != nil {
		t.Fatalf("exact removal Refresh: %v", err)
	}
	reuse, err := guard.Reserve(context.Background(), "db__t", []string{"p_a"})
	if err != nil {
		t.Fatalf("reuse Reserve: %v", err)
	}
	if err := reuse.PrepareCleanupProof(context.Background(), []CandidatePart{shared}); err != nil {
		t.Fatalf("removed delayed candidate name remained claimed: %v", err)
	}
	reuse.Release()
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
