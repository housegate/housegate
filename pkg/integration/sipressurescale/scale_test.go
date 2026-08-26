// Package sipressurescale holds the storage-integrity parts-pressure proof at
// the scale Spec P states, against a real ClickHouse system.parts.
//
// It lives in its own Bazel target rather than in //pkg/integration because
// building the fixture — 10 tables x 12 partitions x 2500 parts = 300,000
// active parts — takes minutes, which would not fit the ordinary integration
// target's timeout budget. It is docker-bound and `manual`-tagged like every
// other integration target, and is listed explicitly in .github/workflows/ci.yml.
package sipressurescale

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/integration/testenv"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

var chEnv *testenv.ClickHouseEnv

func TestMain(m *testing.M) {
	env, cleanup, err := testenv.StartClickHouseForMain()
	if err != nil {
		os.Stderr.WriteString("sipressurescale: " + err.Error() + "\n")
		os.Exit(1)
	}
	chEnv = env
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// chMergeConn adapts clickhouse-go to the narrow sicore.MergeConn port, the
// same way sentio-node's storageintegrityadapter.NewMergeConn does.
type chMergeConn struct{ conn clickhouse.Conn }

func (c chMergeConn) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c chMergeConn) Query(ctx context.Context, query string, args ...any) (sicore.MergeRows, error) {
	return c.conn.Query(ctx, query, args...)
}

// The shape Spec P §1d states verbatim: "10 tables x 12 partitions x 2500 parts
// ~ 300k rows". Every part is one row of the exact part-NAME read.
const (
	scaleTables     = 10
	scalePartitions = 12
	scalePartsEach  = 2500
	scaleSafeParts  = scaleTables * scalePartitions * scalePartsEach
	scaleUnsafeRows = scalePartitions * 10
)

// TestPartsPressure_ExactReadAtSpecScale is Spec P D4's scale acceptance. Before
// the scope split, the single production unbounded exact-name read —
// RestoreBatch, the startup recovery boundary — bound BOTH databases, so it
// returned one row per active part in hg_safe as well as hg_unsafe. hg_safe part
// counts only ever grow in storage-integrity v1 (merges stay stopped), so that
// read's cost grows without bound while nothing consumes its result: Task 6's
// enumeration proved every exact-name consumer keys on UnsafeDatabase.
//
// The assertions are on ROW BUDGET and on which keys the guard installs, not on
// wall clock. Wall clock is logged as evidence but never asserted: it is a
// property of the box's storage, and on fast local disks the pre-split read
// still completes inside a 2s refresh_timeout at this exact shape. What is
// hardware-independent is that the pre-split startup read was ~2500x larger
// than it needed to be and grew with a database it never read a name from.
func TestPartsPressure_ExactReadAtSpecScale(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	unsafeDB := "hg_unsafe_scale_" + suffix
	safeDB := "hg_safe_scale_" + suffix
	const hot = "db__hot"

	partSettings := "SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, " +
		"parts_to_throw_insert = 1000000, parts_to_delay_insert = 1000000, max_parts_in_total = 10000000"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+unsafeDB)
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+safeDB)
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+unsafeDB+" SYNC")
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+safeDB+" SYNC")
	})
	createTable := func(database, table string) {
		mustExec(t, conn, fmt.Sprintf(
			"CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) "+
				"ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) %s",
			database, table, partSettings))
		mustExec(t, conn, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", database, table))
	}
	createTable(unsafeDB, hot)
	for i := 0; i < scaleTables; i++ {
		createTable(safeDB, fmt.Sprintf("db__noisy%d", i))
	}

	// One row per block, one block per part. max_block_size bounds the source
	// block for INSERT ... SELECT and min_insert_block_size_* disable
	// squashing, so numbers(N) with N/scalePartitions blocks lands
	// scalePartsEach parts in each of scalePartitions partitions.
	fill := func(database, table string, rows int) error {
		return conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s.%s SELECT "+
				"reinterpretAsFixedString(cityHash64(number)) || reinterpretAsFixedString(cityHash64(number + 1)) || "+
				"reinterpretAsFixedString(cityHash64(number + 2)) || reinterpretAsFixedString(cityHash64(number + 3)), "+
				"concat('p', toString(number %% %d)), number FROM numbers(%d) "+
				"SETTINGS max_block_size = %d, min_insert_block_size_rows = 0, "+
				"min_insert_block_size_bytes = 0, max_insert_threads = 1",
			database, table, scalePartitions, rows, scalePartitions))
	}
	if err := fill(unsafeDB, hot, scaleUnsafeRows); err != nil {
		t.Fatalf("fill hg_unsafe: %v", err)
	}
	fillStart := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, scaleTables)
	for i := 0; i < scaleTables; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = fill(safeDB, fmt.Sprintf("db__noisy%d", i), scalePartitions*scalePartsEach)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("fill hg_safe table %d: %v", i, err)
		}
	}
	t.Logf("built %d active parts in %s (%s)", scaleSafeParts, safeDB, time.Since(fillStart).Round(time.Millisecond))

	assertPartCount(t, conn, safeDB, scaleSafeParts)
	assertPartCount(t, conn, unsafeDB, scaleUnsafeRows)

	guard := sicore.NewPartsPressureGuard(chMergeConn{conn: conn}, sicore.PartsPressureConfig{
		UnsafeDatabase: unsafeDB, SafeDatabase: safeDB,
		SoftPartsPerPartition: 2400, HardPartsPerPartition: 2950,
		// The production defaults, so the startup read is judged against the
		// budget a real deployment gives it.
		RefreshTimeout: 2 * time.Second, SnapshotTTL: 6 * time.Second,
	})

	// The two query shapes, against real system.parts at the stated scale.
	oldQuery, oldArgs := guard.BuildExactPartsQuery(sicore.PartsScope{
		Database: unsafeDB, IncludeSafeDatabase: true, SafeDatabase: safeDB,
	})
	newQuery, newArgs := guard.BuildExactPartsQuery(sicore.PartsScope{Database: unsafeDB})
	oldRows, oldElapsed := countRows(t, conn, oldQuery, oldArgs...)
	newRows, newElapsed := countRows(t, conn, newQuery, newArgs...)
	t.Logf("pre-split exact read (both databases): %d rows in %s", oldRows, oldElapsed.Round(time.Millisecond))
	t.Logf("post-split exact read (hg_unsafe only): %d rows in %s", newRows, newElapsed.Round(time.Millisecond))
	if oldRows < scaleSafeParts {
		t.Fatalf("fixture is not at the stated scale: the both-database exact read returned %d rows, want >= %d", oldRows, scaleSafeParts)
	}
	if newRows != scaleUnsafeRows {
		t.Fatalf("the unsafe-only exact read returned %d rows, want %d; it must not scale with hg_safe", newRows, scaleUnsafeRows)
	}

	// The production caller of the exact full read is the startup recovery
	// boundary. At the stated scale it must complete inside refresh_timeout,
	// must not latch restoreBlocked, and must install no safe-database key.
	restoreStart := time.Now()
	if _, err := guard.RestoreBatch(ctx, nil); err != nil {
		t.Fatalf("startup RestoreBatch with %d parts in hg_safe: %v", scaleSafeParts, err)
	}
	t.Logf("startup RestoreBatch completed in %s (refresh_timeout=2s)", time.Since(restoreStart).Round(time.Millisecond))
	if err := guard.Allow(hot, "p_p0"); err != nil {
		t.Fatalf("a completed startup read must leave admissions open, not a latched restoreBlocked: %v", err)
	}

	snapshot, err := guard.Refresh(ctx)
	if err != nil {
		t.Fatalf("post-restore exact refresh at scale: %v", err)
	}
	if _, ok := guard.Snapshot(); !ok {
		t.Fatal("an hg_unsafe-only full pass must leave a usable snapshot")
	}
	for key := range snapshot {
		if key.Database == safeDB {
			t.Fatalf("the guard's exact read installed a safe-database key %+v at %d parts", key, scaleSafeParts)
		}
	}
	if len(snapshot) != scalePartitions {
		t.Fatalf("the exact read produced %d keys; hg_unsafe has one table x %d partitions", len(snapshot), scalePartitions)
	}

	// The bounded aggregate still covers both databases: that is what feeds
	// storage_integrity_safe_parts, and it is bounded by tables x partitions.
	counts, err := guard.RefreshCounts(ctx)
	if err != nil {
		t.Fatalf("RefreshCounts at scale: %v", err)
	}
	safeCounted := 0
	for key, n := range counts {
		if key.Database == safeDB {
			safeCounted += n
		}
	}
	if safeCounted != scaleSafeParts {
		t.Fatalf("the bounded aggregate must still supply the safe gauge: counted %d, want %d", safeCounted, scaleSafeParts)
	}
	if len(counts) > (scaleTables+1)*scalePartitions {
		t.Fatalf("the aggregate returned %d keys; it must stay bounded by tables x partitions", len(counts))
	}
}

func openDirectCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chEnv.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func assertPartCount(t *testing.T, conn clickhouse.Conn, database string, want int) {
	t.Helper()
	var got uint64
	row := conn.QueryRow(context.Background(),
		"SELECT count() FROM system.parts WHERE database = ? AND active", database)
	if err := row.Scan(&got); err != nil {
		t.Fatalf("count parts in %s: %v", database, err)
	}
	if int(got) != want {
		t.Fatalf("%s holds %d active parts, want %d", database, got, want)
	}
}

func countRows(t *testing.T, conn clickhouse.Conn, query string, args ...any) (int, time.Duration) {
	t.Helper()
	start := time.Now()
	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("run pressure query: %v\n%s", err, query)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var database, table, partition, partitionKey, partName string
		if err := rows.Scan(&database, &table, &partition, &partitionKey, &partName); err != nil {
			t.Fatalf("scan pressure query: %v", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read pressure query: %v", err)
	}
	return n, time.Since(start)
}
