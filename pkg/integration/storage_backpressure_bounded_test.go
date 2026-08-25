package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

func TestPartsPressure_HotPathReadStaysBoundedWithManyParts(t *testing.T) {
	const (
		noisyParts = 3000
		partitions = 4
	)
	ctx := context.Background()
	conn := openDirectCH(t)
	suffix := uniqueTable(t)
	unsafeDB := "hg_unsafe_bounded_" + suffix
	safeDB := "hg_safe_bounded_" + suffix
	hot, noisy := "db__hot", "db__noisy"
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + unsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + safeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_throw_insert = 100000, parts_to_delay_insert = 100000, max_parts_in_total = 1000000", unsafeDB, hot),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_throw_insert = 100000, parts_to_delay_insert = 100000, max_parts_in_total = 1000000", safeDB, noisy),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, hot),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", safeDB, noisy),
	} {
		mustExec(t, conn, query)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+unsafeDB+" SYNC")
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+safeDB+" SYNC")
	})

	for i := 0; i < noisyParts; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p%d', %d)", safeDB, noisy, i, i%partitions, i))
	}
	for i := 0; i < 200; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p%d', %d)", unsafeDB, hot, i, i%partitions, i))
	}

	guard := sicore.NewPartsPressureGuard(chMergeConn{conn: conn}, sicore.PartsPressureConfig{
		UnsafeDatabase: unsafeDB, SafeDatabase: safeDB,
		SoftPartsPerPartition: 2400, HardPartsPerPartition: 2950,
		RefreshTimeout: 2 * time.Second, SnapshotTTL: 6 * time.Second,
	})

	countRows := func(query string, args ...any) int {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			t.Fatalf("run pressure query: %v\n%s", err, query)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read pressure query: %v", err)
		}
		return n
	}
	aggQuery, aggArgs := guard.BuildAggregateSnapshotQuery()
	if got := countRows(aggQuery, aggArgs...); got > 4*partitions {
		t.Fatalf("aggregate poll returned %d rows; it must be bounded by tables x partitions", got)
	}
	exactQuery, exactArgs := guard.BuildExactPartsQuery(sicore.PartsScope{
		Database: unsafeDB, Table: hot, Partitions: []string{"p_p0"},
	})
	exactRows := countRows(exactQuery, exactArgs...)
	if exactRows == 0 || exactRows > 200 {
		t.Fatalf("scoped admission read returned %d rows; want only the touched partition's parts", exactRows)
	}

	if _, err := guard.Refresh(ctx); err != nil {
		t.Fatalf("seed refresh: %v", err)
	}
	start := time.Now()
	reservation, err := guard.ReserveStatement(ctx, "0xabc:1:n", hot, []string{"p_p0"})
	if err != nil {
		t.Fatalf("ReserveStatement with %d noisy parts: %v", noisyParts, err)
	}
	elapsed := time.Since(start)
	reservation.Release()
	if elapsed > time.Second {
		t.Fatalf("admission took %s with %d parts in hg_safe; the hot path is not bounded", elapsed, noisyParts)
	}
}
