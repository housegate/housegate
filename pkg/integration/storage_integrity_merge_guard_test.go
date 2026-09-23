package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// TestMergeGuard_ReleasesOutdatedPartCleanupWithoutMerging reproduces the
// promotion REPLACE PARTITION on a safe table that an older HouseGate left under
// SYSTEM STOP MERGES. While merges are stopped ClickHouse never deletes the
// replaced parts, and a restart would reactivate them. After the guard runs, the
// pinned setting still prevents merges while the outdated parts are deleted.
func TestMergeGuard_ReleasesOutdatedPartCleanupWithoutMerging(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	suffix := uniqueTable(t)
	safeDB, promoteDB, unsafeDB := "hg_safe_mg_"+suffix, "hg_promote_mg_"+suffix, "hg_unsafe_mg_"+suffix
	const table = "db__t"
	const settings = "SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, old_parts_lifetime = 1"
	for _, db := range []string{safeDB, promoteDB, unsafeDB} {
		mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
		mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), v Int64) ENGINE = MergeTree ORDER BY _hg_row_id %s", db, table, settings))
	}
	t.Cleanup(func() {
		for _, db := range []string{safeDB, promoteDB, unsafeDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	// The pre-fix guard's state.
	mustExec(t, conn, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", safeDB, table))
	mustExec(t, conn, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, table))

	// Two promotions, the way the source performs them.
	for i, v := range []int{7, 8} {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), %d)", unsafeDB, table, i+1, v))
		for _, q := range []string{
			fmt.Sprintf("ALTER TABLE %s.%s ATTACH PARTITION tuple() FROM %s.%s", promoteDB, table, safeDB, table),
			fmt.Sprintf("ALTER TABLE %s.%s ATTACH PARTITION tuple() FROM %s.%s", promoteDB, table, unsafeDB, table),
			fmt.Sprintf("ALTER TABLE %s.%s REPLACE PARTITION tuple() FROM %s.%s", safeDB, table, promoteDB, table),
			fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION tuple()", promoteDB, table),
			fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION tuple()", unsafeDB, table),
		} {
			mustExec(t, conn, q)
		}
	}
	outdated := func() uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.parts WHERE database = ? AND table = ? AND NOT active", safeDB, table).Scan(&n); err != nil {
			t.Fatalf("count outdated parts: %v", err)
		}
		return n
	}
	time.Sleep(3 * time.Second)
	if outdated() == 0 {
		t.Fatal("precondition: under SYSTEM STOP MERGES the replaced safe part must stay outdated on disk")
	}

	guard := sicore.NewMergeGuard(chMergeConn{conn: conn}, []sicore.MergeTable{
		{Database: safeDB, Table: table},
		{Database: unsafeDB, Table: table},
	})
	if err := guard.AssertStopMerges(ctx); err != nil {
		t.Fatalf("guard: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for outdated() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("outdated safe parts were not cleaned after the guard released cleanup (still %d)", outdated())
		}
		time.Sleep(time.Second)
	}

	// The pinned setting still prevents merges: several small parts stay apart.
	for i := 0; i < 4; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), %d)", unsafeDB, table, 100+i, i))
	}
	time.Sleep(10 * time.Second)
	var active, rows uint64
	if err := conn.QueryRow(ctx, "SELECT count(), sum(rows) FROM system.parts WHERE database = ? AND table = ? AND active", unsafeDB, table).Scan(&active, &rows); err != nil {
		t.Fatalf("count unsafe parts: %v", err)
	}
	if active != 4 || rows != 4 {
		t.Fatalf("unsafe parts = %d holding %d rows, want 4 unmerged single-row parts", active, rows)
	}
	var safeRows uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", safeDB, table)).Scan(&safeRows); err != nil {
		t.Fatalf("count safe rows: %v", err)
	}
	if safeRows != 2 {
		t.Fatalf("safe rows = %d, want 2", safeRows)
	}
}

func TestMergeGuard_RefusesRealTableWithoutPinnedSetting(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	db := "hg_safe_mgu_" + uniqueTable(t)
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExec(t, conn, "CREATE TABLE "+db+".db__t (_hg_row_id FixedString(32), v Int64) ENGINE = MergeTree ORDER BY _hg_row_id")
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })
	mustExec(t, conn, "SYSTEM STOP MERGES "+db+".db__t")

	guard := sicore.NewMergeGuard(chMergeConn{conn: conn}, []sicore.MergeTable{{Database: db, Table: "db__t"}})
	if err := guard.AssertStopMerges(ctx); !errors.Is(err, sicore.ErrMergeSettingNotPinned) {
		t.Fatalf("err = %v, want ErrMergeSettingNotPinned", err)
	}
}
