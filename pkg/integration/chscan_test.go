package integration

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestChexecScanParts_MatchesExecutorRowHashing(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	db := "hg_scan_test"
	table := uniqueTable(t)
	qualified := db + "." + table

	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExec(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified))
	mustExec(t, conn, fmt.Sprintf(`
		CREATE TABLE %s (
			_hg_row_id FixedString(32),
			p String,
			v UInt64
		) ENGINE = MergeTree
		PARTITION BY p
		ORDER BY tuple()`, qualified))
	mustExec(t, conn, "SYSTEM STOP MERGES "+qualified)
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
	})

	sch := payloadexec.TableSchema{
		TableID:     "db.scan_t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
	rid0 := payloadexec.RowID("net-1", "db.scan_t", "acct/1/n", 0)
	rid1 := payloadexec.RowID("net-1", "db.scan_t", "acct/1/n", 1)
	insertScanRows(t, conn, qualified, scanRow{rid: rid0, p: "p0", v: 1}, scanRow{rid: rid1, p: "p0", v: 2})

	want := lthash.New()
	h0, err := payloadexec.RowElementHash(sch, rid0, []any{"p0", uint64(1)})
	if err != nil {
		t.Fatalf("row 0 hash: %v", err)
	}
	h1, err := payloadexec.RowElementHash(sch, rid1, []any{"p0", uint64(2)})
	if err != nil {
		t.Fatalf("row 1 hash: %v", err)
	}
	want.AddHash(h0)
	want.AddHash(h1)

	partNames := activePartNames(t, ctx, conn, db, table)
	if len(partNames) != 1 {
		t.Fatalf("active parts = %d, want 1 (%v)", len(partNames), partNames)
	}
	got, err := chexec.ScanParts(ctx, conn, qualified, sch, []string{partNames[0]})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("scan results = %d, want 1", len(got))
	}
	if got[0].RowCount != 2 || got[0].RowLtHash != "0x"+hex.EncodeToString(want.Bytes()) {
		t.Fatalf("scan mismatch: %+v", got[0])
	}

	if _, err := chexec.ScanParts(ctx, conn, qualified, sch, []string{"nope_0_0_0"}); err == nil {
		t.Fatal("missing part must error")
	}
}

type scanRow struct {
	rid []byte
	p   string
	v   uint64
}

func insertScanRows(t *testing.T, conn clickhouse.Conn, table string, rows ...scanRow) {
	t.Helper()
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+table)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	for _, row := range rows {
		if err := batch.Append(row.rid, row.p, row.v); err != nil {
			t.Fatalf("batch.Append: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send: %v", err)
	}
}

func activePartNames(t *testing.T, ctx context.Context, conn clickhouse.Conn, database, table string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT name FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", database, table)
	if err != nil {
		t.Fatalf("query active parts: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan part name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("active part rows: %v", err)
	}
	return names
}

// TestChexecScanAllParts_MatchesScanPartsOverEveryActivePart pins the full
// partition scan of spec 2026-10-14 §8.5 step 4 (a restore scratch is scanned
// without knowing its part names): ScanAllParts equals ScanParts over the
// names of every active part, sorted by part name, and after a merge it reads
// only the merged part, whose rows still sum to the same accumulator.
func TestChexecScanAllParts_MatchesScanPartsOverEveryActivePart(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	db := "hg_scan_all_test"
	table := uniqueTable(t)
	qualified := db + "." + table

	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
	})
	mustExec(t, conn, fmt.Sprintf(`
		CREATE TABLE %s (
			_hg_row_id FixedString(32),
			p String,
			v UInt64
		) ENGINE = MergeTree
		PARTITION BY p
		ORDER BY tuple()`, qualified))
	mustExec(t, conn, "SYSTEM STOP MERGES "+qualified)

	sch := payloadexec.TableSchema{
		TableID:     "db.scan_all_t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
	rid := func(ordinal uint64) []byte { return payloadexec.RowID("net-1", "db.scan_all_t", "acct/1/n", ordinal) }
	insertScanRows(t, conn, qualified, scanRow{rid: rid(0), p: "p0", v: 1}, scanRow{rid: rid(1), p: "p0", v: 2})
	insertScanRows(t, conn, qualified, scanRow{rid: rid(2), p: "p0", v: 3})
	insertScanRows(t, conn, qualified, scanRow{rid: rid(3), p: "p1", v: 4})

	scanAll := func(wantParts int) []chexec.PartScanResult {
		t.Helper()
		names := activePartNames(t, ctx, conn, db, table)
		if len(names) != wantParts {
			t.Fatalf("active parts = %v, want %d", names, wantParts)
		}
		all, err := chexec.ScanAllParts(ctx, conn, qualified, sch)
		if err != nil {
			t.Fatalf("ScanAllParts: %v", err)
		}
		named, err := chexec.ScanParts(ctx, conn, qualified, sch, names)
		if err != nil {
			t.Fatalf("ScanParts(%v): %v", names, err)
		}
		if !reflect.DeepEqual(all, named) {
			t.Fatalf("ScanAllParts = %+v, ScanParts over every active part = %+v", all, named)
		}
		got := make([]string, len(all))
		var rows uint64
		for i, part := range all {
			got[i] = part.PartName
			rows += part.RowCount
		}
		if !slices.Equal(got, names) || !slices.IsSorted(got) || rows != 4 {
			t.Fatalf("ScanAllParts parts = %v (%d rows), want the active parts %v sorted by name with 4 rows", got, rows, names)
		}
		return all
	}
	sum := func(parts []chexec.PartScanResult) string {
		t.Helper()
		hashes := make([]string, len(parts))
		for i, part := range parts {
			hashes[i] = part.RowLtHash
		}
		total, err := payloadexec.SumPartRowLtHashes(hashes)
		if err != nil {
			t.Fatalf("SumPartRowLtHashes: %v", err)
		}
		return total
	}

	before := sum(scanAll(3))

	// A merge retires both parts of p0; only the merged part stays active.
	mustExec(t, conn, "SYSTEM START MERGES "+qualified)
	mustExec(t, conn, "OPTIMIZE TABLE "+qualified+" PARTITION 'p0' FINAL")
	mustExec(t, conn, "SYSTEM STOP MERGES "+qualified)
	if after := sum(scanAll(2)); after != before {
		t.Fatalf("the rows' accumulator changed across a merge: %.20s… != %.20s…", after, before)
	}
}

// TestChexecScanAllParts_EmptyTableIsEmpty: a table without rows, never
// written or emptied, scans to an empty slice and no error, which is how a
// caller sees a rename race (spec 2026-10-14 §8.5 step 4).
func TestChexecScanAllParts_EmptyTableIsEmpty(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	db := "hg_scan_all_test"
	table := uniqueTable(t)
	qualified := db + "." + table

	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
	})
	mustExec(t, conn, "CREATE TABLE "+qualified+" (_hg_row_id FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY tuple()")
	sch := payloadexec.TableSchema{TableID: "db.scan_all_empty", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}

	got, err := chexec.ScanAllParts(ctx, conn, qualified, sch)
	if err != nil || len(got) != 0 {
		t.Fatalf("ScanAllParts(new table) = %+v, %v; want no part and no error", got, err)
	}
	rid := payloadexec.RowID("net-1", "db.scan_all_empty", "acct/1/n", 0)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+qualified)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	if err := batch.Append(rid, uint64(1)); err != nil {
		t.Fatalf("batch.Append: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send: %v", err)
	}
	if got, err := chexec.ScanAllParts(ctx, conn, qualified, sch); err != nil || len(got) != 1 || got[0].RowCount != 1 {
		t.Fatalf("ScanAllParts(one row) = %+v, %v; want one part of one row", got, err)
	}
	mustExec(t, conn, "TRUNCATE TABLE "+qualified)
	if got, err := chexec.ScanAllParts(ctx, conn, qualified, sch); err != nil || len(got) != 0 {
		t.Fatalf("ScanAllParts(truncated table) = %+v, %v; want no part and no error", got, err)
	}
}
