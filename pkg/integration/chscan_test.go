package integration

import (
	"context"
	"encoding/hex"
	"fmt"
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
