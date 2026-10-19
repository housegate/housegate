package chexec

import (
	"context"
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestScanAllPartsScansEveryActivePartWithScanPartsRefusals pins, without
// ClickHouse, what ScanAllParts sends and refuses (spec 2026-10-14 §8.5 step
// 4): ScanParts's query without the _part filter, so every active part of the
// table is read; ScanParts's refusals, before any query; and an empty answer
// is an empty slice, not an error (the rename-race signal). ScanParts's own
// query is unchanged.
func TestScanAllPartsScansEveryActivePartWithScanPartsRefusals(t *testing.T) {
	ctx := context.Background()
	schema := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}

	conn := &recordingConn{}
	got, err := ScanAllParts(ctx, conn, "hg_rstage.s_1", schema)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("ScanAllParts(empty table) = %#v, %v; want an empty slice and no error", got, err)
	}
	if _, err := ScanParts(ctx, conn, "hg_rstage.s_1", schema, []string{"all_1_1_0", "all_2_2_0"}); err == nil {
		t.Fatal("ScanParts of parts an empty table lacks must fail")
	}
	want := []string{
		"SELECT _part, _hg_row_id, `p`, `v` FROM `hg_rstage`.`s_1`",
		"SELECT _part, _hg_row_id, `p`, `v` FROM `hg_rstage`.`s_1` WHERE _part IN (?, ?)",
	}
	if !slices.Equal(conn.queries, want) {
		t.Fatalf("queries = %q, want %q", conn.queries, want)
	}

	for _, tc := range []struct {
		table  string
		schema payloadexec.TableSchema
		want   string
	}{
		{"s_1", schema, `chexec: qualifiedTable must be db.table, got "s_1"`},
		{"hg_rstage.s_1", payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Array(UInt64)"}}}, `chexec: unsupported column type "Array(UInt64)" for part scan (column "v")`},
	} {
		if got, err := ScanAllParts(ctx, panicConn{}, tc.table, tc.schema); err == nil || err.Error() != tc.want || got != nil {
			t.Errorf("ScanAllParts(%q) = %v, %v; want nil, %q", tc.table, got, err, tc.want)
		}
		if _, err := ScanParts(ctx, panicConn{}, tc.table, tc.schema, []string{"all_1_1_0"}); err == nil || err.Error() != tc.want {
			t.Errorf("ScanParts(%q) = %v, want the same refusal %q", tc.table, err, tc.want)
		}
	}
}
