package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func tablesConn() *fakeMergeConn {
	return &fakeMergeConn{engines: map[MergeTable]string{
		{Database: "hg_safe", Table: "db1__ok"}:         pinnedEngine,
		{Database: "hg_unsafe", Table: "db1__ok"}:       pinnedEngine,
		{Database: "hg_safe", Table: "db1__unpinned"}:   "MergeTree ORDER BY _hg_row_id",
		{Database: "hg_unsafe", Table: "db1__unpinned"}: pinnedEngine,
		{Database: "hg_safe", Table: "db1__pending"}:    pinnedEngine,
	}}
}

func TestMergeGuardAssertTablesReportsPerTable(t *testing.T) {
	conn := tablesConn()
	report, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok", "db1.unpinned", "db1.missing"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Tables["db1.ok"] != nil {
		t.Fatalf("db1.ok = %v, want healthy", report.Tables["db1.ok"])
	}
	if !errors.Is(report.Tables["db1.unpinned"], ErrMergeSettingNotPinned) {
		t.Fatalf("db1.unpinned = %v, want ErrMergeSettingNotPinned", report.Tables["db1.unpinned"])
	}
	if !errors.Is(report.Tables["db1.missing"], ErrMergeGuardTableMissing) {
		t.Fatalf("db1.missing = %v, want ErrMergeGuardTableMissing", report.Tables["db1.missing"])
	}
	if len(report.Tables) != 3 {
		t.Fatalf("report has %d table entries, want exactly the 3 requested", len(report.Tables))
	}
	// A Pending table's present hg_* table is checked, but absence of its
	// sibling is not an error.
	if err, ok := report.Other[MergeTable{Database: "hg_safe", Table: "db1__pending"}]; !ok || err != nil {
		t.Fatalf("Other[hg_safe.db1__pending] = %v (present %v), want checked and healthy", err, ok)
	}
	for _, stmt := range conn.execs {
		if strings.Contains(stmt, "db1__unpinned`") && strings.Contains(stmt, "hg_safe") {
			t.Fatalf("an unpinned table must never be started: %q", stmt)
		}
	}
	if !strings.Contains(strings.Join(conn.execs, "\n"), "SYSTEM START MERGES `hg_safe`.`db1__pending`") {
		t.Fatalf("a present Pending table must be started too: %v", conn.execs)
	}
}

func TestMergeGuardAssertTablesAttributesAMergeToItsTable(t *testing.T) {
	conn := tablesConn()
	conn.merging = []MergeTable{{Database: "hg_unsafe", Table: "db1__ok"}}
	report, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok"})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(report.Tables["db1.ok"], ErrNativeMergesEnabled) {
		t.Fatalf("db1.ok = %v, want ErrNativeMergesEnabled", report.Tables["db1.ok"])
	}
}

func TestMergeGuardAssertTablesEnumerationFailureIsGlobal(t *testing.T) {
	conn := tablesConn()
	conn.settingsErr = errors.New("clickhouse down")
	_, err := NewMergeGuard(conn, nil).AssertTables(context.Background(), []string{"db1.ok"})
	if err == nil || !strings.Contains(err.Error(), "clickhouse down") {
		t.Fatalf("err = %v, want the enumeration failure", err)
	}
}
