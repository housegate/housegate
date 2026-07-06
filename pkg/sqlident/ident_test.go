package sqlident

import "testing"

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "db.table", want: "db.table"},
		{name: "quoted", in: "`db`.`table`", want: "db.table"},
		{name: "spaces", in: " db . `table` ", want: "db.table"},
		{name: "empty segment", in: "db..table", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizePath(tt.in); got != tt.want {
				t.Fatalf("NormalizePath(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSplitLastPath(t *testing.T) {
	db, table := SplitLastPath("cluster.db.table")
	if db != "cluster.db" || table != "table" {
		t.Fatalf("SplitLastPath = (%q,%q), want (cluster.db,table)", db, table)
	}
}

func TestQuotePath(t *testing.T) {
	if got := QuotePath("db.`ta``ble`"); got != "`db`.`ta````ble`" {
		t.Fatalf("QuotePath = %q", got)
	}
}
