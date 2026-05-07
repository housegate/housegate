package forward

import "testing"

func TestMatchUse(t *testing.T) {
	cases := []struct {
		sql    string
		wantDB string
		wantOK bool
	}{
		{"USE tenant1", "tenant1", true},
		{"use tenant1", "tenant1", true},
		{"  USE  tenant1 ", "tenant1", true},
		{"USE tenant1;", "tenant1", true},
		{"USE `tenant-1`", "tenant-1", true},
		{"USE \"tenant1\"", "tenant1", true},
		{"USE tenant1 SETTINGS x=1", "", false},
		{"SELECT 1", "", false},
		{"-- USE comment", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		gotDB, gotOK := matchUse(c.sql)
		if gotDB != c.wantDB || gotOK != c.wantOK {
			t.Errorf("matchUse(%q) = (%q, %v) want (%q, %v)",
				c.sql, gotDB, gotOK, c.wantDB, c.wantOK)
		}
	}
}
