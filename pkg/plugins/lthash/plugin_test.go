package lthashplugin

import "testing"

func TestParseInsertTable(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"INSERT INTO balances VALUES", "balances"},
		{"insert into db1.balances (a, b) VALUES", "db1.balances"},
		{"  INSERT  INTO  `db1`.`balances` FORMAT Native", "db1.balances"},
		{"INSERT INTO `weird table` VALUES", "weird table"},
		{"INSERT INTO balances\n(a,b) VALUES", "balances"},
		{"SELECT * FROM balances", ""},
		{"CREATE TABLE balances (a UInt8) ENGINE=Memory", ""},
		{"INSERT INTO FUNCTION remote('h', db.t) VALUES", ""}, // table functions are out of MVP scope
	}
	for _, c := range cases {
		if got := parseInsertTable(c.sql); got != c.want {
			t.Errorf("parseInsertTable(%q) = %q, want %q", c.sql, got, c.want)
		}
	}
}
