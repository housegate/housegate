package sitablestate

import "testing"

func TestCreateTableCarriesData(t *testing.T) {
	for sql, want := range map[string]bool{
		"CREATE TABLE db1.t2 AS SELECT * FROM db1.t":                                          true,
		"CREATE TABLE db1.t2 ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.src":          true,
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a AS (SELECT a FROM x)":    true,
		"create table db1.t engine = Memory as with 1 as a select a":                          true,
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a":                         false,
		"CREATE TABLE db1.t2 AS db1.t":                                                        false,
		"CREATE TABLE db1.t2 EMPTY AS SELECT * FROM db1.t":                                    false,
		"CREATE TABLE db1.t (a UInt32 DEFAULT 1, b String ALIAS toString(a)) ENGINE = Memory": false,
		"CREATE TABLE db1.t COMMENT 'AS SELECT' ENGINE = Memory":                              false,
		"CREATE TABLE db1.t /* AS SELECT */ (a UInt8) ENGINE = Memory":                        false,
		"CREATE TABLE db1.`as select` (a UInt8) ENGINE = Memory":                              false,
	} {
		if got := createTableCarriesData(sql); got != want {
			t.Errorf("createTableCarriesData(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestMaterializedViewHeader(t *testing.T) {
	for _, tc := range []struct {
		sql                 string
		populate, hasTo     bool
		toDatabase, toTable string
	}{
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.src", false, true, "db1", "t"},
		{"CREATE MATERIALIZED VIEW mv TO `t x` AS SELECT a FROM src", false, true, "", "t x"},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a POPULATE AS SELECT a FROM db1.t", true, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.t", false, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT a FROM db1.src", false, true, "db1", "t"},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY TO DISK 'cold' AS SELECT a FROM db1.src", false, false, "", ""},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.src WHERE x = 'POPULATE' AND b TO c", false, false, "", ""},
	} {
		populate, db, table, hasTo := materializedViewHeader(tc.sql)
		if populate != tc.populate || hasTo != tc.hasTo || db != tc.toDatabase || table != tc.toTable {
			t.Errorf("materializedViewHeader(%q) = %v %q %q %v, want %v %q %q %v", tc.sql, populate, db, table, hasTo, tc.populate, tc.toDatabase, tc.toTable, tc.hasTo)
		}
	}
}
