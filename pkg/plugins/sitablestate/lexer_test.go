package sitablestate

import "testing"

// TestCreateTableCarriesData: the allow-list proves a CREATE TABLE schema-only
// or treats it as data-carrying. Every false-negative probe the Task 5 review
// confirmed as a data-carrying CTAS in ClickHouse 26.8 and the native V2
// engine is a refusal case here.
func TestCreateTableCarriesData(t *testing.T) {
	for sql, want := range map[string]bool{
		// Schema-only: no top-level AS.
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a":                         false,
		"CREATE TABLE db1.t (a UInt32 DEFAULT 1, b String ALIAS toString(a)) ENGINE = Memory": false,
		"CREATE TABLE db1.t COMMENT 'AS SELECT' ENGINE = Memory":                              false,
		"CREATE TABLE db1.t /* AS SELECT */ (a UInt8) ENGINE = Memory":                        false,
		"CREATE TABLE db1.`as select` (a UInt8) ENGINE = Memory":                              false,
		"CREATE TABLE db1.t (a String DEFAULT $$ AS SELECT $$) ENGINE = Memory":               false,
		"CREATE TABLE db1.t (a UInt8) ENGINE = Memory # AS SELECT 1":                          false,
		"CREATE TABLE db1.t (a UInt8) ENGINE = Memory -- AS SELECT 1":                         false,
		"CREATE TABLE db1.t (a UInt8, PROJECTION p (SELECT a ORDER BY a)) ENGINE = Memory":    false,
		// Schema-only: a bare [db.]table schema copy, then only non-data clauses.
		"CREATE TABLE db1.t2 AS db1.t":                 false,
		"CREATE TABLE db1.t2 AS t":                     false,
		"CREATE TABLE db1.t2 AS db1.t ENGINE = Memory": false,
		"CREATE TABLE db1.t2 AS db1.t ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192": false,
		"CREATE TABLE db1.t2 AS db1.t;":   false,
		"CREATE TABLE db1.t2 AS `select`": false,
		// Engine-normalised forwarded bodies (native V2 engine output).
		`CREATE TABLE phys."db1.p" ENGINE=Memory`:                                 false, // EMPTY AS SELECT: the engine drops the body
		`CREATE TABLE phys."db1.p" AS phys."db1.o"`:                               false, // CLONE AS: the engine drops CLONE
		`CREATE TABLE phys."db1.p" AS phys."db1.o" ENGINE=Memory`:                 false,
		`CREATE TABLE phys."db1.p" (a String DEFAULT '(') ENGINE=Memory`:          false,
		`CREATE TABLE phys."db1.p" ENGINE=Memory AS (SELECT 1 AS a) COMMENT ''''`: true,
		`CREATE TABLE phys."db1.p" /* hash */ ENGINE=Memory AS (SELECT 1 AS a)`:   true,
		// Data-carrying bodies.
		"CREATE TABLE db1.t2 AS SELECT * FROM db1.t":                                       true,
		"CREATE TABLE db1.t2 ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.src":       true,
		"CREATE TABLE db1.t (a UInt32) ENGINE = MergeTree ORDER BY a AS (SELECT a FROM x)": true,
		"create table db1.t engine = Memory as with 1 as a select a":                       true,
		// Review probes: confirmed data-carrying, previously missed.
		"CREATE TABLE db1.p ENGINE = Memory AS ((SELECT 1 AS a))":                                     true,
		"CREATE TABLE db1.p ENGINE = Memory AS FROM numbers(3) SELECT number AS a":                    true,
		"CREATE TABLE db1.p ENGINE = MergeTree ORDER BY empty AS SELECT 1 AS empty":                   true,
		"CREATE TABLE db1.p ENGINE = MergeTree PRIMARY KEY empty ORDER BY empty AS SELECT 1 AS empty": true,
		"CREATE TABLE db1.p ENGINE = Memory /* /* */ ( */ AS SELECT 1 AS a":                           true,
		"CREATE TABLE db1.p (a String DEFAULT $$($$) ENGINE = Memory AS SELECT 'x' AS a":              true,
		"CREATE TABLE db1.p ENGINE = Memory COMMENT $$($$ AS SELECT 1 AS a":                           true,
		"CREATE TABLE db1.p ENGINE = Memory COMMENT $$'$$ AS SELECT 1 AS a":                           true,
		"CREATE TABLE db1.p ENGINE = Memory COMMENT $tag$'($tag$ AS SELECT 1 AS a":                    true,
		// Conservative false positive (ruled): EMPTY AS SELECT in unnormalised SQL.
		"CREATE TABLE db1.t2 EMPTY AS SELECT * FROM db1.t": true,
		// Not a bare schema copy.
		"CREATE TABLE db1.p AS numbers(3)":                        true,
		"CREATE TABLE db1.p AS db1.o FINAL":                       true,
		"CREATE TABLE db1.p CLONE AS db1.o":                       true,
		"CREATE TABLE db1.p AS db1.o ENGINE = Memory AS SELECT 1": true,
		"CREATE TABLE db1.p AS":                                   true,
		// Uncertainty fails closed: unterminated spans, stray bytes, unbalanced parens.
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory COMMENT 'x":  true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory /* x":        true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory /* /* */":    true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory COMMENT $$x": true,
		"CREATE TABLE db1.`p (a UInt8) ENGINE = Memory":            true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory COMMENT $x":  true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory #x":          true,
		"CREATE TABLE db1.p (a UInt8) ENGINE = Memory // x":        true,
		"CREATE TABLE db1.p (a UInt8 ENGINE = Memory":              true,
		"CREATE TABLE db1.p a UInt8) ENGINE = Memory":              true,
	} {
		if got := createTableCarriesData(sql); got != want {
			t.Errorf("createTableCarriesData(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestMaterializedViewHeader(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want mvHeader
		ok   bool
	}{
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT a FROM db1.src", mvHeader{"db1", "mv", "db1", "t", true}, true},
		{"CREATE MATERIALIZED VIEW mv TO `t x` AS SELECT a FROM src", mvHeader{"", "mv", "", "t x", true}, true},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a POPULATE AS SELECT a FROM db1.t", mvHeader{"db1", "mv", "", "", false}, true},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.t", mvHeader{"db1", "mv", "", "", false}, true},
		{"CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT a FROM db1.src", mvHeader{"db1", "mv", "db1", "t", true}, true},
		{"CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR TO db1.p AS SELECT a FROM db1.o", mvHeader{"db1", "mv", "db1", "p", true}, true},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY TO DISK 'cold' AS SELECT a FROM db1.src", mvHeader{"db1", "mv", "", "", false}, true},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.src WHERE x = 'POPULATE' AND b TO c", mvHeader{"db1", "mv", "", "", false}, true},
		{"CREATE MATERIALIZED VIEW db1.mv /* TO db1.x */ TO db1.t AS SELECT a FROM db1.src", mvHeader{"db1", "mv", "db1", "t", true}, true},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t (a UInt8) AS SELECT a FROM db1.src", mvHeader{"db1", "mv", "db1", "t", true}, true},
		{"create or replace materialized view if not exists `db 1`.\"m v\" on cluster c TO db1.t AS SELECT 1", mvHeader{"db 1", "m v", "db1", "t", true}, true},
		// A view named TO is the view's name, not a target keyword.
		{"CREATE MATERIALIZED VIEW db1.to ENGINE = Memory AS SELECT 1", mvHeader{"db1", "to", "", "", false}, true},
		// Uncertainty: the header cannot be read, so the view and target are unknown.
		{"CREATE MATERIALIZED VIEW db1.mv /* TO db1.t AS SELECT a FROM db1.src", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.t TO db1.u AS SELECT 1", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW db1.mv TO (SELECT 1) AS SELECT 1", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW db1.mv TO $x AS SELECT 1", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW (x) AS SELECT 1", mvHeader{}, false},
		{"ATTACH MATERIALIZED VIEW db1.mv TO db1.t AS SELECT 1", mvHeader{}, false},
		{"CREATE VIEW db1.v AS SELECT 1", mvHeader{}, false},
		{"CREATE MATERIALIZED VIEW db1. AS SELECT 1", mvHeader{}, false},
	} {
		got, ok := materializedViewHeader(tc.sql)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("materializedViewHeader(%q) = %+v %v, want %+v %v", tc.sql, got, ok, tc.want, tc.ok)
		}
	}
}
