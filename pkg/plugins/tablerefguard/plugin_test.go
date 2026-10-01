package tablerefguard

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

var reserved = []string{"hg_safe", "hg_unsafe", "hg_promote"}

func TestCheck(t *testing.T) {
	for _, tc := range []struct{ name, sql, phys, want string }{
		// G1 reserved names: every surface but comments.
		{"reserved qualifier", "SELECT * FROM hg_safe.db1__t", "phys", RuleReservedName},
		{"reserved promote", "SELECT * FROM hg_promote.db1__t", "phys", RuleReservedName},
		{"reserved, other case", "SELECT * FROM HG_Safe.db1__t", "phys", RuleReservedName},
		{"reserved in a literal", "SELECT * FROM db1.o WHERE s = 'hg_unsafe'", "phys", RuleReservedName},
		{"reserved in a carrier literal", "SELECT * FROM merge('hg_unsafe', 'x')", "phys", RuleReservedName},
		{"reserved in a heredoc", "SELECT $$hg_safe$$", "phys", RuleReservedName},
		{"reserved in a quoted identifier", "SELECT * FROM `hg_safe`.`db1__t`", "phys", RuleReservedName},
		{"reserved in a comment", "SELECT 1 /* hg_safe */", "phys", ""},
		{"reserved in a line comment", "SELECT 1 -- hg_safe\n", "phys", ""},
		{"reserved as part of a longer word", "SELECT hg_safe_count FROM db1.o", "phys", ""},
		// G2 physical database.
		{"phys qualifier", "SELECT * FROM phys.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys backtick qualifier", "SELECT * FROM `phys`.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys double-quote qualifier", `SELECT * FROM "phys".x`, "phys", RulePhysicalDatabase},
		{"phys qualifier across a comment", "SELECT * FROM phys/* c */.x", "phys", RulePhysicalDatabase},
		{"phys qualifier across a spaced comment", "SELECT * FROM phys /* c */ .x", "phys", RulePhysicalDatabase},
		{"phys qualifier across a line comment", "SELECT * FROM phys -- c\n.x", "phys", RulePhysicalDatabase},
		{"phys IN operand", "SELECT * FROM db1.o WHERE a IN phys.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys as a CREATE ... AS source", "CREATE TABLE db1.n AS phys.`db2.x`", "phys", RulePhysicalDatabase},
		{"phys tuple element is over-reported as a qualifier", "SELECT phys.1 FROM db1.o", "phys", RulePhysicalDatabase},
		{"use phys", "USE phys", "phys", RulePhysicalDatabase},
		{"use quoted phys", "USE `phys`", "phys", RulePhysicalDatabase},
		{"use double-quoted phys", `use "phys"`, "phys", RulePhysicalDatabase},
		{"use a tenant database", "USE db1", "phys", ""},
		{"show tables from phys", "SHOW TABLES FROM phys", "phys", RulePhysicalDatabase},
		{"show columns in phys", "SHOW COLUMNS FROM t IN phys", "phys", RulePhysicalDatabase},
		{"show columns from phys", "show columns from t from `phys`", "phys", RulePhysicalDatabase},
		{"show tables from a tenant database", "SHOW TABLES FROM db1", "phys", ""},
		{"lookup literal", "SELECT joinGet('phys.`db2.x`', 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup literal, space before the parenthesis", "SELECT dictGet ('phys.d', 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup literal, comment before the parenthesis", "SELECT dictGetString/* c */('phys.d', 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup heredoc", "SELECT joinGetOrNull($$phys.j$$, 'v', 1)", "phys", RulePhysicalDatabase},
		{"lookup database argument", "SELECT hasColumnInTable('phys', 't', 'c')", "phys", RulePhysicalDatabase},
		{"lookup bare name argument", "SELECT dictHas('db1.d', phys)", "phys", RulePhysicalDatabase},
		{"lookup nested call argument", "SELECT dictIsIn('db1.d', toUInt64(phys), 1)", "phys", RulePhysicalDatabase},
		{"lookup on a tenant table", "SELECT hasColumnInTable('db1', 'o', 'a')", "phys", ""},
		{"phys after a lookup's argument list", "SELECT joinGet('db1.j', 'v', 1), 'phys'", "phys", ""},
		{"phys as a column", "SELECT phys FROM db1.o", "phys", ""},
		{"phys as an unqualified table", "SELECT * FROM phys", "phys", ""},
		{"phys in an ordinary literal", "SELECT * FROM db1.o WHERE name = 'phys'", "phys", ""},
		{"phys-prefixed ordinary literal", "SELECT * FROM db1.o WHERE name = 'phys.x'", "phys", ""},
		{"phys in the driver's metadata read", "SELECT name FROM system.tables WHERE database = 'phys' AND name LIKE 'db1.%'", "phys", ""},
		{"dotted quoted name is not a qualifier", "SELECT * FROM db1.`phys.x`", "phys", ""},
		{"dotted double-quoted name is not a qualifier", `SELECT * FROM db1."phys.x"`, "phys", ""},
		{"database names are case-sensitive", "SELECT * FROM PHYS.x", "phys", ""},
		{"show databases like phys", "SHOW DATABASES LIKE 'phys%'", "phys", ""},
		{"physical database named default, DEFAULT keyword", "CREATE TABLE db1.n (a UInt8 DEFAULT 0) ENGINE = Memory", "default", ""},
		{"physical database named default, lower-case default keyword", "CREATE TABLE db1.n (a UInt8 default 0) ENGINE = Memory", "default", ""},
		{"physical database named default, qualifier", "SELECT * FROM default.x", "default", RulePhysicalDatabase},
		{"no physical database configured", "SELECT * FROM phys.x", "", ""},
		// G3 the §5 T5 refused table functions.
		{"merge on an own table", "SELECT * FROM merge('db1', 'o')", "phys", RuleCarrierCallable},
		{"merge, upper case", "SELECT * FROM MERGE('db1', 'o')", "phys", RuleCarrierCallable},
		{"merge across a comment", "SELECT * FROM merge /* c */ ('db1', 'o')", "phys", RuleCarrierCallable},
		{"merge as a quoted name", "SELECT * FROM `merge`('db1', 'o')", "phys", RuleCarrierCallable},
		{"merge naming phys", "SELECT * FROM merge('phys', 'x')", "phys", RuleCarrierCallable},
		{"remote", "SELECT * FROM remote('h', db1.o)", "phys", RuleCarrierCallable},
		{"remoteSecure", "SELECT * FROM remoteSecure('h', db1.o)", "phys", RuleCarrierCallable},
		{"mergeTreeIndex", "SELECT * FROM mergeTreeIndex('db1', 'o')", "phys", RuleCarrierCallable},
		{"mergeTreeProjection", "SELECT * FROM mergeTreeProjection('db1', 'o', 'p')", "phys", RuleCarrierCallable},
		{"executable", "SELECT * FROM executable('x.sh', CSV, 'a UInt8')", "phys", RuleCarrierCallable},
		{"fuzzJSON", "SELECT * FROM fuzzJSON('db1.o')", "phys", RuleCarrierCallable},
		{"heredoc argument", "SELECT * FROM merge($$phys$$, 'x')", "phys", RuleCarrierCallable},
		{"numbers", "SELECT * FROM numbers(10)", "phys", ""},
		{"url is a non-goal", "SELECT * FROM url('http://x', CSV)", "phys", ""},
		{"sqlite is a non-goal", "SELECT * FROM sqlite('x.db', 't')", "phys", ""},
		{"a column named merge", "SELECT merge FROM db1.o", "phys", ""},
		{"a merge combinator", "SELECT uniqMerge(s) FROM db1.o", "phys", ""},
		{"MergeTree engine with parentheses", "CREATE TABLE db1.n (a UInt8) ENGINE = MergeTree() ORDER BY a", "phys", ""},
		{"MergeTree engine without parentheses", "CREATE TABLE db1.n (a UInt8) ENGINE = MergeTree ORDER BY a", "phys", ""},
		{"ReplacingMergeTree engine", "CREATE TABLE db1.n (a UInt8, v UInt8) ENGINE = ReplacingMergeTree(v) ORDER BY a", "phys", ""},
		// G4 identifier placeholders, position-blind by design.
		{"identifier placeholder", "SELECT * FROM {p:Identifier}", "phys", RuleIdentifierPlaceholder},
		{"identifier placeholder with spaces", "SELECT * FROM { p : identifier }", "phys", RuleIdentifierPlaceholder},
		{"column identifier placeholder (deliberate)", "SELECT {c:Identifier} FROM db1.o", "phys", RuleIdentifierPlaceholder},
		{"value placeholder", "SELECT * FROM db1.o WHERE a = {v:UInt64}", "phys", ""},
		{"placeholder text in a literal", "SELECT * FROM db1.o WHERE s = '{p:Identifier}'", "phys", ""},
		// G5 escapes.
		{"escaped backtick identifier", "SELECT * FROM db1.`\\x74`", "phys", RuleEscapedIdentifier},
		{"escaped double-quoted identifier", `SELECT * FROM db1."\x74"`, "phys", RuleEscapedIdentifier},
		{"backslash in a string literal", `SELECT * FROM db1.o WHERE s = 'a\nb'`, "phys", ""},
		{"escaped quote in a string literal", `SELECT * FROM db1.o WHERE s = 'it\'s'`, "phys", ""},
		{"backslash in a heredoc", `SELECT $$a\b$$`, "phys", ""},
		// Spec N D1 spans and the scanner's other refusals, all mapped to the
		// scan rule (decision D7).
		{"stray dollar", "SELECT $ FROM db1.o", "phys", RuleScan},
		{"dollar after an identifier byte", "SELECT 1 AS x$$, * FROM phys.t AS y$$", "phys", RuleScan},
		{"unterminated literal", "SELECT 'x FROM db1.o", "phys", RuleScan},
		{"unterminated block comment", "SELECT 1 /* x", "phys", RuleScan},
		{"unterminated heredoc", "SELECT $$x", "phys", RuleScan},
		{"unterminated quoted identifier", "SELECT * FROM `x", "phys", RuleScan},
		{"bare hash", "SELECT 1 #x\n", "phys", RuleScan},
		{"Unicode whitespace before a dot", "SELECT * FROM phys\u00a0.x", "phys", RuleScan},
		{"non-ASCII bare word", "SELECT 1 AS phys\u00e9", "phys", RuleScan},
		// ClickHouse accepts a leading UTF-8 BOM; the scanner refuses every
		// non-ASCII byte outside a span. A known, safe false refusal.
		{"leading UTF-8 BOM (known false refusal)", "\ufeffSELECT 1", "phys", RuleScan},
		{"non-ASCII inside a quoted identifier", "SELECT 1 AS `phys\u00e9`", "phys", ""},
		{"empty statement", "", "phys", ""},
		// Fix round 1, I1: string literals are compared as ClickHouse decodes
		// them (measured on 26.8.1: joinGet('ph\x79s.j', ...) reads phys.j).
		{"escaped lookup literal", `SELECT joinGet('ph\x79s.j', 'v', 1)`, "phys", RulePhysicalDatabase},
		{"escaped lookup database argument", `SELECT hasColumnInTable('ph\x79s', 't', 'c')`, "phys", RulePhysicalDatabase},
		{"\\N-split lookup literal", `SELECT dictGet('ph\Nys.d', 'v', 1)`, "phys", RulePhysicalDatabase},
		{"escaped reserved name", `SELECT dictGet('hg\x5fsafe.d', 'v', 1)`, "phys", RuleReservedName},
		{"\\N-split reserved name", `SELECT * FROM db1.o WHERE s = 'hg_\Nsafe'`, "phys", RuleReservedName},
		{"kept backslash is not a reserved name", `SELECT * FROM db1.o WHERE s LIKE 'hg\_safe'`, "phys", ""},
		{"heredoc lookup is not decoded", `SELECT joinGet($$ph\x79s.j$$, 'v', 1)`, "phys", ""},
		{"undecodable \\x escape", `SELECT * FROM db1.o WHERE s = '\x4g'`, "phys", RuleScan},
		// Fix round 1, I2: a table named after a carrier or lookup, followed
		// by its column list, is not a call (measured on 26.8.1: none of
		// these positions accepts a table function).
		{"qualified table named cluster with a column list", "INSERT INTO db1.cluster (a) VALUES (1)", "phys", ""},
		{"unqualified table named cluster with a column list", "INSERT INTO cluster (a) VALUES (1)", "phys", ""},
		{"INSERT INTO TABLE a table named Loop", "INSERT INTO TABLE Loop (a) VALUES (1)", "phys", ""},
		{"CREATE TABLE named Merge", "CREATE TABLE db1.`Merge` (a UInt8) ENGINE = MergeTree ORDER BY a", "phys", ""},
		{"CREATE TABLE IF NOT EXISTS named remote", "CREATE TABLE IF NOT EXISTS remote (a UInt8) ENGINE = Memory", "phys", ""},
		{"CREATE OR REPLACE TABLE named dictionary", "CREATE OR REPLACE TABLE dictionary (a UInt8) ENGINE = Memory", "phys", ""},
		{"CREATE TEMPORARY TABLE named executable", "CREATE TEMPORARY TABLE executable (a UInt8)", "phys", ""},
		{"CREATE VIEW named mysql with columns", "CREATE VIEW mysql (a UInt8) AS SELECT 1 AS a", "phys", ""},
		{"materialized view TO a table named cluster", "CREATE MATERIALIZED VIEW db1.mv TO db1.cluster (a UInt8) AS SELECT a FROM db1.o", "phys", ""},
		{"index and projection named after carriers", "CREATE TABLE db1.n (a UInt8, INDEX merge (a) TYPE minmax, PROJECTION remote (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a", "phys", ""},
		{"ADD INDEX named cluster", "ALTER TABLE db1.n ADD INDEX cluster (a) TYPE minmax", "phys", ""},
		{"CREATE DICTIONARY named jdbc", "CREATE DICTIONARY db1.jdbc (a UInt64) PRIMARY KEY a SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)", "phys", ""},
		{"table named dictGetter with a phys column", "INSERT INTO db1.dictGetter (phys) VALUES (1)", "phys", ""},
		{"qualified callee is no table function", "SELECT * FROM db1.merge('db1', 'o')", "phys", ""},
		{"INSERT INTO FUNCTION remote", "INSERT INTO FUNCTION remote('h', db1.o) VALUES (1)", "phys", RuleCarrierCallable},
		{"INSERT INTO TABLE FUNCTION remote", "INSERT INTO TABLE FUNCTION remote('h', db1.o) VALUES (1)", "phys", RuleCarrierCallable},
		{"DESCRIBE TABLE merge", "DESCRIBE TABLE merge('db1', 'o')", "phys", RuleCarrierCallable},
		{"DESC remote", "DESC remote('h', db1.o)", "phys", RuleCarrierCallable},
		{"CREATE TABLE AS merge", "CREATE TABLE db1.n AS merge('db1', 'o')", "phys", RuleCarrierCallable},
		{"JOIN cluster", "SELECT * FROM db1.o JOIN cluster('c', db1.p) USING a", "phys", RuleCarrierCallable},
		// Fix round 1, M1: G3 mirrors the engines' T5 refusals.
		{"table function the engines do not recognise", "SELECT * FROM timeSeriesSamples('db1', 'o')", "phys", RuleCarrierCallable},
		{"icebergS3", "SELECT * FROM icebergS3('http://x')", "phys", RuleCarrierCallable},
		{"primes", "SELECT * FROM primes(10)", "phys", RuleCarrierCallable},
		{"viewIfPermitted", "SELECT * FROM viewIfPermitted(SELECT 1 ELSE null('a UInt8'))", "phys", RuleCarrierCallable},
		{"mergeTree table function", "SELECT * FROM mergeTree('db1', 'o')", "phys", RuleCarrierCallable},
		{"mergeTreePartInfo is a scalar function", "SELECT mergeTreePartInfo('all_1_1_0')", "phys", ""},
		{"timeSeries aggregate functions are not table functions", "SELECT timeSeriesRateToGrid(1, 2, 3, 4)(t, v) FROM db1.o", "phys", ""},
		{"Buffer engine", "CREATE TABLE db1.b (a UInt8) ENGINE = Buffer(db1, o, 1, 10, 100, 10000, 1000000, 10000000, 100000000)", "phys", RuleCarrierCallable},
		{"Distributed engine", "CREATE TABLE db1.d (a UInt8) ENGINE = Distributed(c, db1, o)", "phys", RuleCarrierCallable},
		{"Merge engine", "CREATE TABLE db1.m (a UInt8) ENGINE = Merge('db1', 'o')", "phys", RuleCarrierCallable},
		{"Kafka engine without =", "CREATE TABLE db1.k (a UInt8) ENGINE Kafka", "phys", RuleCarrierCallable},
		{"quoted URL engine", "CREATE TABLE db1.u (a UInt8) ENGINE = `URL`('http://x', CSV)", "phys", RuleCarrierCallable},
		{"engine name in the wrong case", "CREATE TABLE db1.n (a UInt8) ENGINE = memory", "phys", RuleCarrierCallable},
		{"Replicated engine with arguments", "CREATE TABLE db1.n (a UInt8) ENGINE = ReplicatedMergeTree('/p', 'r') ORDER BY a", "phys", RuleCarrierCallable},
		{"materialized view engine", "CREATE MATERIALIZED VIEW db1.mv ENGINE = Buffer(db1, o, 1, 1, 1, 1, 1, 1, 1) AS SELECT a FROM db1.o", "phys", RuleCarrierCallable},
		{"Replicated engine without arguments", "CREATE TABLE db1.n (a UInt8) ENGINE = ReplicatedMergeTree ORDER BY a", "phys", ""},
		{"Replicated engine with empty arguments", "CREATE TABLE db1.n (a UInt8) ENGINE = ReplicatedReplacingMergeTree() ORDER BY a", "phys", ""},
		{"Memory engine without =", "CREATE TABLE db1.n (a UInt8) ENGINE Memory", "phys", ""},
		{"quoted Memory engine", "CREATE TABLE db1.n (a UInt8) engine=`Memory`", "phys", ""},
		{"a column named engine", "CREATE TABLE db1.n (engine String) ENGINE = Log", "phys", ""},
		{"engine in a view body", "CREATE VIEW db1.v AS SELECT engine FROM system.tables", "phys", ""},
		{"engine alias after AS SELECT", "CREATE TABLE db1.n ENGINE = Memory AS SELECT 1 AS engine", "phys", ""},
		{"engine after a CREATE ... AS table", "CREATE TABLE db1.n AS db1.o ENGINE = Memory", "phys", ""},
		{"engine read outside CREATE", "SELECT engine FROM system.tables WHERE engine = 'Buffer'", "phys", ""},
		{"database engine", "CREATE DATABASE db1 ENGINE = Atomic", "phys", ""},
		{"database engine named after a carrier", "CREATE DATABASE db2 ENGINE = MySQL('h:9004', 'phys', 'u', 'p')", "phys", RuleCarrierCallable},
		// Fix round 1: the physical database as a DATABASE object.
		{"drop database phys", "DROP DATABASE phys", "phys", RulePhysicalDatabase},
		{"drop database if exists phys", "DROP DATABASE IF EXISTS `phys`", "phys", RulePhysicalDatabase},
		{"create database if not exists phys", `CREATE DATABASE IF NOT EXISTS "phys"`, "phys", RulePhysicalDatabase},
		{"attach database phys", "ATTACH DATABASE phys", "phys", RulePhysicalDatabase},
		{"detach database phys", "DETACH DATABASE phys", "phys", RulePhysicalDatabase},
		{"show create database phys", "SHOW CREATE DATABASE phys", "phys", RulePhysicalDatabase},
		{"exists database phys", "EXISTS DATABASE phys", "phys", RulePhysicalDatabase},
		{"alter database phys", "ALTER DATABASE phys MODIFY COMMENT 'x'", "phys", RulePhysicalDatabase},
		{"truncate database phys", "TRUNCATE DATABASE phys", "phys", RulePhysicalDatabase},
		{"rename database to phys", "RENAME DATABASE db1 TO phys", "phys", RulePhysicalDatabase},
		{"rename database phys", "rename database `phys` to db9", "phys", RulePhysicalDatabase},
		{"drop a tenant database", "DROP DATABASE db1", "phys", ""},
		{"a column named database", "SELECT database, name FROM system.tables WHERE database = 'phys'", "phys", ""},
		// Fix round 2: every database-object position ClickHouse 26.8.1
		// parses (measured with clickhouse format).
		{"truncate all tables from phys", "TRUNCATE ALL TABLES FROM phys", "phys", RulePhysicalDatabase},
		{"truncate tables from quoted phys like", "TRUNCATE TABLES FROM `phys` LIKE '%'", "phys", RulePhysicalDatabase},
		{"truncate all tables from if exists phys", `truncate all tables from if exists "phys" NOT LIKE 'x'`, "phys", RulePhysicalDatabase},
		{"truncate all tables from a tenant database", "TRUNCATE ALL TABLES FROM db1", "phys", ""},
		{"show temporary tables from phys", "SHOW TEMPORARY TABLES FROM phys", "phys", RulePhysicalDatabase},
		{"show dictionaries in phys", "SHOW DICTIONARIES IN phys", "phys", RulePhysicalDatabase},
		{"show index from t in phys", "SHOW EXTENDED INDEXES FROM t IN `phys`", "phys", RulePhysicalDatabase},
		{"system sync database replica phys", "SYSTEM SYNC DATABASE REPLICA phys", "phys", RulePhysicalDatabase},
		{"system drop replica from database phys", "SYSTEM DROP DATABASE REPLICA 'r' FROM DATABASE phys", "phys", RulePhysicalDatabase},
		{"backup database phys", "BACKUP DATABASE phys TO Disk('b', '1.zip')", "phys", RulePhysicalDatabase},
		{"backup database as phys", "BACKUP DATABASE db1 AS phys TO Disk('b', '1.zip')", "phys", RulePhysicalDatabase},
		{"restore database as phys", "RESTORE DATABASE db1 AS `phys` FROM Disk('b', '1.zip')", "phys", RulePhysicalDatabase},
		{"restore all except database phys", "RESTORE ALL EXCEPT DATABASE phys FROM Disk('b', '1.zip')", "phys", RulePhysicalDatabase},
		{"use database phys", "USE DATABASE phys", "phys", RulePhysicalDatabase},
		{"detach database phys permanently", "DETACH DATABASE phys PERMANENTLY", "phys", RulePhysicalDatabase},
		{"alter database phys modify setting", "ALTER DATABASE phys MODIFY SETTING x = 1", "phys", RulePhysicalDatabase},
		{"describe database phys", "DESCRIBE DATABASE phys", "phys", RulePhysicalDatabase},
		{"a column named tables", "SELECT tables FROM db1.o WHERE tables IN (1)", "phys", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, detail := Check(tc.sql, tc.phys, reserved)
			if rule != tc.want {
				t.Fatalf("rule = %q (%s), want %q", rule, detail, tc.want)
			}
			if (rule == "") != (detail == "") {
				t.Fatalf("rule %q with detail %q: a hit needs a detail and a pass none", rule, detail)
			}
		})
	}
}

// TestCheck_ScanDetails pins that each scanner refusal names its cause, so an
// operator reading a scan rejection knows what to change.
func TestCheck_ScanDetails(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"SELECT $ FROM db1.o", "$"},
		{"SELECT 1 #x\n", "#"},
		{"SELECT 1 AS phys\u00e9", "non-ASCII"},
		{"SELECT 'x", "unterminated"},
	} {
		rule, detail := Check(tc.sql, "phys", reserved)
		if rule != RuleScan || !strings.Contains(detail, tc.want) {
			t.Errorf("Check(%q) = %q, %q; want rule %q with a detail mentioning %q", tc.sql, rule, detail, RuleScan, tc.want)
		}
	}
}

func newSessionForTest(t *testing.T) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chsession.New(1, client)
}

func queryFor(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
}

func TestOnQuery_SessionGate(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeEnforce}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		want bool // refused?
	}{
		{"ordinary", func(*chsession.SessionState) {}, true},
		{"driver", func(s *chsession.SessionState) { s.SetIsDriver(true) }, true},
		{"origin-side forwarding", func(s *chsession.SessionState) { s.SetForwarding(true) }, true},
		{"maintenance", func(s *chsession.SessionState) { s.SetMaintenance(true) }, false},
		{"platform operator", func(s *chsession.SessionState) { s.SetPlatformOperator(true) }, false},
		{"peer trusted", func(s *chsession.SessionState) { s.SetPeerTrust("peer:9001") }, false},
		{"forwarded from peer", func(s *chsession.SessionState) { s.SetPeerTrustForwarded("peer:9001", true) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t)
			tc.set(sess.State())
			// Through the chain, so the marker methods are exercised too: the
			// chain's forward and peer-trust filters must leave the decision to
			// OnQuery.
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			err := chain.OnQuery(context.Background(), queryFor(sess, "SELECT * FROM phys.`db2.x`"))
			if (err != nil) != tc.want {
				t.Fatalf("refused = %v, want %v (%v)", err != nil, tc.want, err)
			}
			if err != nil && !strings.Contains(err.Error(), "table-reference guard: physical_database: protected database phys is not addressable; the rewriter applies the same policy") {
				t.Fatalf("message = %q", err.Error())
			}
		})
	}
}

// TestOnQuery_ForwardedFromPeerAloneSkips pins the IsForwardedFromPeer branch
// on its own: SetPeerTrustForwarded also sets IsPeerTrusted, so the session
// gate test cannot tell the two checks apart.
func TestOnQuery_ForwardedFromPeerAloneSkips(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	sess := newSessionForTest(t)
	sess.State().IsForwardedFromPeer = true
	if snap := sess.State().Snapshot(); snap.IsPeerTrusted || !snap.IsForwardedFromPeer {
		t.Fatalf("snapshot = %+v, want only IsForwardedFromPeer", snap)
	}
	if err := p.OnQuery(context.Background(), queryFor(sess, "USE phys")); err != nil {
		t.Fatalf("a forwarded-from-peer session must skip the guard: %v", err)
	}
}

func TestOnQuery_Message(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeEnforce}
	err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "SELECT * FROM phys.`db2.x`"))
	if err == nil || err.Error() != "table-reference guard: physical_database: protected database phys is not addressable; the rewriter applies the same policy" {
		t.Fatalf("err = %v", err)
	}
}

func TestOnQuery_EmptyModeEnforces(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "USE phys")); err == nil {
		t.Fatal("an empty mode must enforce")
	}
}

func TestOnQuery_ObserveModeCountsWithoutRefusing(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeObserve}
	before := testutil.ToFloat64(rejections.WithLabelValues(RuleIdentifierPlaceholder))
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "SELECT * FROM {p:Identifier}")); err != nil {
		t.Fatalf("observe mode must not refuse: %v", err)
	}
	if after := testutil.ToFloat64(rejections.WithLabelValues(RuleIdentifierPlaceholder)); after != before+1 {
		t.Fatalf("counter = %v, want %v", after, before+1)
	}
}

func TestOnQuery_EnforceModeCounts(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved, Mode: ModeEnforce}
	before := testutil.ToFloat64(rejections.WithLabelValues(RuleScan))
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "SELECT $ FROM db1.o")); err == nil {
		t.Fatal("enforce mode must refuse")
	}
	if after := testutil.ToFloat64(rejections.WithLabelValues(RuleScan)); after != before+1 {
		t.Fatalf("counter = %v, want %v", after, before+1)
	}
}

func TestOnQuery_PassingStatementIsNotCounted(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	sum := func() float64 {
		var total float64
		for _, rule := range []string{RuleReservedName, RulePhysicalDatabase, RuleCarrierCallable, RuleIdentifierPlaceholder, RuleEscapedIdentifier, RuleScan} {
			total += testutil.ToFloat64(rejections.WithLabelValues(rule))
		}
		return total
	}
	was := sum()
	if err := p.OnQuery(context.Background(), queryFor(newSessionForTest(t), "SELECT a FROM db1.o WHERE name = 'phys'")); err != nil {
		t.Fatalf("err = %v", err)
	}
	if got := sum(); got != was {
		t.Fatalf("a passing statement changed the counters: %v -> %v", was, got)
	}
}

func TestOnQuery_ScansOriginalSQLWhenBodyIsEmpty(t *testing.T) {
	p := &Plugin{PhysicalDatabase: "phys", ReservedDatabases: reserved}
	qctx := &plugin.QueryContext{Session: newSessionForTest(t), OriginalSQL: "USE phys"}
	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("the original SQL must be scanned when the query body is absent")
	}
	if err := p.OnQuery(context.Background(), &plugin.QueryContext{OriginalSQL: "USE phys"}); err != nil {
		t.Fatalf("a query without a session is not the guard's: %v", err)
	}
}

func TestMarkers(t *testing.T) {
	p := &Plugin{}
	if !p.RunOnForward() || !p.RunOnPeerTrust() || !p.RejectUndecodableQuery() {
		t.Fatal("the guard must opt into forward and peer-trust sessions and reject undecodable queries")
	}
}

func TestConfigValidate(t *testing.T) {
	for _, mode := range []string{"", "enforce", "observe"} {
		if err := (Config{Mode: mode}).Validate(); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	if err := (Config{Mode: "audit"}).Validate(); err == nil || !strings.Contains(err.Error(), `tableref_guard.mode "audit" is invalid`) {
		t.Fatalf("err = %v", err)
	}
}
