package sitablestate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

func newSession(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

func accessed(ids ...string) []sqlmeta.AccessedTable {
	var out []sqlmeta.AccessedTable
	for _, id := range ids {
		db, table, _ := strings.Cut(id, ".")
		out = append(out, sqlmeta.AccessedTable{OriginalDatabase: db, OriginalTable: table, LogicalDatabase: db})
	}
	return out
}

// statusSnapshot has db1.o Ordinary, db1.p Pending, db1.r Refused, db1.a
// Active and db1.g Gone; everything else is Ordinary.
func statusSnapshot() sitable.Snapshot {
	return sitable.NewSnapshot(7, sitable.Ordinary, []sitable.Table{
		{ID: "db1.p", Status: sitable.Pending},
		{ID: "db1.r", Status: sitable.Refused, RefusedCode: "column_type", RefusedReason: "column b: type Decimal(10, 2) is not admitted"},
		{ID: "db1.a", Status: sitable.Active},
		{ID: "db1.g", Status: sitable.Gone},
	})
}

func run(t *testing.T, typ sqlmeta.StatementType, sql string, tables []sqlmeta.AccessedTable) error {
	t.Helper()
	qctx := &plugin.QueryContext{
		Session:        newSession(t, 1),
		OriginalSQL:    sql,
		Query:          &chproto.Query{Body: sql},
		StatementType:  typ,
		AccessedTables: tables,
		TableSnapshot:  statusSnapshot(),
	}
	return (&Plugin{}).OnQuery(context.Background(), qctx)
}

type outcome struct {
	code   int32  // 0 = allowed
	prefix string // message prefix when refused
}

func check(t *testing.T, name string, err error, want outcome) {
	t.Helper()
	if want.code == 0 {
		if err != nil {
			t.Errorf("%s: err = %v, want allowed", name, err)
		}
		return
	}
	var ce *chproto.ClientError
	if !errors.As(err, &ce) {
		t.Errorf("%s: err = %v, want ClientError code %d", name, err, want.code)
		return
	}
	if ce.Code != want.code || !strings.HasPrefix(ce.Message, want.prefix) || ce.KeepSession {
		t.Errorf("%s: got code %d message %q keep=%v, want code %d prefix %q", name, ce.Code, ce.Message, ce.KeepSession, want.code, want.prefix)
	}
}

var (
	ok         = outcome{}
	pendingErr = func(id string) outcome {
		return outcome{733, "storage_integrity: table " + id + " is pending activation (retryable)"}
	}
	refusedErr = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " was refused: column_type: column b"}
	}
	unknownErr = func(id string) outcome { return outcome{60, "Table " + id + " does not exist"} }
	purgingErr = func(id string) outcome {
		return outcome{733, "storage_integrity: table " + id + " is still being purged; retry CREATE later (retryable)"}
	}
	withDataErr = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " is governed by storage integrity and cannot be created with data; create the table first, then INSERT"}
	}
	unreadableErr = outcome{392, "storage_integrity: the materialized view header cannot be read, so its view name and target may be governed by storage integrity; create the table first, then INSERT"}
	alterErr      = func(id string) outcome {
		return outcome{392, "storage_integrity: table " + id + " is governed by storage integrity; ALTER and RENAME are not supported"}
	}
)

// TestDecisionMatrix is spec 2026-09-24 §7.2 row by row.
func TestDecisionMatrix(t *testing.T) {
	type column struct {
		name string
		typ  sqlmeta.StatementType
		sql  func(id string) string
	}
	columns := []column{
		{"data read", sqlmeta.StatementTypeSelect, func(id string) string { return "SELECT * FROM " + id }},
		{"data write", sqlmeta.StatementTypeInsert, func(id string) string { return "INSERT INTO " + id + " FORMAT Native" }},
		{"describe", sqlmeta.StatementTypeDescribe, func(id string) string { return "DESCRIBE TABLE " + id }},
		{"show create", sqlmeta.StatementTypeShowCreateTable, func(id string) string { return "SHOW CREATE TABLE " + id }},
		{"exists", sqlmeta.StatementTypeExistsTable, func(id string) string { return "EXISTS TABLE " + id }},
		{"create same name", sqlmeta.StatementTypeCreateTable, func(id string) string { return "CREATE TABLE " + id + " (a UInt8) ENGINE = Memory" }},
		{"drop", sqlmeta.StatementTypeDropTable, func(id string) string { return "DROP TABLE " + id }},
		{"alter", sqlmeta.StatementTypeAlterTable, func(id string) string { return "ALTER TABLE " + id + " ADD COLUMN b UInt8" }},
		{"rename", sqlmeta.StatementTypeRenameTable, func(id string) string { return "RENAME TABLE " + id + " TO db1.z" }},
	}
	rows := map[string][]outcome{
		"db1.o": {ok, ok, ok, ok, ok, ok, ok, ok, ok},
		"db1.p": {pendingErr("db1.p"), pendingErr("db1.p"), ok, ok, ok, ok, ok, alterErr("db1.p"), alterErr("db1.p")},
		"db1.r": {refusedErr("db1.r"), refusedErr("db1.r"), ok, ok, ok, ok, ok, refusedErr("db1.r"), refusedErr("db1.r")},
		"db1.a": {ok, ok, ok, ok, ok, ok, ok, ok, ok},
		"db1.g": {unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), purgingErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g"), unknownErr("db1.g")},
	}
	for id, want := range rows {
		for i, col := range columns {
			tables := accessed(id)
			if col.typ == sqlmeta.StatementTypeRenameTable {
				tables = accessed(id, "db1.z")
			}
			check(t, id+" / "+col.name, run(t, col.typ, col.sql(id), tables), want[i])
		}
	}
}

// TestDataCarryingCreationIntoGovernedTables is §7.3 rule 2.
func TestDataCarryingCreationIntoGovernedTables(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    sqlmeta.StatementType
		sql    string
		tables []sqlmeta.AccessedTable
		want   outcome
	}{
		{"CTAS into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.o", accessed("db1.p"), withDataErr("db1.p")},
		{"CTAS into an active name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.a ENGINE = MergeTree ORDER BY a AS SELECT a FROM db1.o", accessed("db1.a"), withDataErr("db1.a")},
		{"CTAS into an ordinary name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.o"), ok},
		// Final ruling I1: for data-carrying creation a Refused name is
		// governed too, so it cannot be dropped and refilled by CTAS while it
		// is still Refused. Plain CREATE and a schema copy still pass.
		{"CTAS into a refused name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.r ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.r"), withDataErr("db1.r")},
		{"CREATE OR REPLACE AS SELECT into a refused name", sqlmeta.StatementTypeCreateTable, "CREATE OR REPLACE TABLE db1.r ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.r"), withDataErr("db1.r")},
		{"CREATE OR REPLACE AS SELECT into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE OR REPLACE TABLE db1.p ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.p"), withDataErr("db1.p")},
		{"unlexable CREATE of a refused name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.r (a UInt8) ENGINE = Memory COMMENT 'x", accessed("db1.r"), withDataErr("db1.r")},
		{"schema copy into a refused name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.r AS db1.o ENGINE = Memory", accessed("db1.r", "db1.o"), ok},
		// Conservative false positive (ruled): unnormalised EMPTY AS SELECT is
		// refused. The engine's forwarded body drops an EMPTY body, see
		// TestCreateTableLexesTheForwardedBody.
		{"CTAS EMPTY into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = MergeTree ORDER BY a EMPTY AS SELECT 1 AS a", accessed("db1.p"), withDataErr("db1.p")},
		// Engine-shaped: rewriter-go v0.13.0 reported only the CTAS target
		// ([db1.o]), not its SELECT sources, so the gate cannot see db1.p here.
		// The v0.16.0 floor reports the source (spec 2026-09-26; shared corpus
		// case si_tr_ctas_own_source_rewritten), which the next row pins.
		{"CTAS reading a pending table (engine-shaped)", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = Memory AS SELECT * FROM db1.p", accessed("db1.o"), ok},
		// Contract-dependent: IF the engine reported the source, it is a data
		// read and the Pending refusal applies.
		{"CTAS reading a pending table (contract-dependent: source reported)", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = Memory AS SELECT * FROM db1.p", accessed("db1.o", "db1.p"), pendingErr("db1.p")},
		{"CTAS into a pending name, extra parens", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = Memory AS ((SELECT 1 AS a))", accessed("db1.p"), withDataErr("db1.p")},
		{"CTAS into a pending name, FROM-first", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = Memory AS FROM numbers(3) SELECT number AS a", accessed("db1.p"), withDataErr("db1.p")},
		{"CTAS into a pending name, heredoc comment", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = Memory COMMENT $$'$$ AS SELECT 1 AS a", accessed("db1.p"), withDataErr("db1.p")},
		{"schema copy into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p AS db1.o ENGINE = Memory", accessed("db1.p", "db1.o"), ok},
		{"plain CREATE of a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p (a UInt8) ENGINE = Memory", accessed("db1.p"), ok},
		{"unlexable CREATE of a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p (a UInt8) ENGINE = Memory COMMENT 'x", accessed("db1.p"), withDataErr("db1.p")},
		{"unlexable CREATE of an ordinary name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o (a UInt8) ENGINE = Memory COMMENT 'x", accessed("db1.o"), ok},
		{"schema clone of a pending table", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o AS db1.p", accessed("db1.o", "db1.p"), ok},
		{"MV POPULATE into a pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p ENGINE = Memory POPULATE AS SELECT a FROM db1.o", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO a pending table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.p AS SELECT a FROM db1.o", accessed("db1.mv", "db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO an active table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.a AS SELECT a FROM db1.o", accessed("db1.mv", "db1.a", "db1.o"), withDataErr("db1.a")},
		{"MV TO a refused table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.r AS SELECT a FROM db1.o", accessed("db1.mv", "db1.r", "db1.o"), refusedErr("db1.r")},
		{"MV TO an ordinary table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), ok},
		{"MV reading a pending source", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.p", accessed("db1.mv", "db1.p"), pendingErr("db1.p")},
		// The engine omits a REFRESH ... TO target from AccessedTables (and from
		// its forwarded body); the gate takes it from the parsed header.
		{"refreshable MV TO a pending table, target not reported", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR TO db1.p AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), withDataErr("db1.p")},
		{"MV TO an active table, target not reported", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.a AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), withDataErr("db1.a")},
		// Spec gap (ruled): an MV whose own name is governed is refused in any
		// form, because its inner storage ingests rows under that name.
		{"plain MV named like a pending table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"plain MV named like an active table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.a ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.a", "db1.o"), withDataErr("db1.a")},
		{"MV named like a pending table TO an ordinary one", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p TO db1.o AS SELECT a FROM db1.o", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"plain MV named like a refused table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.r ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.r", "db1.o"), withDataErr("db1.r")},
		{"MV named like a refused table TO an ordinary one", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.r TO db1.o AS SELECT a FROM db1.o", accessed("db1.r", "db1.o"), withDataErr("db1.r")},
		// The view's own name comes from the header, not AccessedTables[0].
		{"MV named like a pending table, view not reported first", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.o", "db1.p"), withDataErr("db1.p")},
		{"MV named like a pending table, view not reported", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW IF NOT EXISTS db1.p ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.o"), withDataErr("db1.p")},
		{"plain MV with an ordinary name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), ok},
		// An unreadable header is refused whatever the engine reported.
		{"MV with an unreadable header", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv /* TO db1.p AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), unreadableErr},
		{"MV with an unreadable header, nothing accessed", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv /* TO db1.p AS SELECT a FROM db1.o", nil, unreadableErr},
		{"MV with no view name, nothing accessed", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW AS SELECT 1", nil, unreadableErr},
		// Spec 2026-09-26 T10: ClickHouse reads `\x70` as p but the lexer cannot, so
		// the header is unreadable and the statement is refused.
		{"MV TO an escaped pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x70` AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), unreadableErr},
		{"schema copy into an escaped pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.`\\x70` AS db1.o ENGINE = Memory", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"view over a gone table", sqlmeta.StatementTypeCreateView, "CREATE VIEW db1.v AS SELECT a FROM db1.g", accessed("db1.v", "db1.g"), unknownErr("db1.g")},
		{"CTAS into a gone name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.g ENGINE = Memory AS SELECT 1 AS a", accessed("db1.g"), purgingErr("db1.g")},
	} {
		check(t, tc.name, run(t, tc.typ, tc.sql, tc.tables), tc.want)
	}
}

// TestCreateTableLexesTheForwardedBody: the data-carrying check reads the
// post-rewrite Query.Body, which is what ClickHouse executes, and falls back to
// OriginalSQL only without a query packet. The engine normalises comments and
// heredocs; rewriter-go v0.13.0 also dropped an EMPTY AS SELECT body, which
// the v0.16.0 floor keeps (see lexer.go), so the first row pins the gate's
// reading of a dropped body, not today's engine output.
func TestCreateTableLexesTheForwardedBody(t *testing.T) {
	for _, tc := range []struct {
		name, original, body string
		want                 outcome
	}{
		{"EMPTY body dropped by the engine", "CREATE TABLE db1.p ENGINE = Memory EMPTY AS SELECT 1 AS a", `CREATE TABLE phys."db1.p" ENGINE=Memory`, ok},
		{"forwarded body carries data", "CREATE TABLE db1.p (a UInt8) ENGINE = Memory", `CREATE TABLE phys."db1.p" ENGINE=Memory AS (SELECT 1 AS a)`, withDataErr("db1.p")},
		{"no query packet falls back to the original", "CREATE TABLE db1.p ENGINE = Memory AS SELECT 1 AS a", "", withDataErr("db1.p")},
	} {
		qctx := &plugin.QueryContext{
			Session:        newSession(t, 4),
			OriginalSQL:    tc.original,
			StatementType:  sqlmeta.StatementTypeCreateTable,
			AccessedTables: accessed("db1.p"),
			TableSnapshot:  statusSnapshot(),
		}
		if tc.body != "" {
			qctx.Query = &chproto.Query{Body: tc.body}
		}
		check(t, tc.name, (&Plugin{}).OnQuery(context.Background(), qctx), tc.want)
	}
}

// TestMaterializedViewTargetUsesTheSessionDatabase: an unqualified TO target
// resolves against the session's logical database.
func TestMaterializedViewTargetUsesTheSessionDatabase(t *testing.T) {
	sess := newSession(t, 5)
	sess.State().SetLogicalDatabase("db1")
	qctx := &plugin.QueryContext{
		Session: sess, OriginalSQL: "CREATE MATERIALIZED VIEW mv TO p AS SELECT a FROM o",
		StatementType:  sqlmeta.StatementTypeCreateMaterializedView,
		AccessedTables: []sqlmeta.AccessedTable{{OriginalTable: "mv"}, {OriginalTable: "o"}},
		TableSnapshot:  statusSnapshot(),
	}
	check(t, "unqualified TO pending", (&Plugin{}).OnQuery(context.Background(), qctx), withDataErr("db1.p"))
}

// TestRefusalPrecedence: unknown table, then non-retryable, then retryable;
// the first accessed table breaks a tie.
func TestRefusalPrecedence(t *testing.T) {
	check(t, "retryable then unknown", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.g")), unknownErr("db1.g"))
	check(t, "retryable then non-retryable", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.r")), refusedErr("db1.r"))
	check(t, "non-retryable then unknown", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.r", "db1.g", "db1.p")), unknownErr("db1.g"))
	check(t, "two retryable keep the first", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.p", "db1.o", "db2.p")), pendingErr("db1.p"))
	check(t, "ordinary and active only", run(t, sqlmeta.StatementTypeSelect, "SELECT", accessed("db1.o", "db1.a")), ok)
}

func TestUnqualifiedTableUsesTheSessionDatabase(t *testing.T) {
	sess := newSession(t, 2)
	sess.State().SetLogicalDatabase("db1")
	qctx := &plugin.QueryContext{
		Session: sess, OriginalSQL: "SELECT * FROM p", StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{OriginalTable: "p"}}, TableSnapshot: statusSnapshot(),
	}
	check(t, "unqualified pending", (&Plugin{}).OnQuery(context.Background(), qctx), pendingErr("db1.p"))
}

func TestDatabaseLevelStatementsAreNotDecided(t *testing.T) {
	for _, typ := range []sqlmeta.StatementType{sqlmeta.StatementTypeDropDatabase, sqlmeta.StatementTypeCreateDatabase, sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke} {
		check(t, typ.String(), run(t, typ, "X", accessed("db1.g")), ok)
	}
}

// TestBypassRules is §7.1: peer-trusted and origin-side forwarding sessions
// skip the plugin through the chain filters, IsForwardedFromPeer runs it on
// the receiving host, and maintenance / platform-operator sessions keep their
// existing bypass.
func TestBypassRules(t *testing.T) {
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{&Plugin{}}}
	query := func(sess chsession.Session) error {
		return chain.OnQuery(context.Background(), &plugin.QueryContext{
			Session: sess, OriginalSQL: "SELECT * FROM db1.g", StatementType: sqlmeta.StatementTypeSelect,
			AccessedTables: accessed("db1.g"), TableSnapshot: statusSnapshot(),
		})
	}
	peer := newSession(t, 10)
	peer.State().SetPeerTrust("10.0.0.7:9001")
	check(t, "peer-trusted", query(peer), ok)

	forwarding := newSession(t, 11)
	forwarding.State().SetForwarding(true)
	check(t, "origin-side forwarding", query(forwarding), ok)

	forwarded := newSession(t, 12)
	forwarded.State().SetPeerTrustForwarded("10.0.0.7:9001", true)
	check(t, "forwarded from peer", query(forwarded), unknownErr("db1.g"))

	maintenance := newSession(t, 13)
	maintenance.State().SetMaintenance(true)
	check(t, "maintenance", query(maintenance), ok)

	operator := newSession(t, 14)
	operator.State().SetPlatformOperator(true)
	check(t, "platform operator", query(operator), ok)

	check(t, "ordinary session", query(newSession(t, 15)), unknownErr("db1.g"))
}

// TestNoSnapshotFailsClosed: the plugin is registered only when storage
// integrity is enabled, and rewrite always attaches a snapshot on every session
// this plugin runs for, so a nil snapshot can only be a wiring defect. It is
// refused non-retryably, even for a statement that touches no table and for a
// session the plugin would otherwise bypass.
func TestNoSnapshotFailsClosed(t *testing.T) {
	unavailable := outcome{392, "storage_integrity: table state is unavailable for this query"}
	for _, tc := range []struct {
		name   string
		tables []sqlmeta.AccessedTable
		setup  func(chsession.Session)
	}{
		{name: "gone table", tables: accessed("db1.g")},
		{name: "no accessed table"},
		{name: "maintenance", tables: accessed("db1.o"), setup: func(s chsession.Session) { s.State().SetMaintenance(true) }},
	} {
		sess := newSession(t, 3)
		if tc.setup != nil {
			tc.setup(sess)
		}
		qctx := &plugin.QueryContext{Session: sess, StatementType: sqlmeta.StatementTypeSelect, AccessedTables: tc.tables}
		err := (&Plugin{}).OnQuery(context.Background(), qctx)
		check(t, tc.name, err, unavailable)
		var ce *chproto.ClientError
		if errors.As(err, &ce) && ce.Message != unavailable.prefix {
			t.Errorf("%s: message %q, want exactly %q", tc.name, ce.Message, unavailable.prefix)
		}
	}
}

func TestNoQueryOrSessionIsANoOp(t *testing.T) {
	check(t, "nil query context", (&Plugin{}).OnQuery(context.Background(), nil), ok)
	check(t, "nil session", (&Plugin{}).OnQuery(context.Background(), &plugin.QueryContext{TableSnapshot: statusSnapshot(), AccessedTables: accessed("db1.g")}), ok)
}

// replicaTables serve db2.a and db2.b (owner indexer 0) and db3.c (owner
// indexer 3) from member copies (spec 2026-10-14 §11); db1.p is Pending and
// everything else is Ordinary.
func replicaTables() []sitable.Table {
	return []sitable.Table{
		{ID: "db1.p", Status: sitable.Pending},
		{ID: "db2.a", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.a", 4), ReplicaOf: 0},
		{ID: "db2.b", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.b", 1), ReplicaOf: 0},
		{ID: "db3.c", Status: sitable.Replica, CopyTable: sitable.CopyTable("db3.c", 7), ReplicaOf: 3},
	}
}

func replicaSnapshot() sitable.Snapshot {
	return sitable.NewSnapshot(9, sitable.Ordinary, replicaTables())
}

// replicaState is the host's table state while it serves replicaTables: the
// port sitablestate checks a counted replica read against.
func replicaState() *sitable.Fake {
	return sitable.NewFake(sitable.Ordinary, replicaTables()...)
}

// servingReads is the host's counter while it holds the leases of db2 and
// db3: their fences are open for the next hour, so the snapshot checks alone
// decide.
func servingReads() *sitable.ReplicaReads {
	reads := sitable.NewReplicaReads()
	until := time.Now().Add(time.Hour)
	reads.ServeUntil("db2", until)
	reads.ServeUntil("db3", until)
	return reads
}

func replicaQuery(sess chsession.Session, typ sqlmeta.StatementType, sql string, tables []sqlmeta.AccessedTable) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:        sess,
		OriginalSQL:    sql,
		Query:          &chproto.Query{Body: sql},
		StatementType:  typ,
		AccessedTables: tables,
		TableSnapshot:  replicaSnapshot(),
	}
}

func replicaErr(id string, owner uint64) outcome {
	return outcome{392, fmt.Sprintf("storage_integrity: table %s is served read-only from a copy of indexer %d's published data; write through its owner", id, owner)}
}

// TestDecide_ReplicaRows is the Replica row of the spec 2026-09-24 §7.2 matrix
// that spec 2026-10-14 §11 adds: reads and metadata pass, every write, DDL and
// data-carrying creation is refused 392 naming the owner (indexer 0 is a valid
// owner), and an unreadable materialized-view header keeps its own refusal.
func TestDecide_ReplicaRows(t *testing.T) {
	replica := sitable.Table{ID: "db2.a", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.a", 4), ReplicaOf: 0}
	for _, tc := range []struct {
		name  string
		class class
		sev   severity
		want  outcome
	}{
		{"none", classNone, allow, ok},
		{"read", classRead, allow, ok},
		{"metadata", classMetadata, allow, ok},
		{"write", classWrite, nonRetryable, replicaErr("db2.a", 0)},
		{"create", classCreate, nonRetryable, replicaErr("db2.a", 0)},
		{"create with data", classCreateData, nonRetryable, replicaErr("db2.a", 0)},
		{"drop", classDrop, nonRetryable, replicaErr("db2.a", 0)},
		{"other DDL", classOtherDDL, nonRetryable, replicaErr("db2.a", 0)},
		{"view target", classViewTarget, nonRetryable, replicaErr("db2.a", 0)},
		{"unreadable", classUnreadable, nonRetryable, unreadableErr},
	} {
		sev, err := decide(replica, tc.class)
		if sev != tc.sev {
			t.Errorf("%s: severity %d, want %d", tc.name, sev, tc.sev)
		}
		check(t, tc.name, err, tc.want)
	}

	// The same rows end to end, by statement type. INSERT ... SELECT from a
	// copy into an ordinary table is a read of the copy.
	for _, tc := range []struct {
		name   string
		typ    sqlmeta.StatementType
		sql    string
		tables []sqlmeta.AccessedTable
		want   outcome
	}{
		{"select", sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"), ok},
		{"join of two copies", sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a JOIN db3.c USING (k)", accessed("db2.a", "db3.c"), ok},
		{"describe", sqlmeta.StatementTypeDescribe, "DESCRIBE TABLE db2.a", accessed("db2.a"), ok},
		{"show create", sqlmeta.StatementTypeShowCreateTable, "SHOW CREATE TABLE db2.a", accessed("db2.a"), ok},
		{"exists", sqlmeta.StatementTypeExistsTable, "EXISTS TABLE db2.a", accessed("db2.a"), ok},
		{"insert select into an ordinary table", sqlmeta.StatementTypeInsert, "INSERT INTO db1.o SELECT * FROM db2.a", accessed("db1.o", "db2.a"), ok},
		{"insert", sqlmeta.StatementTypeInsert, "INSERT INTO db2.a FORMAT Native", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"insert select into a copy", sqlmeta.StatementTypeInsert, "INSERT INTO db3.c SELECT * FROM db1.o", accessed("db3.c", "db1.o"), replicaErr("db3.c", 3)},
		{"update", sqlmeta.StatementTypeUpdate, "ALTER TABLE db2.a UPDATE x = 1 WHERE 1", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"delete", sqlmeta.StatementTypeDelete, "DELETE FROM db2.a WHERE 1", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"truncate", sqlmeta.StatementTypeTruncateTable, "TRUNCATE TABLE db2.a", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"create same name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db2.a (a UInt8) ENGINE = Memory", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"create as select", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db2.a ENGINE = Memory AS SELECT 1 AS a", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"create as select from a copy", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = Memory AS SELECT * FROM db2.a", accessed("db1.o", "db2.a"), ok},
		{"drop", sqlmeta.StatementTypeDropTable, "DROP TABLE db2.a", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"alter", sqlmeta.StatementTypeAlterTable, "ALTER TABLE db2.a ADD COLUMN b UInt8", accessed("db2.a"), replicaErr("db2.a", 0)},
		{"rename", sqlmeta.StatementTypeRenameTable, "RENAME TABLE db2.a TO db2.z", accessed("db2.a", "db2.z"), replicaErr("db2.a", 0)},
		{"materialized view to a copy", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db2.a AS SELECT a FROM db1.o", accessed("db1.mv", "db2.a", "db1.o"), replicaErr("db2.a", 0)},
		{"materialized view named like a copy", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db2.a ENGINE = Memory AS SELECT a FROM db1.o", accessed("db2.a", "db1.o"), replicaErr("db2.a", 0)},
		{"materialized view reading a copy", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT a FROM db2.a", accessed("db1.mv", "db1.o", "db2.a"), ok},
		{"view over a copy", sqlmeta.StatementTypeCreateView, "CREATE VIEW db1.v AS SELECT a FROM db2.a", accessed("db1.v", "db2.a"), ok},
		{"a copy and a pending table", sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a JOIN db1.p USING (k)", accessed("db2.a", "db1.p"), pendingErr("db1.p")},
	} {
		check(t, tc.name, (&Plugin{}).OnQuery(context.Background(), replicaQuery(newSession(t, 40), tc.typ, tc.sql, tc.tables)), tc.want)
	}
}

// TestOnQuery_ReplicaReadBeginsAndCompletesAnInFlightRead is clarification
// K44 of Plan S3-C4: an allowed query that touches a Replica table counts one
// in-flight read per database from OnQuery until OnQueryComplete, through the
// chain as Relay drives it. A refused query, or one that touches no Replica
// table, counts nothing, and two sessions count separately.
func TestOnQuery_ReplicaReadBeginsAndCompletesAnInFlightRead(t *testing.T) {
	reads := servingReads()
	gate := &Plugin{ReplicaReads: reads, TableState: replicaState()}
	chain := &plugin.PluginChain{
		QueryPlugins:         []plugin.QueryPlugin{gate},
		QueryCompletePlugins: []plugin.QueryCompletePlugin{gate},
		ClosePlugins:         []plugin.ClosePlugin{gate},
	}
	ctx := context.Background()
	sess := newSession(t, 41)
	if err := chain.OnQuery(ctx, replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a JOIN db2.b USING (k) JOIN db3.c USING (k) JOIN db1.o USING (k)", accessed("db2.a", "db2.b", "db3.c", "db1.o"))); err != nil {
		t.Fatalf("a join of copies must be allowed: %v", err)
	}
	if got := reads.Counts(); !reflect.DeepEqual(got, map[string]int{"db2": 1, "db3": 1}) {
		t.Fatalf("in flight = %v, want one read of db2 and one of db3", got)
	}
	other := newSession(t, 42)
	if err := chain.OnQuery(ctx, replicaQuery(other, sqlmeta.StatementTypeDescribe, "DESCRIBE TABLE db2.a", accessed("db2.a"))); err != nil {
		t.Fatalf("DESCRIBE of a copy must be allowed: %v", err)
	}
	if reads.InFlight("db2") != 2 {
		t.Fatalf("db2 in flight = %d, want 2 (two sessions)", reads.InFlight("db2"))
	}
	chain.OnQueryComplete(ctx, sess)
	chain.OnQueryComplete(ctx, sess) // a second complete ends nothing more
	if got := reads.Counts(); !reflect.DeepEqual(got, map[string]int{"db2": 1}) {
		t.Fatalf("after the join completed in flight = %v, want the DESCRIBE's db2 only", got)
	}
	chain.OnQueryComplete(ctx, other)

	for _, tc := range []struct {
		name   string
		typ    sqlmeta.StatementType
		sql    string
		tables []sqlmeta.AccessedTable
	}{
		{"no copy", sqlmeta.StatementTypeSelect, "SELECT * FROM db1.o", accessed("db1.o")},
		{"write to a copy", sqlmeta.StatementTypeInsert, "INSERT INTO db2.a FORMAT Native", accessed("db2.a")},
		{"copy refused by another table", sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a JOIN db1.p USING (k)", accessed("db2.a", "db1.p")},
	} {
		_ = chain.OnQuery(ctx, replicaQuery(sess, tc.typ, tc.sql, tc.tables))
		if got := reads.Counts(); len(got) != 0 {
			t.Fatalf("%s: in flight = %v, want nothing counted", tc.name, got)
		}
		chain.OnQueryComplete(ctx, sess)
	}

	// A lost complete never leaves a database counted: the session's next
	// counted read ends the end it still holds.
	if err := chain.OnQuery(ctx, replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db3.c", accessed("db3.c"))); err != nil {
		t.Fatal(err)
	}
	if err := chain.OnQuery(ctx, replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))); err != nil {
		t.Fatal(err)
	}
	if got := reads.Counts(); !reflect.DeepEqual(got, map[string]int{"db2": 1}) {
		t.Fatalf("after a lost complete in flight = %v, want only the session's current read of db2", got)
	}
	chain.OnClose(sess)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("after close in flight = %v, want empty", got)
	}

	// Without a counter the gate decides exactly as before and counts nothing.
	if err := (&Plugin{}).OnQuery(ctx, replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))); err != nil {
		t.Fatalf("a read of a copy without a counter: %v", err)
	}
	(&Plugin{}).OnQueryComplete(ctx, sess)
	(&Plugin{}).OnClose(sess)
}

// TestOnClose_EndsAnInFlightReplicaRead: a client that disconnects mid-query
// never reaches OnQueryComplete, so the session's close ends its read, and a
// late complete after the close changes nothing.
func TestOnClose_EndsAnInFlightReplicaRead(t *testing.T) {
	reads := servingReads()
	gate := &Plugin{ReplicaReads: reads, TableState: replicaState()}
	sess := newSession(t, 43)
	if err := gate.OnQuery(context.Background(), replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db3.c", accessed("db3.c"))); err != nil {
		t.Fatal(err)
	}
	if reads.InFlight("db3") != 1 {
		t.Fatalf("db3 in flight = %d, want 1", reads.InFlight("db3"))
	}
	(&plugin.PluginChain{ClosePlugins: []plugin.ClosePlugin{gate}}).OnClose(sess)
	if reads.InFlight("db3") != 0 {
		t.Fatalf("after close db3 in flight = %d, want 0", reads.InFlight("db3"))
	}
	gate.OnQueryComplete(context.Background(), sess)
	gate.OnClose(sess)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("in flight = %v, want empty", got)
	}
}

// TestReplicaReadsAreKeptPerSessionAcrossListeners: with internal_listen,
// the external and the internal proxy.Server share this gate but number their
// sessions independently, so two sessions can carry the same ID. Each keeps
// its own read: one session's next read, complete or close never ends the
// other's.
func TestReplicaReadsAreKeptPerSessionAcrossListeners(t *testing.T) {
	reads := servingReads()
	gate := &Plugin{ReplicaReads: reads, TableState: replicaState()}
	chain := &plugin.PluginChain{
		QueryPlugins:         []plugin.QueryPlugin{gate},
		QueryCompletePlugins: []plugin.QueryCompletePlugin{gate},
		ClosePlugins:         []plugin.ClosePlugin{gate},
	}
	ctx := context.Background()
	external, internal := newSession(t, 1), newSession(t, 1)
	if err := chain.OnQuery(ctx, replicaQuery(external, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))); err != nil {
		t.Fatal(err)
	}
	if err := chain.OnQuery(ctx, replicaQuery(internal, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.b", accessed("db2.b"))); err != nil {
		t.Fatal(err)
	}
	if n := reads.InFlight("db2"); n != 2 {
		t.Fatalf("db2 in flight = %d, want 2: a session with the same ID must not end the other's read", n)
	}
	chain.OnClose(internal)
	if n := reads.InFlight("db2"); n != 1 {
		t.Fatalf("after the internal session closed db2 in flight = %d, want the external session's read", n)
	}
	chain.OnQueryComplete(ctx, internal)
	if n := reads.InFlight("db2"); n != 1 {
		t.Fatalf("after a late complete of the closed session db2 in flight = %d, want 1", n)
	}
	chain.OnQueryComplete(ctx, external)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("in flight = %v, want empty", got)
	}
}

// TestOnQuery_OperatorSessionsAreNotCounted: maintenance and platform-operator
// sessions return before the matrix, and peer-trusted and origin-side
// forwarding sessions skip the gate in the chain, so none of them counts a
// read of a copy.
func TestOnQuery_OperatorSessionsAreNotCounted(t *testing.T) {
	reads := servingReads()
	gate := &Plugin{ReplicaReads: reads, TableState: replicaState()}
	chain := &plugin.PluginChain{
		QueryPlugins:         []plugin.QueryPlugin{gate},
		QueryCompletePlugins: []plugin.QueryCompletePlugin{gate},
	}
	for i, setup := range []func(chsession.Session){
		func(s chsession.Session) { s.State().SetMaintenance(true) },
		func(s chsession.Session) { s.State().SetPlatformOperator(true) },
		func(s chsession.Session) { s.State().SetPeerTrust("10.0.0.7:9001") },
		func(s chsession.Session) { s.State().SetForwarding(true) },
	} {
		sess := newSession(t, int64(44+i))
		setup(sess)
		if err := chain.OnQuery(context.Background(), replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))); err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		if got := reads.Counts(); len(got) != 0 {
			t.Fatalf("session %d: in flight = %v, want nothing counted", i, got)
		}
		chain.OnQueryComplete(context.Background(), sess)
	}
}

// TestOnQuery_AReplicaReadPlannedBeforeServingEndedIsRefused is clarification
// K44 of Plan S3-C4 with a rewriter blocked across the host's two lease
// polls: the query took its Replica snapshot in rewrite and waited there
// while the host published a snapshot without db2's Replica entries and then
// found no read of db2 in flight, so it released db2's lease and the member
// may install. When the query reaches the gate it is refused with the
// retryable 733 before anything reaches ClickHouse, and it is not left
// counted. A copy re-served under another name, a table served by another
// owner and a gate without a table state refuse the same way; a query planned
// on the current snapshot is admitted.
func TestOnQuery_AReplicaReadPlannedBeforeServingEndedIsRefused(t *testing.T) {
	ctx := context.Background()
	state := replicaState()
	reads := servingReads()
	gate := &Plugin{ReplicaReads: reads, TableState: state}
	sess := newSession(t, 48)
	stale := replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a JOIN db3.c USING (k)", accessed("db2.a", "db3.c"))
	stale.TableSnapshot = state.Current()
	// The host's first poll publishes a snapshot without db2's entries; its
	// next poll finds no read of db2 in flight and releases the lease.
	state.Set(replicaTables()[0], replicaTables()[3])
	if n := reads.InFlight("db2"); n != 0 {
		t.Fatalf("db2 in flight = %d before the blocked query resumed", n)
	}
	refused := outcome{733, "storage_integrity: table db2.a is no longer served from the copy this query was planned against; retry the query (retryable)"}
	check(t, "stale snapshot", gate.OnQuery(ctx, stale), refused)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("in flight = %v after the refusal, want nothing counted", got)
	}
	gate.OnQueryComplete(ctx, sess)

	// A query planned on the current snapshot reads db3.c.
	fresh := replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db3.c", accessed("db3.c"))
	fresh.TableSnapshot = state.Current()
	if err := gate.OnQuery(ctx, fresh); err != nil || reads.InFlight("db3") != 1 {
		t.Fatalf("OnQuery = %v, db3 in flight %d; want an admitted, counted read", err, reads.InFlight("db3"))
	}
	gate.OnQueryComplete(ctx, sess)

	// db2 is served again, from another incarnation's copy, or by another
	// owner: the old plan names neither.
	planned := replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))
	for name, a := range map[string]sitable.Table{
		"another copy":  {ID: "db2.a", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.a", 5), ReplicaOf: 0},
		"another owner": {ID: "db2.a", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.a", 4), ReplicaOf: 2},
	} {
		state.Set(a, replicaTables()[3])
		check(t, name, gate.OnQuery(ctx, planned), refused)
		if got := reads.Counts(); len(got) != 0 {
			t.Fatalf("%s: in flight = %v, want nothing counted", name, got)
		}
		gate.OnQueryComplete(ctx, sess)
	}
	check(t, "no table state", (&Plugin{ReplicaReads: reads}).OnQuery(ctx, planned), refused)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("without a table state in flight = %v, want nothing counted", got)
	}
}

// TestOnQuery_AFreshReplicaReadAfterTheLeaseDeadlineIsRefused is
// clarification K44 of Plan S3-C4 with the host's table-state refresh blocked
// (review round 2): the host cannot reach its member, so db2's lease is not
// renewed and its fence stays at the local deadline minus
// serve.HostDeadlineMargin, while a refresh blocked in its chain-state read
// keeps the published snapshot answering db2 Replica past that fence and past
// the lease's TTL, when the member may install. A fresh query planned on that
// snapshot is admitted before the fence and refused with the retryable 733
// from the fence on, counting nothing, although the current snapshot still
// serves the copy. db3, whose fence is open, still reads, a join of both is
// refused as a whole, a fence the host closes (a dropped lease) refuses at
// once, and a renewal that moves the fence admits again.
func TestOnQuery_AFreshReplicaReadAfterTheLeaseDeadlineIsRefused(t *testing.T) {
	const ttl, margin = 30 * time.Second, 3 * time.Second
	granted := time.Unix(1_760_000_000, 0)
	var mu sync.Mutex
	now := granted
	at := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = granted.Add(d)
	}
	reads := sitable.NewReplicaReadsWithClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	reads.ServeUntil("db2", granted.Add(ttl-margin))
	reads.ServeUntil("db3", granted.Add(time.Hour))
	// The published snapshot never changes below: the refresh that would
	// publish the end of db2's lease is blocked.
	state := replicaState()
	gate := &Plugin{ReplicaReads: reads, TableState: state}
	ctx := context.Background()
	sess := newSession(t, 49)
	fresh := func(sql string, ids ...string) *plugin.QueryContext {
		qctx := replicaQuery(sess, sqlmeta.StatementTypeSelect, sql, accessed(ids...))
		qctx.TableSnapshot = state.Current()
		return qctx
	}
	refused := func(id string) outcome {
		return outcome{733, fmt.Sprintf("storage_integrity: table %s is no longer served from the copy this query was planned against; retry the query (retryable)", id)}
	}

	at(ttl - margin - time.Second)
	check(t, "a fresh read before the fence", gate.OnQuery(ctx, fresh("SELECT * FROM db2.a", "db2.a")), ok)
	if reads.InFlight("db2") != 1 {
		t.Fatalf("db2 in flight = %d, want the admitted read", reads.InFlight("db2"))
	}
	gate.OnQueryComplete(ctx, sess)

	for _, d := range []time.Duration{ttl - margin, ttl, ttl + 500*time.Millisecond} {
		at(d)
		if got := state.Current().Lookup("db2", "a").Status; got != sitable.Replica {
			t.Fatalf("%v after the grant the published snapshot answers db2.a %v, want Replica", d, got)
		}
		check(t, fmt.Sprintf("a fresh read %v after the grant while the refresh is blocked", d), gate.OnQuery(ctx, fresh("SELECT * FROM db2.a", "db2.a")), refused("db2.a"))
		if got := reads.Counts(); len(got) != 0 {
			t.Fatalf("%v after the grant in flight = %v, want nothing counted", d, got)
		}
		gate.OnQueryComplete(ctx, sess)
	}

	check(t, "db3 under its open fence", gate.OnQuery(ctx, fresh("SELECT * FROM db3.c", "db3.c")), ok)
	gate.OnQueryComplete(ctx, sess)
	check(t, "a join of db3 and db2", gate.OnQuery(ctx, fresh("SELECT * FROM db3.c JOIN db2.b USING (k)", "db3.c", "db2.b")), refused("db2.b"))
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("after the refused join in flight = %v, want nothing counted", got)
	}
	gate.OnQueryComplete(ctx, sess)

	reads.ServeUntil("db3", time.Time{})
	check(t, "db3 after its fence was closed", gate.OnQuery(ctx, fresh("SELECT * FROM db3.c", "db3.c")), refused("db3.c"))
	gate.OnQueryComplete(ctx, sess)
	reads.ServeUntil("db2", granted.Add(2*ttl-margin))
	check(t, "db2 after a renewal moved its fence", gate.OnQuery(ctx, fresh("SELECT * FROM db2.a", "db2.a")), ok)
	gate.OnQueryComplete(ctx, sess)
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("in flight = %v, want empty", got)
	}
}

// TestReplicaReadAdmissionNeverOverlapsARelease is clarification K44 under
// concurrency: sessions plan against the current snapshot, yield, and then
// reach the gate, while a host stops serving db2, waits until no read of db2
// is in flight and releases the lease. No read planned on a snapshot that
// serves db2 is admitted after that release. Each of the host's two signals
// is raced alone: a host that publishes a snapshot without db2's Replica
// entries and leaves db2's fence open (a read planned later finds db2
// Ordinary, which the rewriter sends to its owner), and a host whose refresh
// is blocked, so its snapshot keeps serving db2, that closes db2's fence
// (review round 2). Run it with -race.
func TestReplicaReadAdmissionNeverOverlapsARelease(t *testing.T) {
	ctx := context.Background()
	for _, host := range []struct {
		name string
		stop func(state *sitable.Fake, reads *sitable.ReplicaReads)
	}{
		{"publishes the drop", func(state *sitable.Fake, _ *sitable.ReplicaReads) { state.Set(replicaTables()[0], replicaTables()[3]) }},
		{"closes the fence", func(_ *sitable.Fake, reads *sitable.ReplicaReads) { reads.ServeUntil("db2", time.Time{}) }},
	} {
		for round := range 50 {
			state := replicaState()
			reads := servingReads()
			gate := &Plugin{ReplicaReads: reads, TableState: state}
			var released atomic.Bool
			var late atomic.Int64
			var wg sync.WaitGroup
			for i := range 8 {
				sess := newSession(t, int64(1000+round*8+i))
				wg.Go(func() {
					for range 20 {
						qctx := replicaQuery(sess, sqlmeta.StatementTypeSelect, "SELECT * FROM db2.a", accessed("db2.a"))
						qctx.TableSnapshot = state.Current()
						fromCopy := qctx.TableSnapshot.Lookup("db2", "a").Status == sitable.Replica
						runtime.Gosched()
						if gate.OnQuery(ctx, qctx) == nil && fromCopy {
							if released.Load() {
								late.Add(1)
							}
							runtime.Gosched()
						}
						gate.OnQueryComplete(ctx, sess)
					}
				})
			}
			runtime.Gosched()
			host.stop(state, reads)
			for reads.InFlight("db2") != 0 {
				runtime.Gosched()
			}
			released.Store(true)
			wg.Wait()
			if n := late.Load(); n != 0 {
				t.Fatalf("%s, round %d: %d reads of db2 admitted after the host released its lease", host.name, round, n)
			}
		}
	}
}
