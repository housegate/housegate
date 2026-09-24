package sitablestate

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

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
	alterErr = func(id string) outcome {
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
		{"CTAS into a refused name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.r ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a", accessed("db1.r"), ok},
		{"CTAS EMPTY into a pending name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.p ENGINE = MergeTree ORDER BY a EMPTY AS SELECT 1 AS a", accessed("db1.p"), ok},
		{"CTAS reading a pending table", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o ENGINE = Memory AS SELECT * FROM db1.p", accessed("db1.o", "db1.p"), pendingErr("db1.p")},
		{"schema clone of a pending table", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.o AS db1.p", accessed("db1.o", "db1.p"), ok},
		{"MV POPULATE into a pending name", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.p ENGINE = Memory POPULATE AS SELECT a FROM db1.o", accessed("db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO a pending table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.p AS SELECT a FROM db1.o", accessed("db1.mv", "db1.p", "db1.o"), withDataErr("db1.p")},
		{"MV TO an active table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.a AS SELECT a FROM db1.o", accessed("db1.mv", "db1.a", "db1.o"), withDataErr("db1.a")},
		{"MV TO a refused table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.r AS SELECT a FROM db1.o", accessed("db1.mv", "db1.r", "db1.o"), refusedErr("db1.r")},
		{"MV TO an ordinary table", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT a FROM db1.o", accessed("db1.mv", "db1.o"), ok},
		{"MV reading a pending source", sqlmeta.StatementTypeCreateMaterializedView, "CREATE MATERIALIZED VIEW db1.mv ENGINE = Memory AS SELECT a FROM db1.p", accessed("db1.mv", "db1.p"), pendingErr("db1.p")},
		{"view over a gone table", sqlmeta.StatementTypeCreateView, "CREATE VIEW db1.v AS SELECT a FROM db1.g", accessed("db1.v", "db1.g"), unknownErr("db1.g")},
		{"CTAS into a gone name", sqlmeta.StatementTypeCreateTable, "CREATE TABLE db1.g ENGINE = Memory AS SELECT 1 AS a", accessed("db1.g"), purgingErr("db1.g")},
	} {
		check(t, tc.name, run(t, tc.typ, tc.sql, tc.tables), tc.want)
	}
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
