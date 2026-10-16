package sireserved

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

func newSessionForTest(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

func TestOnQuery_OperatorSessionsRefuseReservedNamespace(t *testing.T) {
	for _, role := range []struct {
		name string
		set  func(*chsession.SessionState)
	}{
		{"maintenance", func(state *chsession.SessionState) { state.SetMaintenance(true) }},
		{"platform operator", func(state *chsession.SessionState) { state.SetPlatformOperator(true) }},
	} {
		for _, query := range []struct {
			name    string
			sql     string
			wantErr string
		}{
			{"safe database", "SELECT * FROM hg_safe.db1__t", "hg_safe"},
			{"unsafe database", "INSERT INTO hg_unsafe.db1__t VALUES (1)", "hg_unsafe"},
			{"reserved column", "SELECT _hg_row_id FROM db1.t", "_hg_row_id"},
		} {
			t.Run(role.name+"/"+query.name, func(t *testing.T) {
				p := &Plugin{
					ReservedDatabases:   []string{"hg_safe", "hg_unsafe"},
					ReservedRowIDColumn: "_hg_row_id",
				}
				sess := newSessionForTest(t, 1)
				role.set(sess.State())
				qctx := &plugin.QueryContext{
					Session:     sess,
					OriginalSQL: query.sql,
					Query:       &chproto.Query{Body: query.sql},
				}
				err := p.OnQuery(context.Background(), qctx)
				if err == nil {
					t.Fatal("operator-bypassed session must be refused on a reserved name")
				}
				if !strings.Contains(err.Error(), query.wantErr) {
					t.Fatalf("error must name %q: %v", query.wantErr, err)
				}
			})
		}
	}
}

func TestOnQuery_RefusesOnForwardedOperatorSession(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe"}, ReservedRowIDColumn: "_hg_row_id"}
	if !p.RunOnForward() || !p.RunOnPeerTrust() {
		t.Fatal("the guard must opt into both chain filters")
	}
	if !p.RejectUndecodableQuery() {
		t.Fatal("an undecodable query cannot be scanned and must fail closed")
	}
	for _, setRole := range []func(*chsession.SessionState){
		func(state *chsession.SessionState) { state.SetMaintenance(true) },
		func(state *chsession.SessionState) { state.SetPlatformOperator(true) },
	} {
		sess := newSessionForTest(t, 2)
		setRole(sess.State())
		sess.State().SetForwarding(true)
		const sql = "SELECT * FROM hg_safe.db1__t"
		qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
		chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
		if err := chain.OnQuery(context.Background(), qctx); err == nil {
			t.Fatal("a forwarded operator session must still be refused")
		}
	}
}

func TestOnQuery_OperatorBypassRefusesIdentifierPlaceholders(t *testing.T) {
	if !proto.FeatureParameters.In(54459) || proto.FeatureParameters.In(54458) {
		t.Fatal("test revisions no longer straddle ClickHouse FeatureParameters")
	}

	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
	}{
		{
			name: "maintenance",
			set:  func(state *chsession.SessionState) { state.SetMaintenance(true) },
		},
		{
			name: "platform operator",
			set:  func(state *chsession.SessionState) { state.SetPlatformOperator(true) },
		},
		{
			name: "forwarded maintenance",
			set: func(state *chsession.SessionState) {
				state.SetMaintenance(true)
				state.SetForwarding(true)
			},
		},
		{
			name: "peer-trusted platform operator",
			set: func(state *chsession.SessionState) {
				state.SetPlatformOperator(true)
				state.SetPeerTrust("10.0.0.7:9000")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
			sess := newSessionForTest(t, 20)
			sess.State().ClientRevision = 54459
			tc.set(sess.State())
			const sql = "SELECT * FROM {db:Identifier}.db1__t"
			qctx := &plugin.QueryContext{
				Session:     sess,
				OriginalSQL: sql,
				Query: &chproto.Query{
					Body:       sql,
					Parameters: []proto.Parameter{{Key: "db", Value: "hg_safe"}},
				},
			}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			err := chain.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "Identifier placeholder") {
				t.Fatalf("privileged Identifier placeholder must be refused, err=%v", err)
			}
		})
	}
}

func TestOnQuery_IdentifierPlaceholderTransportAndDecoyRules(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}

	for _, tc := range []struct {
		name     string
		revision int
		query    *chproto.Query
		wantErr  bool
	}{
		{
			name:     "serialized param setting transport",
			revision: 54458,
			query: &chproto.Query{
				Body:     "SELECT * FROM {db:Identifier}.db1__t",
				Settings: []proto.Setting{{Key: "param_db", Value: "hg_safe"}},
			},
			wantErr: true,
		},
		{
			name:     "old numeric setting transport is conservatively refused",
			revision: 54428,
			query: &chproto.Query{
				Body:        "SELECT * FROM {db:Identifier}.db1__t",
				OldSettings: []chproto.OldSetting{{Key: "param_db", Value: 1}},
			},
			wantErr: true,
		},
		{
			name:     "String placeholder is harmless",
			revision: 54459,
			query: &chproto.Query{
				Body:       "SELECT {value:String}",
				Parameters: []proto.Parameter{{Key: "value", Value: "hg_safe"}},
			},
		},
		{
			name:     "Identifier-shaped text in literal and comments is harmless",
			revision: 54459,
			query: &chproto.Query{
				Body:       "SELECT '{db:Identifier}' AS s /* {other:Identifier} */ -- {last:Identifier}\n",
				Parameters: []proto.Parameter{{Key: "db", Value: "hg_safe"}},
			},
		},
		{
			name:     "literal and comment cannot hide a real placeholder",
			revision: 54459,
			query: &chproto.Query{
				Body:       "SELECT '{fake:Identifier}', * FROM {db:Identifier}.db1__t /* {other:Identifier} */",
				Parameters: []proto.Parameter{{Key: "db", Value: "hg_safe"}},
			},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 21)
			sess.State().ClientRevision = tc.revision
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: tc.query.Body, Query: tc.query}
			err := p.OnQuery(context.Background(), qctx)
			if tc.wantErr && (err == nil || !strings.Contains(err.Error(), "Identifier placeholder")) {
				t.Fatalf("Identifier placeholder must be refused, err=%v", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("harmless parameter syntax must pass: %v", err)
			}
		})
	}
}

// TestOnQuery_IdentifierPlaceholderLeavesOrdinarySessionsAlone: an ordinary
// session and a session forwarded from a peer keep the D6 boundary; the
// receiving host's tablerefguard (G4) and the engines own their placeholders.
// A plain peer loopback is no longer exempt (spec 2026-10-14 §14):
// TestOnQuery_PeerSessionsRefuseIdentifierPlaceholders.
func TestOnQuery_IdentifierPlaceholderLeavesOrdinarySessionsAlone(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
	}{
		{name: "ordinary", set: func(*chsession.SessionState) {}},
		{name: "forwarded from a peer", set: func(state *chsession.SessionState) { state.SetPeerTrustForwarded("10.0.0.7:9000", true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 22)
			sess.State().ClientRevision = 54459
			tc.set(sess.State())
			const sql = "SELECT * FROM {db:Identifier}.db1__t"
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{
				Body: sql, Parameters: []proto.Parameter{{Key: "db", Value: "hg_safe"}},
			}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			if err := chain.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("non-operator, non-loopback sessions must stay out of scope: %v", err)
			}
		})
	}
}

func TestOnQuery_OperatorBypassRefusesLiteralIdentifierChannels(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		sql  string
		want string
	}{
		{
			name: "maintenance merge database literal",
			set:  func(state *chsession.SessionState) { state.SetMaintenance(true) },
			sql:  "SELECT * FROM merge('hg_safe', '^db1__t$')",
			want: "hg_safe",
		},
		{
			name: "platform remote table literal",
			set:  func(state *chsession.SessionState) { state.SetPlatformOperator(true) },
			sql:  "SELECT * FROM remote('127.0.0.1:9000', 'hg_unsafe.db1__t')",
			want: "hg_unsafe",
		},
		{
			name: "forwarded maintenance remote table literal",
			set: func(state *chsession.SessionState) {
				state.SetMaintenance(true)
				state.SetForwarding(true)
			},
			sql:  "SELECT * FROM remote('127.0.0.1:9000', 'hg_safe.db1__t')",
			want: "hg_safe",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 23)
			tc.set(sess.State())
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: tc.sql, Query: &chproto.Query{Body: tc.sql}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			err := chain.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("literal identifier channel must be refused with %q, err=%v", tc.want, err)
			}
		})
	}
}

func TestOnQuery_OperatorBypassRefusesObjectCarrierCallables(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct {
		name    string
		sql     string
		carrier string
	}{
		{"remote computed target", "SELECT * FROM remote('host', concat('hg_', 'safe', '.db1__t'))", "remote"},
		{"remoteSecure", "SELECT * FROM remoteSecure('host', 'other.u')", "remoteSecure"},
		{"cluster computed target", "SELECT * FROM cluster('c', concat('hg_', 'safe'), 'db1__t')", "cluster"},
		{"clusterAllReplicas", "SELECT * FROM clusterAllReplicas('c', 'other', 'u')", "clusterAllReplicas"},
		{"merge computed target", "SELECT * FROM merge(concat('hg_', 'safe'), '^db1__t$')", "merge"},
		{"loop", "SELECT * FROM loop('other.u')", "loop"},
		{"dictionary", "SELECT * FROM dictionary('other.dict')", "dictionary"},
		{"timeSeriesData", "SELECT * FROM timeSeriesData(other.ts)", "timeSeriesData"},
		{"timeSeriesTags", "SELECT * FROM timeSeriesTags(other.ts)", "timeSeriesTags"},
		{"timeSeriesMetrics", "SELECT * FROM timeSeriesMetrics(other.ts)", "timeSeriesMetrics"},
		{"timeSeriesSelector", "SELECT * FROM timeSeriesSelector(other.ts, 'x', 0, 1)", "timeSeriesSelector"},
		{"prometheusQuery", "SELECT * FROM prometheusQuery(other.ts, 'x', 1)", "prometheusQuery"},
		{"prometheusQueryRange", "SELECT * FROM prometheusQueryRange(other.ts, 'x', 1, 2, 3)", "prometheusQueryRange"},
		{"mergeTree prefix", "SELECT * FROM mergeTreeCodecBlockCounts(concat('hg_', 'safe'), 'db1__t')", "mergeTreeCodecBlockCounts"},
		{"Remote table engine", "CREATE TABLE other.x (a UInt64) ENGINE = Remote('host', 'other', 'u')", "Remote"},
		{"Distributed table engine", "CREATE TABLE other.x (a UInt64) ENGINE = Distributed('c', 'other', 'u')", "Distributed"},
		{"Merge table engine", "CREATE TABLE other.x (a UInt64) ENGINE = Merge('other', '^u$')", "Merge"},
		{"Buffer table engine", "CREATE TABLE other.x (a UInt64) ENGINE = Buffer('other', 'u', 1, 1, 1, 1, 1, 1, 1)", "Buffer"},
		{"dictionary CLICKHOUSE source", "CREATE DICTIONARY other.d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(DB 'other' TABLE 'u'))", "CLICKHOUSE"},
		// Foreign-connector family (Spec N D4). PRE-FIX: each row returns
		// err=<nil>, because concat('hg_', 'safe') never appears as the single
		// identifier hg_safe on either surface.
		{"mysql computed target", "SELECT * FROM mysql('h', concat('hg_', 'safe'), 'db1__t', 'u', 'p')", "mysql"},
		{"postgresql computed target", "SELECT * FROM postgresql('h:5432', concat('hg_', 'safe'), 'db1__t', 'u', 'p')", "postgresql"},
		{"mongodb computed target", "SELECT * FROM mongodb('h:27017', concat('hg_', 'safe'), 'db1__t', 'u', 'p', 'a UInt32')", "mongodb"},
		{"jdbc computed target", "SELECT * FROM jdbc('ds', concat('hg_', 'safe'), 'db1__t')", "jdbc"},
		{"odbc computed target", "SELECT * FROM odbc('ds', concat('hg_', 'safe'), 'db1__t')", "odbc"},
		// Heredoc arguments do not escape the carrier refusal. A heredoc body
		// is blanked from outsideLiterals, so the reserved name is not visible
		// there -- exactly as concat('hg_', 'safe') is not -- and the callable
		// name is what carries the refusal. PRE-FIX and post-fix: refused.
		// These rows exist so a future "inspect carrier arguments" refactor
		// cannot quietly reopen the concatenation bypass.
		{"merge with concatenated heredoc arguments", "SELECT * FROM merge($$hg$$||$$_safe$$, '^db1__t$')", "merge"},
		{"merge with heredoc concat() arguments", "SELECT * FROM merge(concat($$hg_$$, $$safe$$), '^db1__t$')", "merge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 24)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: tc.sql, Query: &chproto.Query{Body: tc.sql}}
			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.carrier)) {
				t.Fatalf("object-carrier callable %q must be refused, err=%v", tc.carrier, err)
			}
		})
	}
}

func TestOnQuery_ObjectCarrierScanAvoidsNonCallableFalsePositives(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, sql := range []string{
		"SELECT remote, cluster, merge FROM ordinary.t",
		"SELECT 'remote(other.u)'",
		"SELECT 1 /* cluster('c', 'other', 'u') */",
		"SELECT myRemote('other.u')",
		"SELECT concat('hg_', 'safe')",
		"SELECT mysql, jdbc FROM ordinary.t",
		// sqlite() and redis() are deliberately outside the carrier list: their
		// second argument is a file-local table and a column name respectively,
		// so neither can name a ClickHouse namespace (plan deviation D-2).
		// These rows exist so the exclusion cannot be "tidied up" back in.
		"SELECT * FROM sqlite('/tmp/x.db', 'db1__t')",
		"SELECT * FROM redis('h:6379', 'k', 'a UInt32')",
	} {
		sess := newSessionForTest(t, 25)
		sess.State().SetMaintenance(true)
		qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("non-carrier SQL %q must pass: %v", sql, err)
		}
	}
}

// TestOnQuery_LeavesOrdinarySessionsAlone: an operator session reading an
// ordinary table passes, and an ordinary session or one forwarded from a peer
// is out of scope even when it names a reserved database (tablerefguard and
// the engines refuse it there). A plain peer loopback naming one is refused:
// TestOnQuery_PeerSessionsRefuseReservedNamespace.
func TestOnQuery_LeavesOrdinarySessionsAlone(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe"}, ReservedRowIDColumn: "_hg_row_id"}

	sess := newSessionForTest(t, 3)
	sess.State().SetMaintenance(true)
	const ordinary = "SELECT * FROM other.u"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: ordinary, Query: &chproto.Query{Body: ordinary}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("ordinary table must pass: %v", err)
	}

	const reserved = "SELECT * FROM hg_safe.db1__t"
	plain := newSessionForTest(t, 4)
	forwarded := newSessionForTest(t, 5)
	forwarded.State().SetPeerTrustForwarded("10.0.0.7:9000", true)
	for name, s := range map[string]chsession.Session{"ordinary": plain, "forwarded from a peer": forwarded} {
		pctx := &plugin.QueryContext{Session: s, OriginalSQL: reserved, Query: &chproto.Query{Body: reserved}}
		chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
		if err := chain.OnQuery(context.Background(), pctx); err != nil {
			t.Fatalf("%s session is out of scope for sireserved: %v", name, err)
		}
	}
}

func TestReservedNamespaceViolation_RefusesOnMention(t *testing.T) {
	const rid = "_hg_row_id"
	dbs := []string{"hg_safe", "hg_unsafe"}

	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"qualifier", "SELECT * FROM hg_safe.db1__t", "hg_safe"},
		{"backtick qualifier", "SELECT * FROM `hg_safe`.`db1__t`", "hg_safe"},
		{"double-quoted qualifier", `SELECT * FROM "hg_safe"."db1__t"`, "hg_safe"},
		{"uppercase", "SELECT * FROM HG_SAFE.db1__t", "hg_safe"},
		{"truncate database", "TRUNCATE DATABASE hg_safe", "hg_safe"},
		{"drop database", "DROP DATABASE IF EXISTS hg_safe", "hg_safe"},
		{"create database", "CREATE DATABASE IF NOT EXISTS hg_safe", "hg_safe"},
		{"use", "USE hg_safe", "hg_safe"},
		{"show tables", "SHOW TABLES FROM hg_safe", "hg_safe"},
		{"truncate all", "TRUNCATE ALL TABLES FROM hg_unsafe", "hg_unsafe"},
		{"reserved column", "SELECT _hg_row_id FROM db1.t", rid},
		{"reserved column quoted", "SELECT `_hg_row_id` FROM db1.t", rid},
		{"line comment before", "USE -- pick one\nhg_safe", "hg_safe"},
		{"hash comment before", "USE # pick one\nhg_safe", "hg_safe"},
		{"shebang comment before", "USE #! pick one\nhg_safe", "hg_safe"},
		{"slash comment before", "USE // pick one\nhg_safe", "hg_safe"},
		{"block comment before", "SHOW TABLES FROM /* pick one */ hg_safe", "hg_safe"},
		{"nested block comment", "SHOW TABLES FROM /* a /* b */ c */ hg_safe", "hg_safe"},
		{"implicit alias position", "SELECT database hg_safe FROM ordinary.t", "hg_safe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, dbs, rid)
			if err != nil {
				t.Fatalf("unexpected scrub error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

func TestReservedNamespaceViolation_LiteralsAndCommentsAreNotMentions(t *testing.T) {
	const rid = "_hg_row_id"
	dbs := []string{"hg_safe", "hg_unsafe"}

	for _, tc := range []struct{ name, sql string }{
		{"ordinary single-quoted literal", "SELECT 'ordinary' AS s FROM ordinary.t"},
		{"line comment", "SELECT a FROM ordinary.t -- hg_safe"},
		{"hash comment", "SELECT a FROM ordinary.t # hg_safe"},
		{"slash comment", "SELECT a FROM ordinary.t // hg_safe"},
		{"block comment", "SELECT a FROM /* hg_safe */ ordinary.t"},
		{"nested block comment", "SELECT a FROM /* x /* hg_safe */ y */ ordinary.t"},
		{"prefix", "SELECT * FROM hg_safe_backup.t"},
		{"suffix", "SELECT * FROM my_hg_safe.t"},
		{"literal prefix", "SELECT 'hg_safe_backup'"},
		{"literal suffix", "SELECT 'my_hg_safe'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, dbs, rid)
			if err != nil {
				t.Fatalf("unexpected scrub error: %v", err)
			}
			if got != "" {
				t.Fatalf("must not fire on %q, got %q", tc.sql, got)
			}
		})
	}
}

func TestReservedNamespaceViolation_StringLiteralIdentifierTokensAreMentions(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"database literal", "SELECT 'hg_safe'", "hg_safe"},
		{"case-insensitive database literal", "SELECT 'HG_UNSAFE.db1__t'", "hg_unsafe"},
		{"reserved row id literal", "SELECT '_hg_row_id'", "_hg_row_id"},
		{"doubled quote does not hide token", "SELECT 'it''s hg_safe'", "hg_safe"},
		{"merge identifier channel", "SELECT * FROM merge('hg_safe', '^db1__t$')", "hg_safe"},
		{"remote identifier channel", "SELECT * FROM remote('127.0.0.1:9000', 'hg_unsafe.db1__t')", "hg_unsafe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if err != nil || got != tc.want {
				t.Fatalf("literal identifier token = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestReservedNamespaceViolation_BackslashBearingLiteralIsRefused(t *testing.T) {
	for _, sql := range []string{
		`SELECT 'ordinary\\path'`,
		`SELECT * FROM merge('hg\x5Fsafe', '^db1__t$')`,
		`SELECT '\x5Fhg\x5Frow\x5Fid'`,
		`SELECT 'a\' hg_safe b'`,
	} {
		if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id"); err == nil || !strings.Contains(err.Error(), "backslash") {
			t.Fatalf("backslash-bearing literal must be refused: sql=%q err=%v", sql, err)
		}
	}
}

func TestReservedNamespaceViolation_UnterminatedInputIsAnError(t *testing.T) {
	for _, sql := range []string{
		"SELECT 'unterminated FROM ordinary.t",
		"SELECT a FROM ordinary.t /* unterminated",
		"SELECT a FROM ordinary.t /* outer /* inner */ still open",
		"SELECT * FROM `unterminated",
		`SELECT * FROM "unterminated`,
	} {
		if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id"); err == nil {
			t.Fatalf("unterminated input must be an error: %q", sql)
		}
	}
}

func TestReservedNamespaceViolation_EscapedQuotedIdentifierIsRefused(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM `hg\\x5Fsafe`.t",
		`SELECT * FROM "hg\x5Fsafe".t`,
		"SELECT `\\x5Fhg\\x5Frow\\x5Fid` FROM db1.t",
		`SELECT "\x5Fhg\x5Frow\x5Fid" FROM db1.t`,
		"SELECT * FROM `hg\\u005Fsafe`.t",
		"SELECT * FROM `ordinary\\x5Ftable`.t",
	} {
		if !strings.Contains(sql, `\`) {
			t.Fatalf("case lost its backslash: %q", sql)
		}
		if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id"); err == nil {
			t.Fatalf("an escaped quoted identifier must be refused: %q", sql)
		}
	}
}

func TestReservedNamespaceViolation_PlainQuotedIdentifiersResolve(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM `hg_safe`.`t`",
		`SELECT * FROM "hg_safe"."t"`,
	} {
		got, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id")
		if err != nil || got != "hg_safe" {
			t.Fatalf("plain quoted identifier %q = %q, %v", sql, got, err)
		}
	}
	for _, sql := range []string{
		"SELECT * FROM `ordinary`.`t`",
		`SELECT * FROM "ordinary"."t"`,
	} {
		if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id"); err != nil {
			t.Fatalf("unrelated quoted identifier %q: %v", sql, err)
		}
	}
}

func TestReservedNamespaceViolation_OrdinaryColumnIsRefusedByDesign(t *testing.T) {
	got, err := ReservedNamespaceViolation("SELECT hg_safe FROM ordinary.t", []string{"hg_safe"}, "_hg_row_id")
	if err != nil || got != "hg_safe" {
		t.Fatalf("mention-is-the-rule = %q, %v", got, err)
	}
}

// TestReservedNamespaceViolation_HeredocCannotHideAComment is the Spec N 1a
// regression. ClickHouse's heredoc ($$...$$ / $tag$...$tag$) is a string
// literal, so a --, # or // inside one is content, not a comment. The shipped
// scanner copied `$` through its default branch, so consumeLineComment blanked
// the rest of the statement from both surfaces and the guard saw nothing.
func TestReservedNamespaceViolation_HeredocCannotHideAComment(t *testing.T) {
	const rid = "_hg_row_id"
	dbs := []string{"hg_safe", "hg_unsafe"}
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// PRE-FIX: all four return "" (no violation). Reproduced at
		// scratchpad/heredoc-repro, 6/8 statements passed the guard.
		{"bare heredoc hiding a line comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		// reservedNamespaceViolationOnSurface reports the FIRST reserved name on
		// the surface, and this reproduction statement mentions two: the
		// _hg_row_id column precedes hg_unsafe. The next row isolates the
		// database so both reserved names stay pinned.
		{"bare heredoc hiding a hash comment", "SELECT $$#$$ AS x, _hg_row_id FROM hg_unsafe.db1__t", rid},
		{"bare heredoc hiding a hash comment, database only", "SELECT $$#$$ AS x, count() FROM hg_unsafe.db1__t", "hg_unsafe"},
		{"heredoc in an INSERT ... SELECT export", "INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t", "hg_safe"},
		{"tagged heredoc hiding a slash comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		// PRE-FIX: already refused. Controls that must not regress.
		{"control: no heredoc", "SELECT count() FROM hg_safe.db1__t", "hg_safe"},
		{"control: reserved column", "SELECT _hg_row_id FROM db1.t", rid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, dbs, rid)
			if err != nil {
				t.Fatalf("unexpected scan error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocBodyReachesTheLiteralSurface is the
// half that is easy to break while fixing the half above. ClickHouse table
// functions read literal arguments as identifiers, so a heredoc body must be
// blanked from outsideLiterals and written VERBATIM to withLiterals. These
// statements are refused TODAY (the body carries no comment marker, so hg_safe
// survives as ordinary bytes); a fix that blanks heredoc bodies from both
// surfaces would turn a currently-caught statement into a bypass.
func TestReservedNamespaceViolation_HeredocBodyReachesTheLiteralSurface(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		// PRE-FIX: already refused. MUST still be refused after the fix.
		{"merge database heredoc", "SELECT * FROM merge($$hg_safe$$, '^db1__t$')", "hg_safe"},
		// PRE-FIX: refused. Post-fix the tagged body must resolve to hg_safe too.
		{"tagged merge database heredoc", "SELECT * FROM merge($tag$hg_safe$tag$, '^db1__t$')", "hg_safe"},
		{"reserved column in a heredoc", "SELECT $$_hg_row_id$$", "_hg_row_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if err != nil || got != tc.want {
				t.Fatalf("heredoc body on the literal surface = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocIsNotABlanketRefusal keeps the fix from
// degenerating into "any $ is an error": an ordinary heredoc naming nothing
// reserved must still pass, or every maintenance session that uses one breaks.
func TestReservedNamespaceViolation_HeredocIsNotABlanketRefusal(t *testing.T) {
	for _, sql := range []string{
		"SELECT $$ordinary$$ AS x FROM other.u",
		"SELECT $tag$ordinary$tag$ AS x FROM other.u",
		"SELECT $$hg_safe_backup$$ AS x FROM other.u",
		"SELECT $$$$ AS empty FROM other.u",
		"SELECT $$a$$, $$b$$ FROM other.u",
		// No escape processing inside a heredoc: measured on the live v0.9.0
		// polyglot grammar, merge($$hg\x5Fsafe$$, ...) comes back Success and
		// re-emits as merge('hg\\x5Fsafe', ...), so the body is the literal
		// text hg\x5Fsafe and is NOT hg_safe. Heredoc bodies therefore do not
		// inherit the backslash refusal consumeStringLiteral applies to '...'.
		`SELECT $$hg\x5Fsafe$$ AS x FROM other.u`,
	} {
		t.Run(sql, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if err != nil {
				t.Fatalf("unexpected scan error: %v", err)
			}
			if got != "" {
				t.Fatalf("must not fire on %q, got %q", sql, got)
			}
		})
	}
}

// TestReservedNamespaceViolation_UnterminatedOrStrayDollarIsRefused pins the
// two remaining rows of the reproduction. An unterminated heredoc is an error
// like an unterminated literal or block comment; a `$` that opens nothing is
// refused, because outside a heredoc opener and a quoted span `$` is not part
// of any identifier or operator this guard needs to admit, and copying it
// through is exactly what produced the bypass (Spec N D1).
func TestReservedNamespaceViolation_UnterminatedOrStrayDollarIsRefused(t *testing.T) {
	for _, sql := range []string{
		// PRE-FIX: all return "" (no violation).
		"SELECT $$unterminated",
		"SELECT $tag$unterminated$tog$",
		"SELECT 1 $ 2",
		"SELECT $ FROM other.u",
		"SELECT $1 FROM other.u",
		// A well-formed heredoc followed by an unterminated opener: the guard
		// refuses the whole statement rather than guessing where the second
		// span ends.
		"SELECT $t$a$t$ AS x, $t$ FROM hg_safe.db1__t",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id"); err == nil {
				t.Fatalf("unterminated or stray $ must be an error: %q", sql)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocOpenerCharsetIsNarrowerThanTheGrammar
// pins a deliberate divergence measured against the live v0.9.0 polyglot
// grammar. The grammar accepts heredoc tags this guard does not model — a
// leading digit ($1t$), a digits-only tag ($1$) and a non-ASCII tag ($ta$ with
// a multi-byte rune) all parse as heredocs and re-emit as ordinary quoted
// strings. Spec N D1 specifies the charset [A-Za-z_][0-9A-Za-z_]*, which is a
// strict SUBSET of that. Narrower is the safe direction: an opener the guard
// does not recognise falls to the stray-$ refusal, costing a false refusal
// rather than opening a span the guard would blank from the executable
// surface. Widening this charset past the grammar is what would create a new
// bypass, so these rows exist to make any such widening fail loudly.
func TestReservedNamespaceViolation_HeredocOpenerCharsetIsNarrowerThanTheGrammar(t *testing.T) {
	for _, sql := range []string{
		// PRE-FIX: all return "" (no violation), no error.
		"SELECT $1t$x$1t$ AS c FROM other.u",
		"SELECT $1$x$1$ AS c FROM other.u",
		"SELECT $tä$x$tä$ AS c FROM other.u",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id"); err == nil {
				t.Fatalf("a heredoc tag outside the guard's charset must be refused, not parsed: %q", sql)
			}
		})
	}
}

// TestReservedNamespaceViolation_HeredocSpanMatchesTheGrammar pins the span
// boundaries against behaviour measured on the live v0.9.0 grammar: the
// closing tag is matched byte-exactly (so $tag$x$TAG$ does not close), a lone
// `$` inside a body is content, and a heredoc opener outranks a single quote
// exactly as it outranks a comment marker.
func TestReservedNamespaceViolation_HeredocSpanMatchesTheGrammar(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		want    string
		wantErr bool
	}{
		// PRE-FIX: "" and no error. The closing tag is case-sensitive, so this
		// heredoc is never terminated.
		{"closing tag is case sensitive", "SELECT $tag$x$TAG$ AS c FROM other.u", "", true},
		// PRE-FIX: "" and no error -- `--` inside the body was read as a comment.
		{"lone dollar inside a body is content", "SELECT $t$--$x$t$ AS c, count() FROM hg_safe.db1__t", "hg_safe", false},
		// PRE-FIX: an error -- the ' inside the heredoc opened a string literal
		// that ran to the end of the statement. Post-fix the heredoc wins and
		// the trailing SQL is scanned.
		{"heredoc outranks a single quote", "SELECT $$'$$ AS x, count() FROM hg_safe.db1__t", "hg_safe", false},
		// PRE-FIX and post-fix: a comment outranks a heredoc opener, so the
		// blanked tail is genuinely not executed by ClickHouse either.
		{"comment outranks a heredoc opener", "SELECT 1 FROM other.u -- $$hg_safe$$", "", false},
		// PRE-FIX and post-fix: a single quote outranks a heredoc opener.
		{"single quote outranks a heredoc opener", "SELECT '$$' AS x, count() FROM hg_safe.db1__t", "hg_safe", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(tc.sql, []string{"hg_safe", "hg_unsafe"}, "_hg_row_id")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("%q must be a scan error, got %q", tc.sql, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected scan error on %q: %v", tc.sql, err)
			}
			if got != tc.want {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestOnQuery_OperatorSessionRefusesHeredocHiddenReservedName drives the same
// statements through the plugin so the refusal is proved on the production
// path, including the error text an operator actually receives.
func TestOnQuery_OperatorSessionRefusesHeredocHiddenReservedName(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct{ name, sql, want string }{
		{"maintenance heredoc comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		{"maintenance tagged heredoc comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__t", "hg_safe"},
		{"maintenance heredoc export", "INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t", "hg_safe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 30)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: tc.sql, Query: &chproto.Query{Body: tc.sql}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			err := chain.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("heredoc-hidden reserved name must be refused with %q, err=%v", tc.want, err)
			}
		})
	}
}

// TestReservedNamespaceViolation_LineCommentEndsOnlyAtLineFeed closes a
// privileged-session bypass measured on ClickHouse 26.8: a line comment ends
// only at \n, so in "SELECT * -- c\r/*\nFROM hg_safe.t -- */" ClickHouse
// executes FROM hg_safe.t. The scanner used to end the comment at \r, open a
// block comment on the /* that ClickHouse treats as comment text, and blank
// the reserved name from both surfaces.
func TestReservedNamespaceViolation_LineCommentEndsOnlyAtLineFeed(t *testing.T) {
	for _, sql := range []string{
		"SELECT * -- c\r/*\nFROM hg_safe.t -- */",
		"SELECT * # c\r'\nFROM hg_safe.t # '",
		"SELECT * #! c\r`\nFROM hg_safe.t #! `",
		"SELECT * // c\r$$\nFROM hg_safe.t // $$",
	} {
		t.Run(sql, func(t *testing.T) {
			got, err := ReservedNamespaceViolation(sql, []string{"hg_safe"}, "_hg_row_id")
			if err != nil || got != "hg_safe" {
				t.Fatalf("ReservedNamespaceViolation(%q) = %q, %v; want hg_safe", sql, got, err)
			}
		})
	}
	// A # that opens no comment is a ClickHouse syntax error; the guard
	// refuses it instead of blanking the rest of the line.
	if _, err := ReservedNamespaceViolation("SELECT 1 #x\n", []string{"hg_safe"}, "_hg_row_id"); err == nil {
		t.Fatal("a bare # must be refused")
	}
}

// TestOnQuery_CarrierCannotHideBehindACarriageReturnComment is the
// executable-surface half of the line-comment bypass: a quote ClickHouse
// reads as comment text used to blank a carrier call from OutsideLiterals.
func TestOnQuery_CarrierCannotHideBehindACarriageReturnComment(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	sql := "SELECT * -- c\r'\nFROM merge(currentDatabase(), 't') -- '"
	sess := newSessionForTest(t, 31)
	sess.State().SetMaintenance(true)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
	if err := chain.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "merge") {
		t.Fatalf("carrier hidden behind a CR-terminated comment must be refused, err=%v", err)
	}
}

// TestOnQuery_DollarInsideAnIdentifierCannotOpenAHeredoc closes a
// privileged-session bypass measured on ClickHouse 26.8: x$$ and y$$ are
// identifiers there, so "SELECT 1 AS x$$, * FROM merge(...) AS y$$" executes
// the merge() call, while the scanner read $$, * FROM merge(...) AS y$$ as a
// heredoc and saw no carrier, no placeholder and no complete reserved name.
func TestOnQuery_DollarInsideAnIdentifierCannotOpenAHeredoc(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, sql := range []string{
		"SELECT 1 AS x$$, * FROM merge(concat('hg_','safe'), '^t$') AS y$$",
		"SELECT 1 AS x$t$, * FROM {p:Identifier} AS y$t$",
		"SELECT 1 AS x$$, * FROM hg_safe.t AS y$$",
	} {
		t.Run(sql, func(t *testing.T) {
			sess := newSessionForTest(t, 32)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			if err := chain.OnQuery(context.Background(), qctx); err == nil {
				t.Fatalf("a $ inside an identifier must not hide %q", sql)
			}
		})
	}
}

// TestOnQuery_UnicodeWhitespaceCannotHideACarrier closes a privileged-session
// bypass measured on ClickHouse 26.8: Unicode whitespace separates tokens
// there, so "merge<U+00A0>(...)" calls merge(), while the carrier scan only
// skipped ASCII whitespace before "(". Every non-ASCII byte outside quotes,
// literals, comments and heredoc bodies is now refused by the scanner.
func TestOnQuery_UnicodeWhitespaceCannotHideACarrier(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, sql := range []string{
		"SELECT count() FROM merge\u00a0(currentDatabase(), '^t$')",
		"SELECT count() FROM merge\u200b(currentDatabase(), '^t$')",
	} {
		t.Run(sql, func(t *testing.T) {
			sess := newSessionForTest(t, 33)
			sess.State().SetMaintenance(true)
			qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
			if err := chain.OnQuery(context.Background(), qctx); err == nil {
				t.Fatalf("a carrier behind Unicode whitespace must be refused: %q", sql)
			}
		})
	}
}

// TestOnQuery_RefusesEveryReservedDatabase is spec 2026-09-26 T11 and spec
// 2026-10-14 §14: the guard, built from sitable.ReservedDatabases() as build.go
// does, refuses each reserved database, hg_promote and the stage-3 member
// databases included, on an operator session (the operator message) and on a
// peer loopback (the peer 392).
func TestOnQuery_RefusesEveryReservedDatabase(t *testing.T) {
	p := &Plugin{ReservedDatabases: sitable.ReservedDatabases(), ReservedRowIDColumn: "_hg_row_id"}
	for _, db := range sitable.ReservedDatabases() {
		sql := "SELECT * FROM " + db + ".db1__t"
		sess := newSessionForTest(t, 3)
		sess.State().SetPlatformOperator(true)
		err := p.OnQuery(context.Background(), &plugin.QueryContext{
			Session:     sess,
			OriginalSQL: sql,
			Query:       &chproto.Query{Body: sql},
		})
		if err == nil || !strings.Contains(err.Error(), db) {
			t.Errorf("%s: err = %v, want a refusal naming the database", sql, err)
		}
		var ce *chproto.ClientError
		if errors.As(err, &ce) {
			t.Errorf("%s: the operator refusal must stay a plain error (403), got %+v", sql, ce)
		}
		err = runChain(context.Background(), p, peerQuery(t, sql, false))
		if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited || ce.Message != peerReservedMessage(db) {
			t.Errorf("%s on a peer loopback: err = %v, want the 392 naming %s", sql, err, db)
		}
	}
}

// peerGuard is the guard as build.go wires it.
func peerGuard() *Plugin {
	return &Plugin{ReservedDatabases: sitable.ReservedDatabases(), ReservedRowIDColumn: "_hg_row_id"}
}

// peerQuery returns a query context on a peer-trusted session: a rewriter
// remote() loopback when forwarded is false, a forward pivot otherwise.
func peerQuery(t *testing.T, sql string, forwarded bool) *plugin.QueryContext {
	t.Helper()
	sess := newSessionForTest(t, 40)
	sess.State().SetPeerTrustForwarded("10.0.0.7:9000", forwarded)
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
}

// runChain runs p through a one-plugin chain so its marker methods decide.
func runChain(ctx context.Context, p *Plugin, qctx *plugin.QueryContext) error {
	return (&plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}).OnQuery(ctx, qctx)
}

func peerReservedMessage(name string) string {
	return "storage_integrity: reserved name " + name + " is not addressable on a peer connection; the peer guard refuses any mention, string literals included"
}

func requirePeerRefusal(t *testing.T, err error, message string) {
	t.Helper()
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited || ce.Message != message {
		t.Fatalf("err = %v, want 392 %q", err, message)
	}
}

func captureLogs(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return log.WithContext(context.Background(), log.New(slog.NewTextHandler(&buf, nil))), &buf
}

// peerLegForms is the regression corpus of the SQL a storage-integrity host
// receives on a legitimate rewriter remote() loopback (peer-trusted, not
// forwarded): ClickHouse's secondary queries for the legs the origin's
// rewriter emits — rewriter-go nodes.go ActionRemote in FROM/JOIN and
// IN-operand position, dblevel.go SHOW TABLES of a remote logical database,
// and the structure probe ClickHouse sends because rewriter-go never passes a
// structure argument. Rows marked (measured) are ClickHouse 26.8.1 output
// pinned in sipeerguard's tests, rows marked (measured 25.8) are ClickHouse
// 25.8.28.1 output, and the others follow the same formatting.
// TestPeerReservedGuard_MeasuredRemoteSecondaryQueriesPass captures from
// ClickHouse itself every class one server emits; it cannot see the
// structure probe (ClickHouse reads a local shard's structure without
// sending it) or the bare IN-operand form (ClickHouse refuses it at the
// initiator). Every row except the system.tables probe names the physical
// database phys, and no other word contains "phys", so
// TestOnQuery_PeerLegFormsWithAReservedNameAreRefused can put a reserved
// database in its place.
var peerLegForms = map[string]string{
	// (a) The remote() structure probe; 25.8.28.1 writes the database bare
	// (measured against a second server).
	"structure probe":                 "DESC TABLE `phys`.`db1.t`",
	"structure probe (measured 25.8)": "DESC TABLE phys.`db1.t`",
	// (b) A table read pushed down to the leg.
	"pushed-down count (measured)":        "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1`",
	"bare physical database (measured)":   "SELECT `__table1`.`value` AS `value` FROM phys.`db1.t` AS `__table1`",
	"pushed-down filter":                  "SELECT `__table1`.`id` AS `id`, `__table1`.`s` AS `s` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`id` IN (1, 2)",
	"aggregation with GROUP BY and LIMIT": "SELECT `__table1`.`s` AS `s`, sum(`__table1`.`x`) AS `sum(x)` FROM `phys`.`db1.t` AS `__table1` GROUP BY `__table1`.`s` ORDER BY `__table1`.`s` ASC LIMIT 10",
	"uniq intermediate state":             "SELECT uniq(`__table1`.`s`) AS `uniq(s)` FROM `phys`.`db1.t` AS `__table1`",
	"FINAL and PREWHERE":                  "SELECT `__table1`.`id` AS `id` FROM `phys`.`db1.t` AS `__table1` FINAL PREWHERE `__table1`.`x` > 0.5",
	"pre-analyzer formatting":             "SELECT count() FROM phys.`db1.t`",
	"backslash alias (measured)":          "SELECT extract(`__table1`.`s`, '\\\\d+') AS `extract(s, '\\\\\\\\d+')` FROM `phys`.`db1.t` AS `__table1`",
	"newline alias (measured)":            "SELECT concat(`__table1`.`s`, '\\n') AS `concat(s, '\\\\n')` FROM `phys`.`db1.t` AS `__table1`",
	"quote alias (measured)":              "SELECT concat(`__table1`.`s`, 'it\\'s') AS `concat(s, 'it\\\\'s')` FROM `phys`.`db1.t` AS `__table1`",
	"decimal alias (measured)":            "SELECT avg(`__table1`.`x` * 1.5) AS `avg(multiply(x, 1.5))` FROM `phys`.`db1.t` AS `__table1`",
	"countIf alias (measured)":            "SELECT countIf(`__table1`.`x` > 0.5) AS `countIf(greater(x, 0.5))` FROM `phys`.`db1.t` AS `__table1`",
	"Nested column (measured)":            "SELECT `__table1`.`n.a` AS `n.a` FROM `phys`.`db1.t` AS `__table1`",
	"dotted alias (measured)":             "SELECT `__table1`.`s` AS `db1.alias` FROM `phys`.`db1.t` AS `__table1`",
	// A client literal the origin's tablerefguard G4 admits becomes alias text.
	"Identifier-shaped literal in an alias": "SELECT concat(`__table1`.`s`, '{p:Identifier}') AS `concat(s, '{p:Identifier}')` FROM `phys`.`db1.t` AS `__table1`",
	// (c) SHOW TABLES of a remote logical database.
	"SHOW TABLES structure probe": "DESC TABLE `system`.`tables`",
	"SHOW TABLES filtered read":   "SELECT `__table1`.`name` AS `name` FROM `system`.`tables` AS `__table1` WHERE (`__table1`.`database` = 'phys') AND startsWith(`__table1`.`name`, 'db1.')",
	// (d) GLOBAL IN / GLOBAL JOIN: the origin computes the local side, hg_safe
	// reads included, and ships it as an external table.
	"GLOBAL IN external table (analyzer)":     "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`id` GLOBAL IN (`_data_7020342823616227185_10950398713282440232`)",
	"GLOBAL IN external table (pre-analyzer)": "SELECT count() FROM phys.`db1.t` WHERE id GLOBAL IN _data1",
	"GLOBAL JOIN external table":              "SELECT `__table1`.`id` AS `id`, `__table2`.`v` AS `v` FROM `phys`.`db1.t` AS `__table1` GLOBAL ALL INNER JOIN `_data_7020342823616227185_10950398713282440232` AS `__table2` USING (`id`)",
	// The 25.8.28.1 analyzer aliases the external table and writes LIMIT as a
	// typed cast.
	"GLOBAL IN external table with an alias (measured 25.8)": "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`id` GLOBAL IN (`_data_12979155211854796489_16094174518441047580` AS `__table2`)",
	"LIMIT as a typed cast (measured 25.8)":                  "SELECT `__table1`.`s` AS `s`, sum(`__table1`.`x`) AS `sum(x)` FROM `phys`.`db1.t` AS `__table1` GROUP BY `__table1`.`s` ORDER BY `__table1`.`s` ASC LIMIT _CAST(10, 'UInt64')",
	// (e) A nested remote() carrier, whose literals name another host, the
	// physical database and the peer envelope: ClickHouse pushes a
	// remote()-left JOIN of another remote() and an IN subquery over another
	// remote() into the leg (measured on 25.8.28.1, arguments as the
	// rewriter renders them). rewriter-go's bare IN-operand form is refused by
	// ClickHouse 25.8 and 26.3 at the initiator; it stays admitted here so the
	// rule never depends on that refusal.
	"remote JOIN pushed into the leg (measured 25.8)":        "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` ALL INNER JOIN remote('10.0.0.5:9000', 'phys', 'db1.u', '__route__|10.0.0.5:9000|__peer__|0x5a1f0e07c3e5d2b6a7f8e9d0c1b2a3f4e5d6c7b8', 'eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiJwZWVyLWhlbGxvIn0.c2lnbmF0dXJlX2J5dGVz') AS `__table2` ON `__table1`.`id` = `__table2`.`id`",
	"remote IN subquery pushed into the leg (measured 25.8)": "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`id` IN (SELECT `__table1`.`id` AS `id` FROM remote('10.0.0.5:9000', 'phys', 'db1.u', '__route__|10.0.0.5:9000|__peer__|0x5a1f0e07c3e5d2b6a7f8e9d0c1b2a3f4e5d6c7b8', 'eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiJwZWVyLWhlbGxvIn0.c2lnbmF0dXJlX2J5dGVz') AS `__table1`)",
	"remote IN operand inside a remote outer":                "SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`id` IN remote('127.0.0.1:19001', 'phys', 'db1.l', '__route__|10.0.0.5:9000|__peer__|0x5a1f0e07c3e5d2b6a7f8e9d0c1b2a3f4e5d6c7b8', 'eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiJwZWVyLWhlbGxvIn0.c2lnbmF0dXJlX2J5dGVz')",
}

// TestOnQuery_PeerLegFormsPass is spec 2026-10-14 §14's regression rule: every
// legitimate peer leg passes the peer rules, through the chain and through
// PeerReservedNamespaceViolation.
func TestOnQuery_PeerLegFormsPass(t *testing.T) {
	p := peerGuard()
	for name, sql := range peerLegForms {
		t.Run(name, func(t *testing.T) {
			if err := runChain(context.Background(), p, peerQuery(t, sql, false)); err != nil {
				t.Fatalf("legitimate peer leg %q refused: %v", sql, err)
			}
			if got, err := PeerReservedNamespaceViolation(sql, sitable.ReservedDatabases(), "_hg_row_id"); err != nil || got != "" {
				t.Fatalf("PeerReservedNamespaceViolation(%q) = %q, %v; want no violation", sql, got, err)
			}
		})
	}
}

// TestOnQuery_PeerLegFormsWithAReservedNameAreRefused: the same shapes with a
// reserved database in the physical database's place, as identifier or as
// literal, and with the reserved row-id column, are refused with the 392 that
// names it.
func TestOnQuery_PeerLegFormsWithAReservedNameAreRefused(t *testing.T) {
	p := peerGuard()
	var withoutPhysical []string
	for name, sql := range peerLegForms {
		if !strings.Contains(sql, "phys") {
			withoutPhysical = append(withoutPhysical, name)
			continue
		}
		for _, db := range sitable.ReservedDatabases() {
			reserved := strings.ReplaceAll(sql, "phys", db)
			t.Run(name+"/"+db, func(t *testing.T) {
				requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, reserved, false)), peerReservedMessage(db))
			})
		}
	}
	sort.Strings(withoutPhysical)
	if strings.Join(withoutPhysical, ",") != "SHOW TABLES structure probe" {
		t.Fatalf("corpus rows without the physical database = %v; every other row must name phys", withoutPhysical)
	}
	for _, sql := range []string{
		"SELECT `__table1`.`_hg_row_id` AS `_hg_row_id` FROM `phys`.`db1.t` AS `__table1`",
		"SELECT count() AS `count()` FROM `phys`.`db1.t` AS `__table1` WHERE `__table1`.`_hg_row_id` > 0",
	} {
		t.Run(sql, func(t *testing.T) {
			requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, sql, false)), peerReservedMessage("_hg_row_id"))
		})
	}
}

// TestOnQuery_PeerSessionsRefuseReservedNamespace is the mention rule on a peer
// loopback for every reserved database, in every surface but comments: plain,
// quoted and upper-case identifiers, table-function literals, heredocs, an IN
// subquery, metadata literals, and escapes ClickHouse decodes.
func TestOnQuery_PeerSessionsRefuseReservedNamespace(t *testing.T) {
	p := peerGuard()
	for _, db := range sitable.ReservedDatabases() {
		escaped := strings.ReplaceAll(db, "_", `\x5F`)
		escapedLower := strings.ReplaceAll(db, "_", `\x5f`)
		for _, tc := range []struct{ name, sql string }{
			{"qualifier", "SELECT * FROM " + db + ".db1__t"},
			{"pushed-down leg", "SELECT count() AS `count()` FROM `" + db + "`.`db1__t` AS `__table1`"},
			{"double-quoted qualifier", `SELECT * FROM "` + db + `"."db1__t"`},
			{"upper case", "SELECT * FROM " + strings.ToUpper(db) + ".db1__t"},
			{"remote database literal", "SELECT count() FROM remote('127.0.0.1:9000', '" + db + "', 'db1__t')"},
			{"merge database literal", "SELECT * FROM merge('" + db + "', '^db1__t$')"},
			{"heredoc", "SELECT * FROM merge($$" + db + "$$, '^db1__t$')"},
			{"IN subquery", "SELECT count() FROM `phys`.`db1.t` WHERE id IN (SELECT id FROM " + db + ".db1__t)"},
			{"metadata literal", "SELECT name FROM system.tables WHERE database = '" + db + "'"},
			{"escaped literal", "SELECT * FROM merge('" + escaped + "', '^db1__t$')"},
			{"escaped quoted identifier", "SELECT * FROM `" + escapedLower + "`.db1__t"},
			{"USE", "USE " + db},
			{"INSERT", "INSERT INTO " + db + ".db1__t VALUES (1)"},
		} {
			t.Run(db+"/"+tc.name, func(t *testing.T) {
				requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, tc.sql, false)), peerReservedMessage(db))
			})
		}
	}
	for _, sql := range []string{
		"SELECT `__table1`.`_hg_row_id` FROM `phys`.`db1.t` AS `__table1`",
		"SELECT '_hg_row_id'",
		`SELECT '\x5Fhg\x5Frow\x5Fid'`,
	} {
		requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, sql, false)), peerReservedMessage("_hg_row_id"))
	}
	// ClickHouse reads \N as nothing, so 'hg_\Nsafe' is hg_safe.
	requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, `SELECT * FROM t WHERE s = 'hg_\Nsafe'`, false)), peerReservedMessage("hg_safe"))
}

// TestOnQuery_PeerSessionsRefuseIdentifierPlaceholders: every {name:Identifier}
// placeholder ClickHouse would parse is refused on a peer loopback, and text
// that only looks like one — a value placeholder, a literal, a heredoc, a
// quoted identifier (where ClickHouse writes projection aliases) or a
// comment — is not.
func TestOnQuery_PeerSessionsRefuseIdentifierPlaceholders(t *testing.T) {
	p := peerGuard()
	const want = "storage_integrity: a peer statement may not carry ClickHouse Identifier placeholders"
	for _, sql := range []string{
		"SELECT * FROM {db:Identifier}.db1__t",
		"SELECT * FROM { db : identifier }",
		"SELECT {c:Identifier} FROM `phys`.`db1.t`",
		"SELECT * FROM {db/* c */:Identifier}.t",
		"SELECT * FROM `phys`.{t:Identifier}",
	} {
		t.Run(sql, func(t *testing.T) {
			requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, sql, false)), want)
		})
	}
	for _, sql := range []string{
		"SELECT count() FROM `phys`.`db1.t` WHERE id = {v:UInt64}",
		"SELECT '{p:Identifier}'",
		"SELECT $${p:Identifier}$$",
		"SELECT 1 AS `{p:Identifier}`",
		"SELECT 1 /* {p:Identifier} */",
	} {
		t.Run(sql, func(t *testing.T) {
			if err := runChain(context.Background(), p, peerQuery(t, sql, false)); err != nil {
				t.Fatalf("%q is no placeholder: %v", sql, err)
			}
		})
	}
}

// TestOnQuery_PeerUnscannableStatementIsRefused: a statement the peer scan
// cannot model fails closed with the scan error in the message.
func TestOnQuery_PeerUnscannableStatementIsRefused(t *testing.T) {
	p := peerGuard()
	for _, sql := range []string{
		"SELECT 'x",
		"SELECT $ FROM `phys`.`db1.t`",
		`SELECT '\x4g'`,
		"SELECT * FROM `phys`.`db1\\x4gt`",
		"SELECT 1 #x\n",
		"SELECT 1 AS physé",
	} {
		t.Run(sql, func(t *testing.T) {
			_, scanErr := sqlsurface.ScanWith(sql, PeerScanOptions)
			if scanErr == nil {
				t.Fatalf("fixture %q must not scan", sql)
			}
			requirePeerRefusal(t, runChain(context.Background(), p, peerQuery(t, sql, false)),
				"storage_integrity: a peer statement that cannot be checked for reserved names is refused: "+scanErr.Error())
		})
	}
}

// TestOnQuery_ForwardedFromPeerSessionsAreNotPeerChecked: a forward pivot
// carries the client's own SQL, which the receiving host's tablerefguard and
// engines check; the peer rules do not apply to it.
func TestOnQuery_ForwardedFromPeerSessionsAreNotPeerChecked(t *testing.T) {
	p := peerGuard()
	for _, sql := range []string{
		"SELECT * FROM hg_safe.db1__t",
		"SELECT * FROM hg_replica.db1__t__i3",
		"SELECT * FROM {db:Identifier}.t",
		"SELECT 'x",
	} {
		if err := runChain(context.Background(), p, peerQuery(t, sql, true)); err != nil {
			t.Fatalf("forwarded-from-peer %q: %v, want nil", sql, err)
		}
	}
}

// TestOnQuery_PeerSessionWithAnOperatorFlagKeepsTheOperatorRules: the operator
// branch is checked first, so a peer session that also carries an operator
// flag gets the stricter operator rules and their plain (403) errors, which
// also refuse what the peer rules admit: a backslash-bearing alias and a
// carrier.
func TestOnQuery_PeerSessionWithAnOperatorFlagKeepsTheOperatorRules(t *testing.T) {
	p := peerGuard()
	for _, tc := range []struct{ name, sql, want string }{
		{"reserved name", "SELECT * FROM hg_replica.db1__t", `storage-integrity reserved name "hg_replica" is not addressable through the proxy`},
		{"measured backslash alias", peerLegForms["backslash alias (measured)"], "backslash"},
		{"carrier", "SELECT * FROM remote('h', 'other', 'u')", `object-carrier callable "remote"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSessionForTest(t, 41)
			sess.State().SetPeerTrust("10.0.0.7:9000")
			sess.State().SetPlatformOperator(true)
			err := runChain(context.Background(), p, &plugin.QueryContext{Session: sess, OriginalSQL: tc.sql, Query: &chproto.Query{Body: tc.sql}})
			var ce *chproto.ClientError
			if err == nil || errors.As(err, &ce) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want the operator refusal containing %q, not the peer 392", err, tc.want)
			}
		})
	}
	if err := runChain(context.Background(), p, peerQuery(t, "SELECT * FROM remote('h', 'other', 'u')", false)); err != nil {
		t.Fatalf("a carrier naming no reserved database must pass the peer rules: %v", err)
	}
}

// TestOnQuery_PeerRefusalCountsAndLogs: each refusal increments its reason
// once and writes exactly one warn line with the contract's fields, the
// statement cut to 512 bytes; an admitted leg counts and logs nothing.
func TestOnQuery_PeerRefusalCountsAndLogs(t *testing.T) {
	p := peerGuard()
	long := "SELECT 'x" + strings.Repeat("数据", 400) + "' FROM hg_replica.db1__t"
	for _, tc := range []struct{ reason, sql, reservedField string }{
		{"reserved_name", long, "reserved_name=hg_replica"},
		{"identifier_placeholder", "SELECT * FROM {db:Identifier}.t", `reserved_name=""`},
		{"statement_unscannable", "SELECT 'x", `reserved_name=""`},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			ctx, buf := captureLogs(t)
			before := testutil.ToFloat64(peerRefusals.WithLabelValues(tc.reason))
			err := runChain(ctx, p, peerQuery(t, tc.sql, false))
			var ce *chproto.ClientError
			if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited {
				t.Fatalf("err = %v, want a 392", err)
			}
			if got := testutil.ToFloat64(peerRefusals.WithLabelValues(tc.reason)) - before; got != 1 {
				t.Fatalf("storage_integrity_peer_reserved_name_refusals_total{reason=%q} grew by %v, want 1", tc.reason, got)
			}
			out := buf.String()
			if strings.Count(out, "\n") != 1 {
				t.Fatalf("want exactly one log line, got %q", out)
			}
			for _, want := range []string{
				"level=WARN",
				"peer statement refused by storage-integrity reserved-name guard",
				"plugin=sireserved",
				"code=392",
				"reason=" + tc.reason,
				tc.reservedField,
				"peer_address=10.0.0.7:9000",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("log line missing %q: %s", want, out)
				}
			}
			if tc.sql == long && (!strings.Contains(out, "…(truncated)") || strings.Contains(out, "FROM hg_replica")) {
				t.Errorf("the statement must be cut at 512 bytes: %s", out)
			}
		})
	}
	ctx, buf := captureLogs(t)
	before := testutil.ToFloat64(peerRefusals.WithLabelValues("reserved_name"))
	if err := runChain(ctx, p, peerQuery(t, peerLegForms["pushed-down count (measured)"], false)); err != nil {
		t.Fatalf("admitted leg refused: %v", err)
	}
	if buf.Len() != 0 || testutil.ToFloat64(peerRefusals.WithLabelValues("reserved_name")) != before {
		t.Fatalf("an admitted leg logged %q or counted a refusal", buf.String())
	}
}

// TestPeerReservedNamespaceViolation_DecodesEscapes is the function the
// integration tests share: names are compared after ClickHouse's decoding, an
// escape ClickHouse keeps as written is not a reserved name, heredoc bodies are
// raw, and scan errors are returned as is.
func TestPeerReservedNamespaceViolation_DecodesEscapes(t *testing.T) {
	dbs := sitable.ReservedDatabases()
	for _, tc := range []struct{ sql, want string }{
		{`SELECT * FROM merge('hg\x5Freplica', '^t$')`, "hg_replica"},
		{"SELECT * FROM `hg\\x5frstage`.t", "hg_rstage"},
		{`SELECT 'hg_\Nxfer'`, "hg_xfer"},
		{`SELECT 'HG_MEMBER\x5FUNSAFE'`, "hg_member_unsafe"},
		{peerLegForms["backslash alias (measured)"], ""},
		{`SELECT * FROM t WHERE s LIKE 'hg\_safe'`, ""},
		{`SELECT $$hg\x5Fsafe$$`, ""},
	} {
		got, err := PeerReservedNamespaceViolation(tc.sql, dbs, "_hg_row_id")
		if err != nil || got != tc.want {
			t.Errorf("PeerReservedNamespaceViolation(%q) = %q, %v; want %q", tc.sql, got, err, tc.want)
		}
	}
	if _, err := PeerReservedNamespaceViolation(`SELECT '\x4g'`, dbs, "_hg_row_id"); !errors.Is(err, sqlsurface.ErrUndecodableStringEscape) {
		t.Fatalf("err = %v, want ErrUndecodableStringEscape", err)
	}
	// The strict twin still refuses what the peer variant decodes.
	if _, err := ReservedNamespaceViolation(`SELECT * FROM merge('hg\x5Freplica', '^t$')`, dbs, "_hg_row_id"); err == nil {
		t.Fatal("ReservedNamespaceViolation must keep refusing backslash-bearing literals")
	}
}

// TestPeerScanOptionsDecodeBothEscapeKinds pins the options sipeerguard shares
// and why the peer rules never use the strict operator scan: it refuses a
// measured, legitimate leg.
func TestPeerScanOptionsDecodeBothEscapeKinds(t *testing.T) {
	if PeerScanOptions != (sqlsurface.Options{DecodeStringEscapes: true, DecodeIdentifierEscapes: true}) {
		t.Fatalf("PeerScanOptions = %+v", PeerScanOptions)
	}
	if _, err := sqlsurface.Scan(peerLegForms["backslash alias (measured)"]); err == nil {
		t.Fatal("the strict scan must refuse the measured backslash alias; the peer rules would then refuse a legitimate leg")
	}
}
