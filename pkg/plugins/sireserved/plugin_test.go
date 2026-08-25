package sireserved

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
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

func TestOnQuery_IdentifierPlaceholderLeavesOrdinaryPeerBypassAlone(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe", "hg_unsafe"}, ReservedRowIDColumn: "_hg_row_id"}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
	}{
		{name: "ordinary", set: func(*chsession.SessionState) {}},
		{name: "ordinary peer", set: func(state *chsession.SessionState) { state.SetPeerTrust("10.0.0.7:9000") }},
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
				t.Fatalf("non-operator D6 peer behavior must remain unchanged: %v", err)
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
	} {
		sess := newSessionForTest(t, 25)
		sess.State().SetMaintenance(true)
		qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("non-carrier SQL %q must pass: %v", sql, err)
		}
	}
}

func TestOnQuery_LeavesOrdinaryAndNonOperatorSessionsAlone(t *testing.T) {
	p := &Plugin{ReservedDatabases: []string{"hg_safe"}, ReservedRowIDColumn: "_hg_row_id"}

	sess := newSessionForTest(t, 3)
	sess.State().SetMaintenance(true)
	const ordinary = "SELECT * FROM other.u"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: ordinary, Query: &chproto.Query{Body: ordinary}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("ordinary table must pass: %v", err)
	}

	peer := newSessionForTest(t, 4)
	peer.State().SetPeerTrust("10.0.0.7:9000")
	const reserved = "SELECT * FROM hg_safe.db1__t"
	pctx := &plugin.QueryContext{Session: peer, OriginalSQL: reserved, Query: &chproto.Query{Body: reserved}}
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
	if err := chain.OnQuery(context.Background(), pctx); err != nil {
		t.Fatalf("a plain peer session is out of scope for the operator guard: %v", err)
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
