package sireserved

import (
	"context"
	"net"
	"strings"
	"testing"

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
		{"single-quoted literal", "SELECT 'hg_safe' AS s FROM ordinary.t"},
		{"escaped quote", `SELECT 'it''s hg_safe' AS s FROM ordinary.t`},
		{"backslash-escaped quote", `SELECT 'a\' hg_safe b' AS s FROM ordinary.t`},
		{"line comment", "SELECT a FROM ordinary.t -- hg_safe"},
		{"hash comment", "SELECT a FROM ordinary.t # hg_safe"},
		{"slash comment", "SELECT a FROM ordinary.t // hg_safe"},
		{"block comment", "SELECT a FROM /* hg_safe */ ordinary.t"},
		{"nested block comment", "SELECT a FROM /* x /* hg_safe */ y */ ordinary.t"},
		{"rid literal", "SELECT '_hg_row_id' AS s FROM ordinary.t"},
		{"prefix", "SELECT * FROM hg_safe_backup.t"},
		{"suffix", "SELECT * FROM my_hg_safe.t"},
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

func TestReservedNamespaceViolation_EscapedBackslashDoesNotSwallowStatement(t *testing.T) {
	got, err := ReservedNamespaceViolation(`SELECT 'a\\', hg_safe FROM ordinary.t`, []string{"hg_safe"}, "_hg_row_id")
	if err != nil {
		t.Fatalf("this literal is terminated: %v", err)
	}
	if got != "hg_safe" {
		t.Fatalf("the mention after the literal must be seen, got %q", got)
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
