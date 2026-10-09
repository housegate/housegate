package sipeerguard

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
)

func guardSession(t *testing.T, peer, forwarded bool) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	sess := chsession.New(1, client)
	if peer {
		sess.State().SetPeerTrustForwarded("10.0.0.9:9000", forwarded)
	}
	return sess
}

func guardQuery(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
}

func newGuard(status sitable.Status) *Plugin {
	return &Plugin{
		PhysicalDatabase: "devnet2",
		// Unrecorded tables default to Ordinary here so each case controls the
		// one table it names.
		TableState: sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "devnet101.swap_new106", Status: status}),
	}
}

var governedForms = map[string]string{
	"measured secondary query": "SELECT count() AS `count()` FROM `devnet2`.`devnet101.swap_new106` AS `__table1`",
	"bare physical database":   "SELECT `__table1`.`value` AS `value` FROM devnet2.`devnet101.swap_new106` AS `__table1`",
	"double-quoted":            `SELECT * FROM "devnet2"."devnet101.swap_new106"`,
	"standalone quoted":        "SELECT * FROM `devnet101.swap_new106`",
	"remote carrier literal":   "SELECT count() FROM remote('localhost:33001', 'devnet2', 'devnet101.swap_new106')",
	"comment before qualifier": "SELECT 1 /* x */ FROM `devnet2` /* y */ . `devnet101.swap_new106`",
	"heredoc does not hide it": "SELECT $$x$$, * FROM `devnet2`.`devnet101.swap_new106`",
	"IN subquery":              "SELECT 1 WHERE 1 IN (SELECT value FROM `devnet2`.`devnet101.swap_new106`)",
}

func TestGuardRefusesGovernedTablesOnPeerSessions(t *testing.T) {
	for _, status := range []sitable.Status{sitable.Pending, sitable.Refused, sitable.Active, sitable.Gone} {
		for name, sql := range governedForms {
			t.Run(status.String()+"/"+name, func(t *testing.T) {
				err := newGuard(status).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), sql))
				var clientErr *chproto.ClientError
				if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited {
					t.Fatalf("err = %v, want 392", err)
				}
				want := "storage_integrity: table devnet101.swap_new106 is governed by storage integrity and must be read through its host indexer; connect with --database devnet101 or USE devnet101"
				if clientErr.Message != want {
					t.Fatalf("message = %q, want %q", clientErr.Message, want)
				}
			})
		}
	}
}

func TestGuardPassesOrdinaryForwardedAndNonPeerSessions(t *testing.T) {
	sql := governedForms["measured secondary query"]
	if err := newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), sql)); err != nil {
		t.Fatalf("Ordinary table refused: %v", err)
	}
	if err := newGuard(sitable.Active).OnQuery(context.Background(), guardQuery(guardSession(t, true, true), sql)); err != nil {
		t.Fatalf("forwarded-from-peer session refused (its host runs the full chain): %v", err)
	}
	if err := newGuard(sitable.Active).OnQuery(context.Background(), guardQuery(guardSession(t, false, false), sql)); err != nil {
		t.Fatalf("non-peer session refused: %v", err)
	}
}

func TestGuardIgnoresNonCandidates(t *testing.T) {
	g := newGuard(sitable.Active)
	sess := guardSession(t, true, false)
	for _, sql := range []string{
		"SELECT * FROM `other`.`devnet101.swap_new106`",             // a different database qualifier
		"SELECT 'devnet101.swap_new106'",                            // a literal outside a carrier
		"SELECT * FROM remote('10.0.0.1:9000', 'devnet2', 'plain')", // address argument skipped; no dot in the table
		"-- `devnet2`.`devnet101.swap_new106`\nSELECT 1",            // inside a comment
	} {
		if err := g.OnQuery(context.Background(), guardQuery(sess, sql)); err != nil {
			t.Fatalf("%q refused: %v", sql, err)
		}
	}
}

const governedMessage = "storage_integrity: table devnet101.swap_new106 is governed by storage integrity and must be read through its host indexer; connect with --database devnet101 or USE devnet101"

// TestGuardDecodesEscapedQuotedIdentifiers: ClickHouse decodes a quoted
// identifier's escapes, so `devnet101\x2eswap_new106` names the governed
// table and is checked like its plain spelling; only an escape ClickHouse
// cannot be predicted to read (\x without two hex digits) is refused.
func TestGuardDecodesEscapedQuotedIdentifiers(t *testing.T) {
	escaped := "SELECT * FROM `devnet2`.`devnet101\\x2eswap_new106`"
	err := newGuard(sitable.Active).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), escaped))
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited || clientErr.Message != governedMessage {
		t.Fatalf("governed escaped identifier: err = %v, want the canonical 392", err)
	}
	if err := newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), escaped)); err != nil {
		t.Fatalf("Ordinary escaped identifier refused: %v", err)
	}

	err = newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), "SELECT * FROM `devnet2`.`devnet101\\x4gswap_new106`"))
	clientErr = nil
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited || !strings.Contains(clientErr.Message, "storage_integrity") {
		t.Fatalf("err = %v, want a 392 refusal of an undecodable identifier", err)
	}
}

// governedDatabaseState mirrors the SI host: an unrecorded table defaults to
// Pending only inside the governed database devnet101 and is Ordinary
// elsewhere. It is both the TableState and its one Snapshot.
type governedDatabaseState map[string]sitable.Status

func (s governedDatabaseState) Current() sitable.Snapshot { return s }
func (governedDatabaseState) Changed() <-chan struct{}    { return nil }
func (governedDatabaseState) Version() uint64             { return 1 }
func (governedDatabaseState) Active() []sitable.Table     { return nil }
func (governedDatabaseState) Schema(string) (sitable.Table, bool) {
	return sitable.Table{}, false
}

func (s governedDatabaseState) Lookup(database, table string) sitable.Table {
	id := database + "." + table
	if status, ok := s[id]; ok {
		return sitable.Table{ID: id, Status: status}
	}
	if database == "devnet101" {
		return sitable.Table{ID: id, Status: sitable.Pending}
	}
	return sitable.Table{ID: id, Status: sitable.Ordinary}
}

// TestGuardPassesMeasuredSecondaryQueryAliases runs projection aliases that
// ClickHouse 26.8.1 emitted in remote() secondary queries against an Ordinary
// table inside a governed database. Aliases are never table references, even
// when they contain '.' or escapes.
func TestGuardPassesMeasuredSecondaryQueryAliases(t *testing.T) {
	g := &Plugin{PhysicalDatabase: "phys", TableState: governedDatabaseState{"devnet101.t": sitable.Ordinary}}
	sess := guardSession(t, true, false)
	for _, sql := range []string{
		"SELECT extract(`__table1`.`s`, '\\\\d+') AS `extract(s, '\\\\\\\\d+')` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT concat(`__table1`.`s`, '\\n') AS `concat(s, '\\\\n')` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT concat(`__table1`.`s`, 'it\\'s') AS `concat(s, 'it\\\\'s')` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT avg(`__table1`.`x` * 1.5) AS `avg(multiply(x, 1.5))` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT countIf(`__table1`.`x` > 0.5) AS `countIf(greater(x, 0.5))` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT `__table1`.`n.a` AS `n.a` FROM `phys`.`devnet101.t` AS `__table1`",
		"SELECT `__table1`.`s` AS `devnet101.alias` FROM `phys`.`devnet101.t` AS `__table1`",
	} {
		if err := g.OnQuery(context.Background(), guardQuery(sess, sql)); err != nil {
			t.Errorf("%q refused: %v", sql, err)
		}
	}
	// The same state still refuses the governed (unrecorded, Pending) table
	// in every candidate form.
	for _, sql := range []string{
		"SELECT count() FROM `phys`.`devnet101.swap_new106` AS `__table1`",
		"SELECT * FROM `devnet101.swap_new106`",
		"SELECT * FROM `phys`.`devnet101.t` AS a JOIN `devnet101.swap_new106` AS b ON 1",
		"SELECT count() FROM remote('10.0.0.1:9000', 'phys', 'devnet101.swap_new106')",
	} {
		var clientErr *chproto.ClientError
		if err := g.OnQuery(context.Background(), guardQuery(sess, sql)); !errors.As(err, &clientErr) || clientErr.Message != governedMessage {
			t.Errorf("%q: err = %v, want the canonical 392", sql, err)
		}
	}
}

func TestGuardMarkers(t *testing.T) {
	p := &Plugin{}
	if !p.RunOnPeerTrust() || !p.RejectUndecodableQuery() {
		t.Fatal("the guard must run on peer-trusted sessions and refuse undecodable queries")
	}
}

func captureLogs(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return log.WithContext(context.Background(), log.New(slog.NewTextHandler(&buf, nil))), &buf
}

func TestRefusalLogsTruncatedStatementAtWarn(t *testing.T) {
	ctx, buf := captureLogs(t)
	// "SELECT 'x" is 9 bytes, so the 512-byte cut lands inside a 3-byte CJK rune.
	sql := "SELECT 'x" + strings.Repeat("数据", 400) + "' FROM `devnet2`.`devnet101.swap_new106`"
	if len(sql) <= 512 {
		t.Fatalf("test statement is only %d bytes", len(sql))
	}
	g := newGuard(sitable.Active)
	var clientErr *chproto.ClientError
	if err := g.OnQuery(ctx, guardQuery(guardSession(t, true, false), sql)); !errors.As(err, &clientErr) {
		t.Fatalf("err = %v, want a ClientError", err)
	}
	out := buf.String()
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("want exactly one log line, got %q", out)
	}
	for _, want := range []string{
		"level=WARN",
		"reason=governed_table",
		"code=392",
		"peer_address=10.0.0.9:9000",
		"devnet101.swap_new106",
		"…(truncated)",
		"plugin=sipeerguard",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log line missing %q: %s", want, out)
		}
	}
	if strings.Contains(out, "FROM `devnet2`") {
		t.Errorf("statement tail was not truncated: %s", out)
	}
	if !utf8.ValidString(out) {
		t.Errorf("log line is not valid UTF-8: %q", out)
	}
}

func TestTruncateStatement(t *testing.T) {
	if got := truncateStatement("SELECT 1"); got != "SELECT 1" {
		t.Fatalf("short statement changed: %q", got)
	}
	exact := strings.Repeat("a", 512)
	if got := truncateStatement(exact); got != exact {
		t.Fatalf("512-byte statement must pass unchanged")
	}
	for pad := 0; pad < 3; pad++ {
		sql := strings.Repeat("a", pad) + strings.Repeat("数", 400)
		got := truncateStatement(sql)
		body, ok := strings.CutSuffix(got, "…(truncated)")
		if !ok {
			t.Fatalf("pad %d: missing marker: %q", pad, got)
		}
		if !utf8.ValidString(body) || len(body) > 512 || len(body) < 510 {
			t.Fatalf("pad %d: body len %d valid=%v", pad, len(body), utf8.ValidString(body))
		}
	}
}

func TestUnscannableRefusalLogsWarn(t *testing.T) {
	ctx, buf := captureLogs(t)
	sql := "SELECT * FROM `devnet2`.`devnet101\\x4gswap_new106`"
	if err := newGuard(sitable.Ordinary).OnQuery(ctx, guardQuery(guardSession(t, true, false), sql)); err == nil {
		t.Fatal("want a refusal")
	}
	out := buf.String()
	for _, want := range []string{"level=WARN", "reason=statement_unscannable", "code=392", "peer_address=10.0.0.9:9000"} {
		if !strings.Contains(out, want) {
			t.Errorf("log line missing %q: %s", want, out)
		}
	}
}

func TestAdmittedQueryEmitsNoRefusalLog(t *testing.T) {
	ctx, buf := captureLogs(t)
	sql := governedForms["measured secondary query"]
	if err := newGuard(sitable.Ordinary).OnQuery(ctx, guardQuery(guardSession(t, true, false), sql)); err != nil {
		t.Fatalf("Ordinary table refused: %v", err)
	}
	if err := newGuard(sitable.Active).OnQuery(ctx, guardQuery(guardSession(t, false, false), sql)); err != nil {
		t.Fatalf("non-peer session refused: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("admitted queries logged: %s", buf.String())
	}
}
