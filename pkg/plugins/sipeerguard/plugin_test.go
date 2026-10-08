package sipeerguard

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

func TestGuardRefusesUndecodableQuotedIdentifier(t *testing.T) {
	err := newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), "SELECT * FROM `devnet2`.`devnet101\\x2eswap_new106`"))
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited || !strings.Contains(clientErr.Message, "storage_integrity") {
		t.Fatalf("err = %v, want a 392 refusal of an undecodable identifier", err)
	}
}

func TestGuardMarkers(t *testing.T) {
	p := &Plugin{}
	if !p.RunOnPeerTrust() || !p.RejectUndecodableQuery() {
		t.Fatal("the guard must run on peer-trusted sessions and refuse undecodable queries")
	}
}
