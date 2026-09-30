package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
)

// TestRewriterMock_RoundTrips is the first sanity test for the
// in-process rewriter mock. It verifies two things:
//
//  1. The mock's gRPC server is reachable, housegate dials it, and the
//     full plugin chain runs (auth-off path), the SQL reaches CH, and
//     the result is correctly relayed back to the client.
//  2. The mock actually observed the SQL — confirms housegate did not
//     short-circuit, fail-open, or talk to some other rewriter. The
//     point of the mock is to drive downstream plugins (commitgate,
//     sessionstate) via a real gRPC response; if SeenSQL is empty
//     here, the whole tier-3 plan is dead-letter.
func TestRewriterMock_RoundTrips(t *testing.T) {
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt)

	conn := openConn(t, proxy.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 through mock-rewriter proxy: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}

	seen := mock.SeenSQL()
	if len(seen) == 0 {
		t.Fatal("rewriter mock recorded zero SQLs — proxy bypassed the rewriter path")
	}
	t.Logf("rewriter mock saw %d statement(s); first = %q", len(seen), seen[0])
}

// TestRewriter_RejectionFailsClosed verifies the rewrite plugin's
// fail-closed contract with storage integrity disabled (spec 2026-09-26
// T8): when the rewriter returns a non-Success code, the client receives
// an Exception carrying the engine message and the ORIGINAL SQL is not
// forwarded to ClickHouse — proven with a side-effecting INSERT whose row
// never arrives. The rejection ends only that query; the session serves
// the next one.
func TestRewriter_RejectionFailsClosed(t *testing.T) {
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt)
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()

	// The table is managed directly on ClickHouse: DDL through the mock
	// would need commitgate AccessedTables the mock does not report.
	direct := openDirectCH(t)
	const table = "rewriter_rejection_fails_closed"
	if err := direct.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := direct.Exec(ctx, "CREATE TABLE "+table+" (a UInt8) ENGINE = Memory"); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = direct.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	seenBefore := len(mock.SeenSQL())
	mock.FailNext(1)
	err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (7)")
	if err == nil || !strings.Contains(err.Error(), "rewriter mock: forced failure via FailNext") {
		t.Fatalf("INSERT with rewriter forced to fail: err = %v, want the rewriter's rejection", err)
	}
	if got := len(mock.SeenSQL()) - seenBefore; got != 1 {
		t.Errorf("mock saw %d SQLs for the rejected INSERT, want exactly 1", got)
	}

	var n uint64
	if err := direct.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count after the rejection: %v", err)
	}
	if n != 0 {
		t.Fatalf("count = %d after a rejected INSERT, want 0: the statement reached ClickHouse", n)
	}

	// The same session still serves queries, and the same INSERT goes
	// through once the rewriter accepts it.
	if err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (8)"); err != nil {
		t.Fatalf("INSERT after the rejection: %v", err)
	}
	if err := direct.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
}

// TestRewriter_OutageFollowsFailOpenOnUnavailable pins the only remaining
// fail-open: with storage integrity disabled and the rewriter down, the
// original SQL reaches ClickHouse only under
// rewriter.fail_open_on_unavailable; otherwise the client gets a generic
// Exception. A query in the instant before housegate notices the stopped
// service is written to a dying transport and fails closed as a rejection,
// so each case retries until the steady outage state is reached.
func TestRewriter_OutageFollowsFailOpenOnUnavailable(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail_open_on_unavailable=%v", failOpen), func(t *testing.T) {
			rewriterOpt, mock := testenv.WithRewriterMock(t)
			proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt,
				testenv.WithConfigMutator(func(c *config.Config) { c.Rewriter.FailOpenOnUnavailable = failOpen }))
			mock.Stop()

			conn := openConn(t, proxy.Addr)
			deadline := time.Now().Add(5 * time.Second)
			for {
				var v uint8
				err := conn.QueryRow(context.Background(), "SELECT 7").Scan(&v)
				if failOpen && err == nil {
					if v != 7 {
						t.Errorf("SELECT 7 = %d, want 7 (original SQL must reach CH unchanged)", v)
					}
					return
				}
				if !failOpen && err != nil && strings.Contains(err.Error(), "rewriter unavailable") {
					if strings.Contains(err.Error(), "rpc error") || strings.Contains(err.Error(), "127.0.0.1") {
						t.Fatalf("Exception %q leaks transport detail", err.Error())
					}
					return
				}
				if !failOpen && err == nil {
					t.Fatal("SELECT 7 succeeded with the rewriter down and the switch off")
				}
				if time.Now().After(deadline) {
					t.Fatalf("no steady outage outcome within 5s; last err = %v", err)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// TestRewriter_SeenSQLMatchesClient pins the wire contract between the
// proxy and the rewriter service: the SQL the client sends is the
// SAME byte string the proxy ships to the rewriter as request.sql.
//
// A regression here would mean housegate is mutating the request body
// before classification — typically a sign of an over-eager filter,
// settings injection, or stale buffer reuse. The downstream policy
// engine sees a different statement than the client signed; that's a
// security-relevant divergence and the kind of thing this assertion
// is here to catch.
func TestRewriter_SeenSQLMatchesClient(t *testing.T) {
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt)

	conn := openConn(t, proxy.Addr)
	// Use a distinctive SQL so the assertion is unambiguous even
	// across version churn — a literal that only this test would
	// emit means no other plugin can have generated it.
	const distinctive = "SELECT 42 AS rewriter_seen_test_marker"
	var v uint8
	if err := conn.QueryRow(context.Background(), distinctive).Scan(&v); err != nil {
		t.Fatalf("distinctive query: %v", err)
	}
	if v != 42 {
		t.Errorf("distinctive query = %d, want 42", v)
	}

	seen := mock.SeenSQL()
	if len(seen) != 1 {
		t.Fatalf("mock saw %d SQLs, want 1: %s", len(seen), formatSeen(seen))
	}
	if seen[0] != distinctive {
		t.Errorf("rewriter received %q, client sent %q — proxy mutated the SQL",
			seen[0], distinctive)
	}
}

func formatSeen(seen []string) string {
	out := ""
	for i, s := range seen {
		out += fmt.Sprintf("\n  [%d] %q", i, s)
	}
	return out
}
