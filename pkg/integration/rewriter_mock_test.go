package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

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
// forwarded to ClickHouse. The rejection ends only that query; the
// session serves the next one.
func TestRewriter_RejectionFailsClosed(t *testing.T) {
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt)

	mock.FailNext(1)
	conn := openConn(t, proxy.Addr)
	var v uint8
	err := conn.QueryRow(context.Background(), "SELECT 7").Scan(&v)
	if err == nil || !strings.Contains(err.Error(), "rewriter mock: forced failure via FailNext") {
		t.Fatalf("SELECT 7 with rewriter forced to fail: err = %v, want the rewriter's rejection", err)
	}
	if got := len(mock.SeenSQL()); got != 1 {
		t.Errorf("mock saw %d SQLs, want exactly 1", got)
	}

	if err := conn.QueryRow(context.Background(), "SELECT 8").Scan(&v); err != nil {
		t.Fatalf("SELECT 8 after the rejection: %v", err)
	}
	if v != 8 {
		t.Errorf("SELECT 8 = %d, want 8", v)
	}
}

// TestRewriter_OutageFollowsFailOpenOnUnavailable pins the only remaining
// fail-open: with storage integrity disabled and the rewriter down, the
// original SQL reaches ClickHouse only under
// rewriter.fail_open_on_unavailable; otherwise the client gets an Exception.
func TestRewriter_OutageFollowsFailOpenOnUnavailable(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail_open_on_unavailable=%v", failOpen), func(t *testing.T) {
			rewriterOpt, mock := testenv.WithRewriterMock(t)
			proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt,
				testenv.WithConfigMutator(func(c *config.Config) { c.Rewriter.FailOpenOnUnavailable = failOpen }))
			mock.Stop()

			conn := openConn(t, proxy.Addr)
			var v uint8
			err := conn.QueryRow(context.Background(), "SELECT 7").Scan(&v)
			if failOpen {
				if err != nil {
					t.Fatalf("SELECT 7 with the rewriter down and the switch on: %v", err)
				}
				if v != 7 {
					t.Errorf("SELECT 7 = %d, want 7 (original SQL must reach CH unchanged)", v)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "rewrite unavailable") {
				t.Fatalf("SELECT 7 with the rewriter down: err = %v, want a rewrite-unavailable Exception", err)
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
