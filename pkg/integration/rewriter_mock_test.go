package integration

import (
	"context"
	"fmt"
	"testing"

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

// TestRewriter_FailOpen verifies the rewriter plugin's fail-open
// contract: when the rewriter returns a non-Success code, the proxy
// logs the failure and forwards the ORIGINAL SQL to ClickHouse so the
// session stays usable.
//
// Constraint: auth stays off here. With auth on the commitgate
// PermissionObserver wakes up and rejects every Unspecified statement
// (which is exactly what fail-open produces — qctx.StatementType is
// never set on the failed path). The fail-open contract is meaningful
// only for the auth-off / no-permission-gating deployment.
func TestRewriter_FailOpen(t *testing.T) {
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr, rewriterOpt)

	mock.FailNext(1)
	conn := openConn(t, proxy.Addr)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 7").Scan(&v); err != nil {
		t.Fatalf("SELECT 7 with rewriter forced to fail: %v", err)
	}
	if v != 7 {
		t.Errorf("SELECT 7 = %d, want 7 (original SQL must reach CH unchanged)", v)
	}

	// One Rewrite call was attempted (and failed); the mock still
	// recorded it.
	if got := len(mock.SeenSQL()); got != 1 {
		t.Errorf("mock saw %d SQLs, want exactly 1", got)
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
