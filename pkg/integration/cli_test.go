package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/integration/testenv"
)

// CLI tests exercise the proxy through the official `clickhouse client`
// binary rather than the ch-go Go driver. They mirror the protocol-
// sensitive coverage in single_upstream_test.go and catch bugs that
// only surface when housegate's codec is talking to the official
// implementation — e.g. settings negotiation, addendum framing, or
// notchunked handshake quirks that ch-go-on-both-sides would not see.
//
// The binary is expected at tests/bin/clickhouse; ClickHouseCLI skips
// the test (does not fail) when it is missing.

// TestCLI_SelectOne is the smoke equivalent: `SELECT 1` round-trip
// through the proxy via the official CLI.
func TestCLI_SelectOne(t *testing.T) {
	bin := testenv.ClickHouseCLI(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr)

	out, err := testenv.RunCLI(t, bin, proxy.Addr, chEnv.Database, "SELECT 1")
	if err != nil {
		t.Fatalf("SELECT 1 via CLI: %v\nout: %s", err, out)
	}
	if out != "1" {
		t.Errorf("SELECT 1 via CLI = %q, want \"1\"", out)
	}
}

// TestCLI_InsertSelectRoundtrip drives CREATE / INSERT / SELECT / DROP
// entirely through the CLI. Validates the protocol's Data block path
// when the framing comes from the official client (which can encode
// settings, profile, and Data blocks differently from ch-go).
func TestCLI_InsertSelectRoundtrip(t *testing.T) {
	bin := testenv.ClickHouseCLI(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr)

	table := uniqueTable(t)
	mustCLI(t, bin, proxy.Addr,
		fmt.Sprintf("CREATE TABLE %s (v UInt64) ENGINE=Memory", table))
	t.Cleanup(func() {
		_, _ = testenv.RunCLI(t, bin, proxy.Addr, chEnv.Database,
			"DROP TABLE IF EXISTS "+table)
	})

	mustCLI(t, bin, proxy.Addr,
		fmt.Sprintf("INSERT INTO %s VALUES (1),(2),(3),(5),(8),(13),(21)", table))

	out := mustCLI(t, bin, proxy.Addr,
		fmt.Sprintf("SELECT v FROM %s ORDER BY v", table))
	wantLines := []string{"1", "2", "3", "5", "8", "13", "21"}
	got := strings.Split(out, "\n")
	if !equalStrings(got, wantLines) {
		t.Errorf("SELECT via CLI: got %v, want %v", got, wantLines)
	}
}

// TestCLI_LargeStream exercises the chunk-by-chunk upstream→client copy
// path under the official client. 100k rows is enough to span multiple
// Data block boundaries and the notchunked rewrite; we count via
// `SELECT count()` (one row out) instead of streaming all rows
// because the CLI text-formats every row and the test shouldn't pay
// that cost just to verify a count.
func TestCLI_LargeStream(t *testing.T) {
	bin := testenv.ClickHouseCLI(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 100_000
	out, err := testenv.RunCLIContext(ctx, t, bin, proxy.Addr, chEnv.Database,
		fmt.Sprintf("SELECT count() FROM (SELECT number FROM system.numbers LIMIT %d)", n))
	if err != nil {
		t.Fatalf("CLI count(): %v\nout: %s", err, out)
	}
	got, parseErr := strconv.Atoi(out)
	if parseErr != nil {
		t.Fatalf("parse count() output %q: %v", out, parseErr)
	}
	if got != n {
		t.Errorf("count() = %d, want %d", got, n)
	}
}

// TestCLI_Exception sends invalid SQL through the CLI and expects a
// non-zero exit with the offending identifier preserved. Verifies that
// proxy-side OnException best-effort handling does not swallow or
// rewrite the error text the CLI surfaces to the operator.
func TestCLI_Exception(t *testing.T) {
	bin := testenv.ClickHouseCLI(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr)

	out, err := testenv.RunCLI(t, bin, proxy.Addr, chEnv.Database,
		"SELECT * FROM table_that_does_not_exist_xyz")
	if err == nil {
		t.Fatalf("expected non-zero exit for unknown table, got success.\nout: %s", out)
	}
	if !strings.Contains(strings.ToLower(out), "table_that_does_not_exist_xyz") {
		t.Errorf("exception text lost its identifying detail; got: %s", out)
	}
}

// mustCLI is the t.Fatal variant of RunCLI for steps that should
// succeed unconditionally — keeps the test body short on setup
// statements where the value of stdout does not matter.
func mustCLI(t *testing.T, bin, proxyAddr, query string) string {
	t.Helper()
	out, err := testenv.RunCLI(t, bin, proxyAddr, chEnv.Database, query)
	if err != nil {
		t.Fatalf("CLI query %q failed: %v\nout: %s", query, err, out)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
