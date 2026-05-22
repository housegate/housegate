package integration

import (
	"context"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/integration/testenv"
)

// TestUseDatabaseSwitch verifies that a `USE <name>` statement issued
// mid-session causes subsequent queries to resolve table names against
// that database. With the rewriter disabled, housegate forwards USE
// transparently — the assertion is that CH (not the proxy) honours the
// USE and routes unqualified table lookups to the right database.
//
// Notes on what is and isn't covered:
//   - SessionState.LogicalDatabase is NOT updated by the proxy in this
//     mode: sessionstate's OnHello captures hello.Database, and only
//     the rewriter (when enabled) calls SetLogicalDatabase from
//     STATEMENT_TYPE_USE. We intentionally do not assert on
//     SessionState fields here — that's a separate test that needs
//     the rewriter.
//   - We do verify that forward.Plugin lets the second USE through
//     when the target database is registered in NetworkState. An
//     unregistered USE target would fail at OnHello-equivalent
//     routing checks (per forward.Plugin code path).
func TestUseDatabaseSwitch(t *testing.T) {
	const dbA = "use_switch_a"
	const dbB = "use_switch_b"

	// Both databases must be known to NetworkState; otherwise the
	// hello/USE machinery rejects them. forward.Plugin reads from
	// the in-memory NetworkState via WithExtraDatabases.
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases(dbA, dbB),
	)

	// Seed databases + identically-named tables with distinct marker
	// values, directly on the container so test setup does not
	// depend on the path we are about to verify.
	seedDB := openConnNoDB(t, chEnv.Addr)
	for _, db := range []string{dbA, dbB} {
		mustExec(t, seedDB, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db))
		t.Cleanup(func() {
			_ = seedDB.Exec(context.Background(),
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", db))
		})
		mustExec(t, seedDB, fmt.Sprintf(
			"CREATE TABLE %s.marker (v UInt64) ENGINE=Memory", db))
	}
	mustExec(t, seedDB, fmt.Sprintf("INSERT INTO %s.marker VALUES (1)", dbA))
	mustExec(t, seedDB, fmt.Sprintf("INSERT INTO %s.marker VALUES (2)", dbB))
	_ = seedDB.Close()

	// Now drive the proxy. clickhouse-go's Exec works for both DDL
	// and USE; the session lives for the lifetime of the underlying
	// conn, which the driver pools, so we must run everything on the
	// same conn. We achieve that by issuing all statements on one
	// `conn` instance and trusting the driver's pool to keep them
	// on the same backing connection within a single request batch.
	//
	// In practice the simplest way to guarantee single-conn affinity
	// is to open with MaxOpenConns=1: the driver then has only one
	// conn to choose from.
	conn := openConnPinned(t, proxy.Addr)
	ctx := context.Background()

	mustExec(t, conn, fmt.Sprintf("USE %s", dbA))
	var vA uint64
	if err := conn.QueryRow(ctx, "SELECT v FROM marker").Scan(&vA); err != nil {
		t.Fatalf("SELECT v FROM marker after USE %s: %v", dbA, err)
	}
	if vA != 1 {
		t.Errorf("after USE %s: v = %d, want 1", dbA, vA)
	}

	mustExec(t, conn, fmt.Sprintf("USE %s", dbB))
	var vB uint64
	if err := conn.QueryRow(ctx, "SELECT v FROM marker").Scan(&vB); err != nil {
		t.Fatalf("SELECT v FROM marker after USE %s: %v", dbB, err)
	}
	if vB != 2 {
		t.Errorf("after USE %s: v = %d, want 2", dbB, vB)
	}
}

// openConnNoDB connects directly to a CH address (not through the proxy)
// without binding a database — used for test setup that creates the
// databases the proxy then routes against.
func openConnNoDB(t *testing.T, chAddr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open (no DB): %v", err)
	}
	return conn
}

// openConnPinned opens a clickhouse-go connection with MaxOpenConns=1,
// pinning the test to a single backing TCP connection. Necessary when
// the test relies on session state that lives on one connection —
// USE/SET only affect the conn that issued them.
func openConnPinned(t *testing.T, proxyAddr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol:     clickhouse.Native,
		MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open (pinned): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustExec(t *testing.T, conn clickhouse.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
