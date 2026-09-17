package integration

import (
	"context"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/integration/testenv"
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
	t.Cleanup(func() {
		if err := seedDB.Close(); err != nil {
			t.Errorf("close seed connection: %v", err)
		}
	})
	for _, db := range []string{dbA, dbB} {
		mustExec(t, seedDB, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db))
		t.Cleanup(func() {
			if err := seedDB.Exec(context.Background(),
				fmt.Sprintf("DROP DATABASE IF EXISTS %s", db)); err != nil {
				t.Errorf("drop database %s: %v", db, err)
			}
		})
		mustExec(t, seedDB, fmt.Sprintf(
			"CREATE TABLE %s.marker (v UInt64) ENGINE=Memory", db))
	}
	mustExec(t, seedDB, fmt.Sprintf("INSERT INTO %s.marker VALUES (1)", dbA))
	mustExec(t, seedDB, fmt.Sprintf("INSERT INTO %s.marker VALUES (2)", dbB))

	ctx := context.Background()
	// Keep one database/sql connection lease for the complete sequence;
	// USE changes session state on the leased physical connection.
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{proxy.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close proxy database handle: %v", err)
		}
	})
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("lease proxy connection: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close proxy connection lease: %v", err)
		}
	})

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbA)); err != nil {
		t.Fatalf("Exec %q: %v", fmt.Sprintf("USE %s", dbA), err)
	}
	var vA uint64
	if err := conn.QueryRowContext(ctx, "SELECT v FROM marker").Scan(&vA); err != nil {
		t.Fatalf("SELECT v FROM marker after USE %s: %v", dbA, err)
	}
	if vA != 1 {
		t.Errorf("after USE %s: v = %d, want 1", dbA, vA)
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbB)); err != nil {
		t.Fatalf("Exec %q: %v", fmt.Sprintf("USE %s", dbB), err)
	}
	var vB uint64
	if err := conn.QueryRowContext(ctx, "SELECT v FROM marker").Scan(&vB); err != nil {
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

func mustExec(t *testing.T, conn clickhouse.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
