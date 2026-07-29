package integration

import (
	"context"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
)

// openSignedConnPinned is the signed+pinned variant: every query is
// JWS-signed AND the driver is told to hold a single backing conn so
// session state (USE/SET) persists across statements within a test.
// We need both: signing is required when auth is on; pinning is
// required when the test depends on per-conn state surviving.
func openSignedConnPinned(t *testing.T, proxyAddr string, signer *auth.RelaySigner) clickhouse.Conn {
	t.Helper()
	return openSignedConnPinnedDB(t, proxyAddr, signer, chEnv.Database)
}

// openSignedConnPinnedDB is openSignedConnPinned with an explicit
// hello.Database — useful for tests that need the proxy to see a
// specific database in the hello (forward.Plugin's OnHello pivot,
// session-state physical-database rewrite tests).
func openSignedConnPinnedDB(t *testing.T, proxyAddr string, signer *auth.RelaySigner, database string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol:     clickhouse.Native,
		SignFunc:     signer.SignToken,
		MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open (signed + pinned): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestSessionState_USEUpdatesLogical pins the rewriter→SessionState
// callback path: when the rewriter classifies a SQL as STATEMENT_TYPE_USE,
// sentioRewriter.maybeUpdateLogicalDatabase calls
// SessionState.SetLogicalDatabase, which the next OnQuery's dynamic_args
// observes via upstream_logical_database_in_context.
//
// Mechanic: signed pinned conn → USE dbA → SELECT 1 → check the second
// Rewrite call's dynamic_args has upstream_logical_database_in_context ==
// dbA (the first call's value was chEnv.Database from hello).
//
// Without the USE-classified callback firing, the second dynamic_args
// would still carry the original hello database, surfacing as the
// assertion failure here.
func TestSessionState_USEUpdatesLogical(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	const altDB = "use_logical_alt"

	// Seed altDB on CH directly so a USE against it doesn't get a
	// "doesn't exist" from upstream.
	seedDB := openConnNoDB(t, chEnv.Addr)
	mustExec(t, seedDB, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", altDB))
	t.Cleanup(func() {
		_ = seedDB.Exec(context.Background(),
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", altDB))
	})
	_ = seedDB.Close()

	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithExtraDatabases(altDB),
		// Owner on BOTH the hello database (for the trailing SELECT 1)
		// and the new altDB (for the USE itself + any subsequent
		// queries the proxy classifies against it).
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthOwner),
		testenv.WithDatabasePermission(signer.Address(), altDB, registry.DbAuthOwner),
	)

	conn := openSignedConnPinned(t, proxy.Addr, signer)
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf("USE %s", altDB)); err != nil {
		t.Fatalf("USE %s: %v", altDB, err)
	}
	var v uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 after USE: %v", err)
	}

	// SeenDynamicArgs is index-parallel to SeenSQL. The first Rewrite
	// call was "USE altDB" (logical = chEnv.Database, still the hello
	// value at that point); the second was "SELECT 1" (logical should
	// now be altDB because USE updated SessionState).
	args := mock.SeenDynamicArgs()
	sqls := mock.SeenSQL()
	if len(args) < 2 {
		t.Fatalf("expected ≥2 rewrite calls, got %d (sqls: %v)", len(args), sqls)
	}
	// Find the SELECT 1 call index. The rewriter mock receives every
	// statement in order, so it's just the last one.
	got := args[len(args)-1]
	if got == nil {
		t.Fatal("last dynamic_args is nil")
	}
	if logical := got.GetUpstreamLogicalDatabaseInContext(); logical != altDB {
		t.Errorf("upstream_logical_database_in_context after USE = %q, want %q (sqls so far: %v)",
			logical, altDB, sqls)
	}
}
