package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/registry"
	pb "github.com/housegate/rewriter-go/gen/pb"
)

// openDirectConn opens a clickhouse-go connection directly to the shared
// CH container, bypassing the proxy. Used for test-data seeding.
func openDirectConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chEnv.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("direct clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestAuth_PermissionIsolationAcrossDatabases verifies that a signer
// with Read permission on database A but NOT on database B can query A
// while being rejected on B.
//
// PermissionCommitGateObserver enforces the gate: the mock rewriter
// attaches a single AccessedTable with the target logical database,
// and the observer checks the signer's DbAuthRead (promoted from
// DbAuthOwner / DbAuthWrite) against that database via NetworkState.
//
// The test sets up two logical databases (perm_iso_a, perm_iso_b)
// both registered on NetworkState, grants Read on perm_iso_a only,
// then exercises:
//
//   - SELECT on perm_iso_a → succeeds (Read granted)
//   - SELECT on perm_iso_b → rejected with permission error
//     (no Read granted; error surfaces before CH is contacted)
func TestAuth_PermissionIsolationAcrossDatabases(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	const (
		dbA = "perm_iso_a"
		dbB = "perm_iso_b"
		tbl = "data"
	)

	rewriterOpt, mock := testenv.WithRewriterMock(t)

	// Attach AccessedTables based on SQL prefix. The mock matches on
	// strings.HasPrefix(casefolded), so we differentiate by database
	// name component. ORDER matters: register the longer prefix first
	// so a partial match on one DB name cannot shadow the other.
	tablesA := []*pb.AccessedTable{{
		OriginalDatabase: dbA,
		OriginalTable:    tbl,
		LogicalDatabase:  dbA,
		PhysicalDatabase: dbA,
	}}
	tablesB := []*pb.AccessedTable{{
		OriginalDatabase: dbB,
		OriginalTable:    tbl,
		LogicalDatabase:  dbB,
		PhysicalDatabase: dbB,
	}}
	mock.SetAccessedTables("SELECT * FROM "+dbA, tablesA)
	mock.SetAccessedTables("SELECT * FROM "+dbB, tablesB)

	// Seed the physical databases and tables on CH directly (bypassing
	// the proxy so commitgate DDL gating is not in our way).
	direct := openDirectConn(t)
	for _, db := range []string{dbA, dbB} {
		if err := direct.Exec(context.Background(),
			fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db),
		); err != nil {
			t.Fatalf("seed create database %s: %v", db, err)
		}
		if err := direct.Exec(context.Background(),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id UInt32) ENGINE = Memory", db, tbl),
		); err != nil {
			t.Fatalf("seed create table %s.%s: %v", db, tbl, err)
		}
	}
	// Insert a row into dbA so the success-side SELECT actually returns
	// a value (QueryRow with zero rows returns "no rows in result set").
	if err := direct.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s.%s VALUES (42)", dbA, tbl),
	); err != nil {
		t.Fatalf("seed insert into %s.%s: %v", dbA, tbl, err)
	}
	t.Cleanup(func() {
		for _, db := range []string{dbA, dbB} {
			_ = direct.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		}
	})

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		testenv.WithExtraDatabases(dbA, dbB),
		testenv.WithDatabasePermission(signer.Address(), dbA, registry.DbAuthRead),
		authProxyConfig([]string{signer.Address()}, false),
	)

	conn := openSignedConn(t, proxy.Addr, signer)

	// dbA: Read granted → must succeed.
	var got uint32
	if err := conn.QueryRow(context.Background(),
		fmt.Sprintf("SELECT * FROM %s.%s", dbA, tbl),
	).Scan(&got); err != nil {
		t.Fatalf("query on permitted database %s failed: %v", dbA, err)
	}
	if got != 42 {
		t.Errorf("query on %s returned %d, want 42", dbA, got)
	}

	// dbB: no Read granted → must be rejected by PermissionObserver
	// before CH is contacted. The error message must indicate the
	// database name so operators can diagnose the denial.
	err = conn.QueryRow(context.Background(),
		fmt.Sprintf("SELECT * FROM %s.%s", dbB, tbl),
	).Scan(new(uint8))
	if err == nil {
		t.Fatalf("query on unpermitted database %s succeeded; expected rejection", dbB)
	}
	if !strings.Contains(err.Error(), dbB) {
		t.Errorf("error = %q, want to contain database name %q", err.Error(), dbB)
	}
}
