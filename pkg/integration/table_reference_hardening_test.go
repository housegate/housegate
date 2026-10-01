package integration

import (
	"context"
	"os"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sitable"
)

func requireNativeLib(t *testing.T) string {
	t.Helper()
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.16.0` and pass --test_env")
	}
	return lib
}

// startTableRefProxy starts a server proxy on the native engine whose
// physical database is phys (created fresh on ClickHouse), with logical
// database db1 registered and the guard in guardMode. Auth is off, so the
// session's account is empty; it owns db1 so commitgate admits its DDL.
func startTableRefProxy(t *testing.T, phys, guardMode string, extra ...testenv.ProxyOption) *testenv.TestProxy {
	t.Helper()
	lib := requireNativeLib(t)
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{"DROP DATABASE IF EXISTS " + phys, "CREATE DATABASE " + phys} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys) })
	opts := append([]testenv.ProxyOption{
		testenv.WithExtraDatabases("db1"),
		testenv.WithDatabasePermission("", "db1", registry.DbAuthOwner),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.TableRefGuard.Mode = guardMode
		}),
	}, extra...)
	return testenv.StartServerProxy(t, chEnv.Addr, opts...)
}

// TestTableReference_TenantSourcesResolveEndToEnd: own-table sources and IN
// operands are rewritten (spec T4); before the hardening the source kept its
// logical name and ClickHouse could not resolve it.
func TestTableReference_TenantSourcesResolveEndToEnd(t *testing.T) {
	proxy := startTableRefProxy(t, "phys_tr1", "enforce")
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE TABLE db1.o (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO db1.o SELECT number + 1 FROM numbers(2)",
		"INSERT INTO db1.o SELECT * FROM db1.o",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.o WHERE a IN db1.o").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("count = %d, want 4", n)
	}
	var has uint8
	if err := conn.QueryRow(ctx, "SELECT hasColumnInTable('db1', 'o', 'a')").Scan(&has); err != nil {
		t.Fatal(err)
	}
	if has != 1 {
		t.Fatalf("hasColumnInTable = %d, want 1 (T6 rewrite to the physical table)", has)
	}
}

// TestTableReference_RefusalsReachTheClientAsExceptions runs each refusal with
// the guard observing (the engine answers) and enforcing (the guard answers
// first where a rule applies); the connection survives every refusal.
func TestTableReference_RefusalsReachTheClientAsExceptions(t *testing.T) {
	const phys = "phys_tr2"
	cases := []struct{ sql, engine, guard string }{
		{"SELECT * FROM {p:Identifier}", "query parameters are not supported in a database or table position", "table-reference guard: identifier_placeholder:"},
		{"SELECT * FROM db1.o WHERE a IN " + phys + ".`db2.x`", "protected database " + phys + " is not addressable", "table-reference guard: physical_database:"},
		{"SELECT * FROM merge('" + phys + "', '.*')", "protected database " + phys + " is not addressable", "table-reference guard: carrier_callable:"},
		{"USE " + phys, "protected database " + phys + " is not addressable", "table-reference guard: physical_database:"},
		{"SELECT * FROM hg_promote.`db2.x`", "protected database hg_promote is not addressable", "table-reference guard: reserved_name:"},
		{"DETACH TABLE db1.o", "statement is not supported", "statement is not supported"},
		{"SELECT 1 SETTINGS enable_analyzer = 0", "table setting enable_analyzer is not accepted", "table setting enable_analyzer is not accepted"},
	}
	for _, mode := range []string{"observe", "enforce"} {
		t.Run(mode, func(t *testing.T) {
			proxy := startTableRefProxy(t, phys, mode)
			conn := openConn(t, proxy.Addr)
			ctx := context.Background()
			for _, tc := range cases {
				want := tc.engine
				if mode == "enforce" {
					want = tc.guard
				}
				if err := conn.Exec(ctx, tc.sql); err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("%s: err = %v, want it to contain %q", tc.sql, err, want)
				}
			}
			var one uint8
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("the session must survive the refusals: %v", err)
			}
		})
	}
}

// TestTableReference_StorageIntegrity: an IN operand naming an Active table
// reads the safe surface; an escaped MV target naming a Pending table is
// refused by sitablestate (T10). The guard observes so G5 does not answer
// first.
func TestTableReference_StorageIntegrity(t *testing.T) {
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"CREATE TABLE hg_safe.db1__t (_hg_row_id FixedString(32), a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO hg_safe.db1__t VALUES (repeat('a', 32), 1)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
	})
	state := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "db1.p", Status: sitable.Pending},
	)
	proxy := startTableRefProxy(t, "phys_tr3", "observe",
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.StorageIntegrity.Enabled = &enabled
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityTableState = state },
	)
	conn := openConn(t, proxy.Addr)
	for _, q := range []string{
		"CREATE TABLE db1.o (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO db1.o SELECT number + 1 FROM numbers(3)",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.o WHERE a IN db1.t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1 (only a = 1 is in the safe read)", n)
	}
	err := conn.Exec(ctx, "CREATE MATERIALIZED VIEW db1.mv TO db1.`\\x70` AS SELECT a FROM db1.o")
	if err == nil || !strings.Contains(err.Error(), "the materialized view header cannot be read") {
		t.Fatalf("err = %v, want the sitablestate unreadable-header refusal", err)
	}
}

// TestTableReference_DriverTrafficIsUnaffected: an indexer-signed driver
// session (SQL_sentio_driver) is ordinary for the guard and the engine; its
// DDL, INSERT … SELECT, IN-operand reads and physical-name metadata reads
// (housegate#218) pass with the guard enforcing.
func TestTableReference_DriverTrafficIsUnaffected(t *testing.T) {
	const phys = "phys_trd"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startTableRefProxy(t, phys, "enforce",
		testenv.WithExtraDatabases("drv1"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "drv1", registry.DbAuthOwner),
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = signer },
	)
	conn := openSignedConn(t, proxy.Addr, signer)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		auth.DriverSettingKey: clickhouse.CustomSetting{Value: "1"},
	}))
	for _, q := range []string{
		"CREATE TABLE drv1.d (a UInt64) ENGINE = MergeTree ORDER BY a",
		"INSERT INTO drv1.d SELECT number FROM numbers(3)",
		"SELECT name FROM system.tables WHERE database = '" + phys + "' AND name LIKE 'drv1.%'",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("driver %s: %v", q, err)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM drv1.d WHERE a IN drv1.d").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
}
