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

// guardRefusal is the full client-facing text of a table-reference guard
// refusal (pkg/plugins/tablerefguard).
func guardRefusal(rule, detail string) string {
	return "table-reference guard: " + rule + ": " + detail + "; the rewriter applies the same policy"
}

// TestTableReference_RefusalsReachTheClientAsExceptions runs each refusal with
// the guard observing (the engine answers) and enforcing (the guard answers
// first where a rule applies); the connection survives every refusal.
func TestTableReference_RefusalsReachTheClientAsExceptions(t *testing.T) {
	const phys = "phys_tr2"
	cases := []struct{ sql, engine, guard string }{
		{"SELECT * FROM {p:Identifier}", "query parameters are not supported in a database or table position",
			guardRefusal("identifier_placeholder", "ClickHouse Identifier query parameters are not accepted")},
		{"SELECT * FROM db1.o WHERE a IN " + phys + ".`db2.x`", "protected database " + phys + " is not addressable",
			guardRefusal("physical_database", "protected database "+phys+" is not addressable")},
		{"SELECT * FROM merge('" + phys + "', '.*')", "protected database " + phys + " is not addressable",
			guardRefusal("carrier_callable", "table function merge is not accepted")},
		{"USE " + phys, "protected database " + phys + " is not addressable",
			guardRefusal("physical_database", "protected database "+phys+" is not addressable")},
		{"SELECT * FROM hg_promote.`db2.x`", "protected database hg_promote is not addressable",
			guardRefusal("reserved_name", "reserved database hg_promote is not addressable")},
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
	// seed is the package's own testcontainers ClickHouse (chEnv), so the
	// whole-database drops touch nothing outside this test run.
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
// (housegate#218) pass with the guard enforcing, and the guard still refuses
// the driver a physical-database qualifier.
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
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("driver %s: %v", q, err)
		}
	}
	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM system.tables WHERE database = '"+phys+"' AND name LIKE 'drv1.%'").Scan(&name); err != nil {
		t.Fatalf("driver physical-name metadata read: %v", err)
	}
	if name != "drv1.d" {
		t.Fatalf("metadata read name = %q, want drv1.d (the physical table of drv1.d)", name)
	}
	want := guardRefusal("physical_database", "protected database "+phys+" is not addressable")
	if err := conn.Exec(ctx, "SELECT * FROM "+phys+".`drv1.d`"); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("driver physical qualifier: err = %v, want it to contain %q (the guard runs on driver sessions)", err, want)
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM drv1.d WHERE a IN drv1.d").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want 3", n)
	}
}

// TestTableReference_GuardRunsOnForwardedFromPeerSessions (final review I1):
// the host that receives a forward-pivoted session owns its original client
// SQL, so its guard checks it against the host's own physical database. The
// origin here has no physical_database (G2 inactive there, as on a
// router-only origin that runs no guard at all), so only the host can refuse
// a qualifier naming the host's physical database.
func TestTableReference_GuardRunsOnForwardedFromPeerSessions(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	const (
		indexerA uint64 = 1
		indexerB uint64 = 2
		dbB             = "tr_fwd_db"
		physB           = "phys_tr_fwd_b"
	)
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, db := range []string{dbB, physB} {
		if err := seed.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
			t.Fatalf("seed %s: %v", db, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+dbB)
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+physB)
	})
	rewriterB, _ := testenv.WithRewriterMock(t)
	hostB := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterB,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithRelayKey(authTestKey1),
		testenv.WithIndexerID(indexerB),
		testenv.WithCredentialReplace(),
		testenv.WithExtraDatabases(dbB),
		testenv.WithLogicalDatabaseAt(dbB, indexerB),
		testenv.WithDatabasePermission(signer.Address(), dbB, registry.DbAuthOwner),
		testenv.WithConfigMutator(func(cfg *config.Config) { cfg.Rewriter.PhysicalDatabase = physB }),
	)
	originA := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithRelayKey(authTestKey1),
		testenv.WithIndexerID(indexerA),
		testenv.WithPeerAt(indexerB, hostB),
		testenv.WithExtraDatabases(dbB),
		testenv.WithLogicalDatabaseAt(dbB, indexerB),
	)
	conn := openSignedConnPinnedDB(t, originA.Addr, signer, dbB)
	var one uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1 through the forward pivot: %v", err)
	}
	want := guardRefusal("physical_database", "protected database "+physB+" is not addressable")
	if err := conn.Exec(ctx, "SELECT * FROM "+physB+".`"+dbB+".x`"); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want the receiving host's guard refusal %q", err, want)
	}
}
