package integration

import (
	"context"
	"sort"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
)

// TestSystemTables_TenantRowFilter (housegate/housegate#218): two accounts
// share one physical database, each owning one logical database. Every
// tenant reaches ClickHouse as the same user, so ClickHouse's own filtering
// of the class (c) system tables separates nothing; the rewriter serves each
// read of them from a derived table that keeps only the rows of the caller's
// own logical databases. Each account must see its own tables, columns,
// indices, projections, parts and mutations there and none of the other
// account's, with the physical shape (physical database, prefixed names)
// the Sentio driver reads unchanged.
func TestSystemTables_TenantRowFilter(t *testing.T) {
	const phys = "phys_sysflt"
	alice, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	bob, err := auth.NewRelaySigner(authTestKey2)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startTableRefProxy(t, phys, "enforce",
		testenv.WithExtraDatabases("alicedb", "bobdb"),
		authProxyConfig([]string{alice.Address(), bob.Address()}, false),
		testenv.WithDatabasePermission(alice.Address(), "alicedb", registry.DbAuthOwner),
		testenv.WithDatabasePermission(bob.Address(), "bobdb", registry.DbAuthOwner),
	)
	ctx := context.Background()

	type tenant struct {
		db   string
		conn clickhouse.Conn
	}
	tenants := []tenant{
		{"alicedb", openSignedConn(t, proxy.Addr, alice)},
		{"bobdb", openSignedConn(t, proxy.Addr, bob)},
	}
	for _, tn := range tenants {
		for _, q := range []string{
			"CREATE TABLE " + tn.db + ".t (a UInt64, b String, INDEX idx_b b TYPE bloom_filter GRANULARITY 1, " +
				"PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a",
			"INSERT INTO " + tn.db + ".t SELECT number, toString(number) FROM numbers(10)",
			"ALTER TABLE " + tn.db + ".t DELETE WHERE a = 1 SETTINGS mutations_sync = 1",
		} {
			if err := tn.conn.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %s: %v", tn.db, q, err)
			}
		}
	}

	// names reads one String column; each query keeps the driver's own
	// physical-name filter so only this physical database's rows are in play.
	// Signed test queries carry no bind arguments, so `?` is the literal.
	names := func(conn clickhouse.Conn, sql string) []string {
		t.Helper()
		rows, err := conn.Query(ctx, strings.ReplaceAll(sql, "?", "'"+phys+"'"))
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatalf("%s: scan: %v", sql, err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		sort.Strings(out)
		return out
	}
	reads := []string{
		"SELECT name FROM system.tables WHERE database = ?",
		"SELECT DISTINCT table FROM system.columns WHERE database = ?",
		"SELECT table FROM system.data_skipping_indices WHERE database = ?",
		"SELECT table FROM system.projections WHERE database = ?",
		"SELECT DISTINCT table FROM system.parts WHERE database = ? AND active",
		"SELECT table FROM system.mutations WHERE database = ?",
	}
	for _, tn := range tenants {
		own := tn.db + ".t"
		for _, sql := range reads {
			got := names(tn.conn, sql)
			if len(got) != 1 || got[0] != own {
				t.Errorf("%s: %s = %q, want only its own table %q", tn.db, sql, got, own)
			}
		}
		// system.databases is filtered to the caller's physical databases.
		if got := names(tn.conn, "SELECT name FROM system.databases WHERE name = ? OR 1"); strings.Join(got, ",") != phys {
			t.Errorf("%s: system.databases = %q, want only %q", tn.db, got, phys)
		}
	}
}
