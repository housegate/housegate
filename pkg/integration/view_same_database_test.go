package integration

import (
	"context"
	"strings"
	"testing"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
)

// TestViewSameDatabase_EndToEnd (housegate#330): through the native engine,
// the permission gate and a real ClickHouse, an account that owns two
// databases cannot make a view of one serve the other, neither by creating
// it with a body or TO table in the other database nor by creating it at
// home and then renaming or exchanging it across. Views, materialized views
// and renames within one database keep working.
func TestViewSameDatabase_EndToEnd(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startTableRefProxy(t, "phys_v330", "enforce",
		testenv.WithExtraDatabases("va1", "va2"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "va1", registry.DbAuthOwner),
		testenv.WithDatabasePermission(signer.Address(), "va2", registry.DbAuthOwner),
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = signer },
	)
	conn := openSignedConn(t, proxy.Addr, signer)
	// An ordinary signed session: driver sessions (indexer-signed) skip the
	// commit gate, and the driver only builds views in its own database.
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	count := func(q string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}
	refused := func(q, want string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want it to contain %q", q, err, want)
		}
	}

	exec("CREATE TABLE va1.t (a UInt64) ENGINE = MergeTree ORDER BY a")
	exec("CREATE TABLE va1.dst (a UInt64) ENGINE = MergeTree ORDER BY a")
	exec("CREATE TABLE va2.t (a UInt64) ENGINE = MergeTree ORDER BY a")
	exec("CREATE TABLE va2.w (a UInt64) ENGINE = MergeTree ORDER BY a")
	exec("INSERT INTO va1.t SELECT number FROM numbers(3)")

	// Within one database: a view, a materialized view and a rename work.
	exec("CREATE VIEW va1.v AS SELECT a FROM va1.t")
	exec("CREATE MATERIALIZED VIEW va1.mv TO va1.dst AS SELECT a FROM va1.t")
	exec("INSERT INTO va1.t VALUES (10)")
	if n := count("SELECT count() FROM va1.v"); n != 4 {
		t.Fatalf("va1.v count = %d, want 4", n)
	}
	if n := count("SELECT count() FROM va1.dst"); n != 1 {
		t.Fatalf("va1.dst count = %d, want 1 (the insert after the materialized view)", n)
	}
	exec("RENAME TABLE va1.v TO va1.v2")

	const create = "may only reference its own database"
	refused("CREATE VIEW va1.x AS SELECT a FROM va2.t", create)
	refused("CREATE VIEW va1.x AS SELECT a FROM va1.t WHERE a IN (SELECT a FROM va2.t)", create)
	refused("CREATE MATERIALIZED VIEW va1.mx TO va1.dst AS SELECT a FROM va2.t", create)
	refused("CREATE MATERIALIZED VIEW va1.mx TO va2.w AS SELECT a FROM va1.t", create)
	refused("CREATE VIEW va1.x AS SELECT number AS a FROM system.numbers", create)

	// Create at home, then move: refused, and nothing moved.
	const move = "cannot move a table or view to another database"
	refused("RENAME TABLE va1.v2 TO va2.v", move)
	refused("EXCHANGE TABLES va1.v2 AND va2.w", move)
	refused("RENAME TABLE va1.mv TO va2.mv", move)
	refused("RENAME TABLE va1.dst TO va2.dst", move)
	if n := count("SELECT count() FROM va2.w"); n != 0 {
		t.Fatalf("va2.w count = %d, want 0 (still the empty table, not the view over va1.t)", n)
	}
	if n := count("SELECT count() FROM va1.v2"); n != 4 {
		t.Fatalf("va1.v2 count = %d, want 4 (the view stayed in va1)", n)
	}
	if err := conn.Exec(ctx, "SELECT count() FROM va2.v"); err == nil {
		t.Fatal("va2.v exists; the refused rename moved the view")
	}
}
