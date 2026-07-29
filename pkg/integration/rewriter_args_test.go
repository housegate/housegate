package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/cfgtypes"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	authplugin "github.com/housegate/housegate/pkg/plugins/auth"
	"github.com/housegate/housegate/pkg/registry"
)

// TestRewriter_PassesDynamicArgs pins the proxy → rewriter wire
// contract for dynamic_args: when auth is on and the signer holds a
// permission bit on a logical database, the proxy must ship a
// RewriteTableDynamicArgs whose database_map contains exactly that
// logical → physical pair (and known_physical_databases lists the
// physical), so the rewriter can do its lookup against the same
// authority the proxy used to authenticate.
//
// Regression catches:
//   - auth-on default-deny: proxy used to ship every known logical
//     regardless of permissions; we want a strict per-account map now.
//   - PhysicalDatabase not threaded through: the field plumbing from
//     cfg → factory.options → buildDynamicArgs is brittle (renames,
//     refactors).
//   - upstream_logical_database_in_context drift: the rewriter relies
//     on this for unqualified target resolution. The hello.Database
//     the test connection uses (chEnv.Database) must surface here.
func TestRewriter_PassesDynamicArgs(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	const phys = "phys_test"
	// Seed the physical database on CH directly. With PhysicalDatabase
	// set, the sessionstate plugin rewrites hello.Database from
	// chEnv.Database → phys before the proxy hands the hello to CH; if
	// phys does not exist, CH rejects the handshake with code=81.
	seedDB := openConnNoDB(t, chEnv.Addr)
	if err := seedDB.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", phys)); err != nil {
		t.Fatalf("seed physical database %s: %v", phys, err)
	}
	t.Cleanup(func() {
		_ = seedDB.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", phys))
	})

	rewriterOpt, mock := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Auth = authplugin.Config{
				Enabled:          true,
				AllowedAddresses: []string{signer.Address()},
				MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
				AllowNoAuth:      false,
			}
			cfg.Rewriter.PhysicalDatabase = phys
		}),
		// Owner grant: buildDatabaseMap requires at least one of
		// Read/Write/Admin/Owner on the database; Owner is the easiest
		// to keep stable across promotion rule changes.
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthOwner),
	)

	conn := openSignedConn(t, proxy.Addr, signer)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("signed SELECT 1: %v", err)
	}

	args := mock.SeenDynamicArgs()
	if len(args) == 0 {
		t.Fatal("rewriter mock received zero dynamic_args — proxy did not ship them")
	}
	got := args[0]
	if got == nil {
		t.Fatal("dynamic_args[0] is nil — proxy attached an option without DynamicArgs")
	}

	// database_map: exactly one entry, chDatabase → phys.
	dbMap := got.GetDatabaseMap()
	if len(dbMap) != 1 {
		t.Errorf("database_map has %d entries, want 1: %v", len(dbMap), dbMap)
	}
	if mapped, ok := dbMap[chEnv.Database]; !ok || mapped != phys {
		t.Errorf("database_map[%q] = %q (ok=%v), want %q", chEnv.Database, mapped, ok, phys)
	}

	// known_physical_databases: contains the configured physical DB.
	found := false
	for _, p := range got.GetKnownPhysicalDatabases() {
		if p == phys {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("known_physical_databases = %v, want to contain %q",
			got.GetKnownPhysicalDatabases(), phys)
	}

	// upstream_logical_database_in_context: the hello.Database (the
	// test connection opens against chEnv.Database).
	if ctx := got.GetUpstreamLogicalDatabaseInContext(); ctx != chEnv.Database {
		t.Errorf("upstream_logical_database_in_context = %q, want %q",
			ctx, chEnv.Database)
	}
}
