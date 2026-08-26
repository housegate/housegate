package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/rewriter"
)

type siReadStateStub struct {
	mu    sync.Mutex
	parts map[string][]string
}

func (s *siReadStateStub) PromotedUnsafeParts(tableID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.parts[tableID]...), nil
}

func (s *siReadStateStub) set(tableID string, parts ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[tableID] = parts
}

// TestStorageIntegrityRead_SafeAndUnsafeLatest drives Spec G end to end
// through the native rewriter engine: an SI table's rows land in
// hg_unsafe (simulated staged write), are visible immediately under
// unsafe_latest, invisible under safe until a (simulated) promotion copies
// them into hg_safe, and never double-counted while the promoted unsafe
// part awaits cleanup. Needs the FFI lib (skips otherwise).
func TestStorageIntegrityRead_SafeAndUnsafeLatest(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.9.0` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"DROP TABLE IF EXISTS hg_unsafe.db1__t",
		"CREATE TABLE hg_unsafe.db1__t (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__t AS hg_unsafe.db1__t ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_unsafe.db1__t",
		"SYSTEM STOP MERGES hg_safe.db1__t",
		"INSERT INTO hg_unsafe.db1__t VALUES (repeat('a', 32), 1), (repeat('b', 32), 2)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})
	var unsafePart string
	if err := seed.QueryRow(ctx, "SELECT name FROM system.parts WHERE database = 'hg_unsafe' AND table = 'db1__t' AND active").Scan(&unsafePart); err != nil {
		t.Fatalf("unsafe part: %v", err)
	}

	port := &siReadStateStub{parts: map[string][]string{}}
	// The ingress lane needs an allowed signer to pass Config.Validate; reuse
	// the same key the envelope-v2 agent test signs with rather than weakening
	// validation. No statement is signed here - this test only reads.
	ingressSigner, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(port),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.t"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
			// Spec P D3: the read-mode surface must behave identically with the
			// signed ingress configured. Without this the only read-mode
			// integration test runs on a deployment where RejectUserSettings
			// is never reached, so SQL_x_read_mode's owned-key membership
			// (Spec K D6) is never exercised end to end.
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = "itest-net-read"
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{ingressSigner.Address()}
		}),
		func(_ *config.Config, opts *housegate.Options) {
			// An enabled ingress needs a consumer; this test issues no signed
			// statement, so nothing is ever handed to it.
			opts.StorageIntegrityAdmissionConsumer = &capturingConsumer{}
		},
	)
	conn := openConn(t, proxy.Addr)
	count := func(mode string) (uint64, error) {
		qctx := ctx
		if mode != "" {
			qctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"SQL_x_read_mode": clickhouse.CustomSetting{Value: mode}}))
		}
		var n uint64
		err := conn.QueryRow(qctx, "SELECT count() FROM db1.t").Scan(&n)
		return n, err
	}
	mustCount := func(mode string, want uint64) {
		t.Helper()
		n, err := count(mode)
		if err != nil {
			t.Fatalf("count(mode=%q): %v", mode, err)
		}
		if n != want {
			t.Fatalf("count(mode=%q) = %d, want %d", mode, n, want)
		}
	}

	// 1. Staged rows: invisible in safe (the default), visible in unsafe_latest.
	mustCount("", 0)
	mustCount("safe", 0)
	mustCount("unsafe_latest", 2)

	// 2. Simulated promotion: rows copied into hg_safe; the unsafe part is
	// now "promoted, not yet cleaned" → excluded from unsafe_latest.
	if err := seed.Exec(ctx, "INSERT INTO hg_safe.db1__t SELECT * FROM hg_unsafe.db1__t"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	port.set("db1.t", unsafePart)
	mustCount("safe", 2)
	mustCount("unsafe_latest", 2) // NOT 4 — the promoted unsafe part is excluded

	// 3. Simulated cleanup: unsafe part dropped, exclusion list emptied.
	if err := seed.Exec(ctx, fmt.Sprintf("ALTER TABLE hg_unsafe.db1__t DROP PART '%s'", unsafePart)); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	port.set("db1.t")
	mustCount("unsafe_latest", 2)

	// 4. Reserved column is unaddressable; SELECT * hides it.
	starRows, err := conn.Query(ctx, "SELECT * FROM db1.t LIMIT 1")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	if got := starRows.Columns(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("SELECT * columns = %v, want [a] (_hg_row_id must be hidden)", got)
	}
	starRows.Close()
	if err := conn.QueryRow(ctx, "SELECT _hg_row_id FROM db1.t").Scan(new(string)); err == nil || !strings.Contains(err.Error(), "reserved column _hg_row_id is not addressable") {
		t.Fatalf("reserved column must be rejected with the rewriter message, got %v", err)
	}
	// 5. Invalid mode is an Exception, not a silent fallback.
	if _, err := count("latest"); err == nil || !strings.Contains(err.Error(), "SQL_x_read_mode") {
		t.Fatalf("invalid SQL_x_read_mode must be rejected, got %v", err)
	}
	// 6. Non-lane write is refused with the spec message (no ingress configured here).
	if err := conn.Exec(ctx, "ALTER TABLE db1.t DELETE WHERE a = 1"); err == nil || !strings.Contains(err.Error(), "accepts writes only through the signed statement lane") {
		t.Fatalf("SI write must be refused, got %v", err)
	}
	// 7. DESCRIBE hides the reserved column.
	rows, err := conn.Query(ctx, "DESCRIBE TABLE db1.t")
	if err != nil {
		t.Fatalf("DESCRIBE: %v", err)
	}
	var names []string
	for rows.Next() {
		var name, typ, dt, de, cm, ce, te string
		if err := rows.Scan(&name, &typ, &dt, &de, &cm, &ce, &te); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("DESCRIBE rows: %v", err)
	}
	if strings.Join(names, ",") != "a" {
		t.Fatalf("DESCRIBE columns = %v, want [a]", names)
	}
}

// TestStorageIntegrityRead_CriticalStatementsAreRefused is the regression
// test for the two Critical Spec I findings: SYSTEM START MERGES could mutate
// the candidate-part boundary, and TRUNCATE DATABASE could empty authoritative
// committed state after a target-less engine rejection. Both statements must
// be Exceptions and both physical tables must remain untouched.
func TestStorageIntegrityRead_CriticalStatementsAreRefused(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.9.0` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_guard"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__guard",
		"DROP TABLE IF EXISTS hg_unsafe.db1__guard",
		"CREATE TABLE hg_unsafe.db1__guard (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__guard AS hg_unsafe.db1__guard ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_unsafe.db1__guard",
		"SYSTEM STOP MERGES hg_safe.db1__guard",
		"INSERT INTO hg_unsafe.db1__guard VALUES (repeat('a', 32), 1)",
		"INSERT INTO hg_unsafe.db1__guard VALUES (repeat('b', 32), 2)",
		"INSERT INTO hg_safe.db1__guard VALUES (repeat('c', 32), 3)",
	} {
		if err := seed.Exec(ctx, query); err != nil {
			t.Fatalf("seed %q: %v", query, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP TABLE IF EXISTS hg_safe.db1__guard")
		_ = seed.Exec(ctx, "DROP TABLE IF EXISTS hg_unsafe.db1__guard")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	activeParts := func(database, table string) uint64 {
		t.Helper()
		var count uint64
		if err := seed.QueryRow(ctx,
			"SELECT count() FROM system.parts WHERE database = ? AND table = ? AND active",
			database, table).Scan(&count); err != nil {
			t.Fatalf("system.parts(%s.%s): %v", database, table, err)
		}
		return count
	}
	rows := func(table string) uint64 {
		t.Helper()
		var count uint64
		if err := seed.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count(%s): %v", table, err)
		}
		return count
	}

	unsafePartsBefore := activeParts("hg_unsafe", "db1__guard")
	safePartsBefore := activeParts("hg_safe", "db1__guard")
	unsafeRowsBefore := rows("hg_unsafe.db1__guard")
	safeRowsBefore := rows("hg_safe.db1__guard")
	if unsafePartsBefore != 2 || safePartsBefore != 1 || unsafeRowsBefore != 2 || safeRowsBefore != 1 {
		t.Fatalf("seed shape = unsafe(parts=%d rows=%d) safe(parts=%d rows=%d), want unsafe(2,2) safe(1,1)",
			unsafePartsBefore, unsafeRowsBefore, safePartsBefore, safeRowsBefore)
	}

	port := &siReadStateStub{parts: map[string][]string{}}
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(port),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.guard"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	conn := openConn(t, proxy.Addr)

	for _, tc := range []struct {
		name        string
		sql         string
		wantMessage string
	}{
		{"system start merges on the unsafe namespace", "SYSTEM START MERGES hg_unsafe.db1__guard",
			"storage-integrity physical table hg_unsafe.db1__guard is not directly addressable"},
		{"system stop merges on the safe namespace", "SYSTEM STOP MERGES hg_safe.db1__guard",
			"storage-integrity physical table hg_safe.db1__guard is not directly addressable"},
		{"truncate the safe database", "TRUNCATE DATABASE hg_safe",
			"storage-integrity physical database hg_safe is not directly addressable"},
		{"unmodelled statement naming nothing storage-integrity", "SYSTEM RELOAD CONFIG",
			"statement class is not modelled by the rewriter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Exec(ctx, tc.sql)
			if err == nil {
				t.Fatalf("%q must be refused with an Exception", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("%q error = %v, want it to contain %q", tc.sql, err, tc.wantMessage)
			}
		})
	}

	if got := activeParts("hg_unsafe", "db1__guard"); got != unsafePartsBefore {
		t.Fatalf("hg_unsafe active parts = %d, want %d; merges must still be stopped", got, unsafePartsBefore)
	}
	if got := activeParts("hg_safe", "db1__guard"); got != safePartsBefore {
		t.Fatalf("hg_safe active parts = %d, want %d", got, safePartsBefore)
	}
	if got := rows("hg_unsafe.db1__guard"); got != unsafeRowsBefore {
		t.Fatalf("hg_unsafe rows = %d, want %d", got, unsafeRowsBefore)
	}
	if got := rows("hg_safe.db1__guard"); got != safeRowsBefore {
		t.Fatalf("hg_safe rows = %d, want %d; TRUNCATE DATABASE must never reach ClickHouse", got, safeRowsBefore)
	}

	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM db1.guard").Scan(&count); err != nil {
		t.Fatalf("SELECT after refusals: %v", err)
	}
	if count != safeRowsBefore {
		t.Fatalf("safe-mode count = %d, want %d", count, safeRowsBefore)
	}
}

// operatorGuardMessage is the distinctive half of pkg/plugins/sireserved's
// refusal. It is deliberately NOT a substring of the rewriter's
// "storage-integrity physical database ... is not directly addressable", so
// asserting it proves the operator guard fired rather than the rewriter — and
// therefore that the maintenance flag really took effect on the session.
const operatorGuardMessage = "is not addressable through the proxy"

// TestStorageIntegrityRead_HeredocCannotHideAReservedName is the Spec N D1
// regression against a real ClickHouse. A maintenance session skips SQL
// rewrite by design (Spec I D6), so pkg/plugins/sireserved is the only control
// on this path; before Spec N a comment marker inside a ClickHouse heredoc
// ($$...$$ / $tag$...$tag$) blanked the rest of the statement from both scan
// surfaces and the guard saw nothing, so the statement executed against the
// protected namespace and returned a result set.
//
// The maintenance flag is set only by authplugin after a JWS verify whose
// signer equals the host-injected Options.Signer.Address() (build.go:374-385),
// and testenv.StartServerProxy never sets opts.Signer — so the session is
// built explicitly here. Steps 1 and 2 below prove the flag actually took
// effect before any negative assertion runs; without them every "must be
// refused" case could pass vacuously on a session that was never privileged.
func TestStorageIntegrityRead_HeredocCannotHideAReservedName(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.9.0` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_heredoc"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	seed := openConnNoDB(t, chEnv.Addr)
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__hd",
		"CREATE TABLE hg_safe.db1__hd (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"SYSTEM STOP MERGES hg_safe.db1__hd",
		"INSERT INTO hg_safe.db1__hd VALUES (repeat('a', 32), 1)",
	} {
		if err := seed.Exec(ctx, query); err != nil {
			t.Fatalf("seed %q: %v", query, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "db1", registry.DbAuthOwner),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthOwner),
		// Options.Signer is the host attestation build.go requires before the
		// validator will honour SQL_sentio_maintenance (build.go:374-385).
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = signer },
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.hd"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	conn := openSignedConn(t, proxy.Addr, signer)
	maintenance := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"SQL_sentio_maintenance": clickhouse.CustomSetting{Value: "1"},
	}))

	// Step 1: the session is alive and not failing for unrelated reasons. If
	// this errors, every negative assertion below would "pass" vacuously.
	var alive uint8
	if err := conn.QueryRow(maintenance, "SELECT 1").Scan(&alive); err != nil {
		t.Fatalf("the maintenance session must serve an ordinary query: %v", err)
	}

	// Step 2: the maintenance flag actually took effect. A plain mention of the
	// reserved database is refused today, and the refusal carries the OPERATOR
	// GUARD's message rather than the rewriter's — which is only possible if
	// rewrite was skipped, i.e. if the session really is privileged. Without
	// this the whole test could pass on an ordinary session.
	err = conn.Exec(maintenance, "SELECT count() FROM hg_safe.db1__hd")
	if err == nil {
		t.Fatal("a reserved name on a maintenance session must be refused; the maintenance wiring is broken")
	}
	if !strings.Contains(err.Error(), operatorGuardMessage) {
		t.Fatalf("refusal must come from the operator guard (so the maintenance flag took effect), got: %v", err)
	}

	// Step 3: the heredoc forms. PRE-FIX each of these reached ClickHouse and
	// returned a result set, because the comment marker inside the heredoc
	// blanked the statement from both scan surfaces.
	for _, tc := range []struct{ name, sql string }{
		{"heredoc hiding a line comment", "SELECT $$--$$ AS x, count() FROM hg_safe.db1__hd"},
		{"tagged heredoc hiding a slash comment", "SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__hd"},
		{"heredoc hiding a hash comment", "SELECT $$#$$ AS x, a FROM hg_safe.db1__hd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Exec(maintenance, tc.sql)
			if err == nil {
				t.Fatalf("%q must reach the client as an Exception, not a result set", tc.sql)
			}
			if !strings.Contains(err.Error(), "hg_safe") {
				t.Fatalf("%q error = %v, want it to name the reserved database", tc.sql, err)
			}
			if !strings.Contains(err.Error(), operatorGuardMessage) {
				t.Fatalf("%q must be refused by the operator guard, got: %v", tc.sql, err)
			}
		})
	}

	// Step 4: the guard is targeted, not a blanket refusal of every heredoc on
	// a maintenance session.
	if err := conn.Exec(maintenance, "SELECT $$ordinary$$ AS x"); err != nil {
		t.Fatalf("an ordinary heredoc on a maintenance session must pass: %v", err)
	}
}

// TestStorageIntegrityRead_SessionLevelSetIsRefused records Spec P D2: an
// SI-configured deployment refuses session-level SET. This is a consequence of
// Spec I D1's catch-all (SET is modelled by no handler in either engine, so it
// reaches the catch-all and returns UnsupportedStatement) plus Spec I D3
// (HouseGate treats any non-Success as a rejection when SI tables are
// configured). It matters because settings_hash commits to the EMPTY user
// settings set: a session-level `SET async_insert=1` issued before an SI INSERT
// would be invisible to both the agent signer and the server ingress, which
// would honestly sign EmptySettingsHash for a statement that then executes
// under different settings. The refusal is what makes that unreachable, and
// this test is what stops a refactor from turning it back into an accident.
func TestStorageIntegrityRead_SessionLevelSetIsRefused(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; run `go run ./cmd fetch-rewriter-lib --tag v0.9.0` and pass --test_env")
	}
	ctx := context.Background()
	const phys = "phys_si_set"
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.db1__t",
		"DROP TABLE IF EXISTS hg_unsafe.db1__t",
		"CREATE TABLE hg_unsafe.db1__t (_hg_row_id FixedString(32), a UInt32) ENGINE = MergeTree ORDER BY a",
		"CREATE TABLE hg_safe.db1__t AS hg_unsafe.db1__t ENGINE = MergeTree ORDER BY a",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_safe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS hg_unsafe")
		_ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys)
	})

	withSI := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithStorageIntegrityReadState(&siReadStateStub{parts: map[string][]string{}}),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Tables = []string{"db1.t"}
			cfg.StorageIntegrity.Read.DefaultMode = string(rewriter.ReadModeSafe)
		}),
	)
	withoutSI := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("db1"),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
		}),
	)

	err := openConn(t, withSI.Addr).Exec(ctx, "SET async_insert = 1")
	if err == nil {
		t.Fatal("an SI-configured deployment must refuse session-level SET: settings_hash commits to the empty user-settings set")
	}
	if !strings.Contains(err.Error(), "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded") {
		t.Fatalf("SET must be refused with Spec I D1's generic catch-all message, got %v", err)
	}

	if err := openConn(t, withoutSI.Addr).Exec(ctx, "SET async_insert = 1"); err != nil {
		t.Fatalf("without SI configured the legacy pass-through must be unchanged, got %v", err)
	}
}
