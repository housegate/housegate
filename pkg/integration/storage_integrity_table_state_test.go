package integration

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

// statusRegistry is the agent's view of the same fake table state the server
// reads: it answers sentio_getStorageIntegrityTableStatus from the snapshot,
// the way sentio-node will (spec 2026-09-24 §10.2, §13).
type statusRegistry struct {
	*network.InMemoryNetworkState
	state sitable.TableState
}

func (r statusRegistry) StorageIntegrityTableStatus(_ context.Context, database, table string) (registry.TableStatus, error) {
	snap := r.state.Current()
	t := snap.Lookup(database, table)
	out := registry.TableStatus{Status: t.Status.String(), RefusedCode: t.RefusedCode, RefusedReason: t.RefusedReason, RegistryVersion: snap.Version()}
	if t.Status == sitable.Active || t.Status == sitable.Gone {
		js, err := json.Marshal(t.Schema)
		if err != nil {
			return registry.TableStatus{}, err
		}
		out.SchemaJSON, out.SchemaHash = string(js), t.SchemaHash
	}
	return out, nil
}

var connectionIDLine = regexp.MustCompile(`(?m)^[0-9]+$`)

// requireSameConnection runs `SELECT connectionId()` around the refused
// statements and requires both answers to match: clickhouse-client kept its
// connection across every refusal (plan ruling R3).
func requireSameConnection(t *testing.T, out string) {
	t.Helper()
	ids := connectionIDLine.FindAllString(out, -1)
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("connectionId() before/after = %v, want one unchanged connection\nout: %s", ids, out)
	}
}

// TestStorageIntegrityTableStateLifecycle is spec 2026-09-24 §11.3: a fake
// TableState drives one table through Pending, Active, Gone and Purged with
// real ClickHouse, the native rewriter, the signed agent lane and the CLI.
func TestStorageIntegrityTableStateLifecycle(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; fetch the contract-V2 library with `go run ./cmd fetch-rewriter-lib --tag` at the tag .github/workflows/ci.yml fetches, and pass --test_env")
	}
	bin := testenv.ClickHouseCLI(t)
	ctx := context.Background()
	const (
		phys      = "phys_ts"
		networkID = "itest-net-table-state"
	)
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"DROP DATABASE IF EXISTS " + phys,
		"CREATE DATABASE " + phys,
		"CREATE DATABASE IF NOT EXISTS hg_safe",
		"CREATE DATABASE IF NOT EXISTS hg_unsafe",
		"DROP TABLE IF EXISTS hg_safe.tsdb__t",
		"DROP TABLE IF EXISTS hg_unsafe.tsdb__t",
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

	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	schema := payloadexec.TableSchema{TableID: "tsdb.t", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}}
	activeT := sitable.Table{ID: "tsdb.t", Status: sitable.Active, Schema: schema, SchemaHash: payloadexec.TableSchemaHash(networkID, schema)}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "tsdb.t", Status: sitable.Pending})
	consumer := &capturingConsumer{}

	server := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("tsdb"),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), "tsdb", registry.DbAuthOwner),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Enabled = &enabled
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityTableState = state
			opts.StorageIntegrityAdmissionConsumer = consumer
		},
	)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.NetworkState = statusRegistry{InMemoryNetworkState: opts.NetworkState.(*network.InMemoryNetworkState), state: state}
		},
	)
	run := func(query string) (string, error) {
		t.Helper()
		return testenv.RunCLI(t, bin, agentProxy.Addr, "", query)
	}
	mustRun := func(query string) string {
		t.Helper()
		out, err := run(query)
		if err != nil {
			t.Fatalf("%q: %v\nout: %s", query, err, out)
		}
		return out
	}
	mustRefuse := func(query, code, prefix string) {
		t.Helper()
		out, err := run(query)
		if err == nil || !strings.Contains(out, "Code: "+code+".") || !strings.Contains(out, prefix) {
			t.Fatalf("%q: err=%v, want code %s and %q\nout: %s", query, err, code, prefix, out)
		}
	}
	const create = "CREATE TABLE tsdb.t (id UInt64, region String) ENGINE = MergeTree ORDER BY id"

	// 1. CREATE, then Pending: data reads and writes are retryable, metadata is
	// allowed. The test creates hg_* in place of the data plane, which ensures
	// them when AddTable commits, so they exist while the table is Pending.
	mustRun(create)
	for _, q := range []string{
		"CREATE TABLE hg_unsafe.tsdb__t (_hg_row_id FixedString(32), id UInt64, region String) ENGINE = MergeTree ORDER BY id SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0",
		"CREATE TABLE hg_safe.tsdb__t AS hg_unsafe.tsdb__t",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	mustRefuse("SELECT count() FROM tsdb.t", "733", "storage_integrity: table tsdb.t is pending activation (retryable)")
	out, err := testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO tsdb.t FORMAT CSV", "1,eu\n")
	if err == nil || !strings.Contains(out, "Code: 733.") || !strings.Contains(out, "storage_integrity: table tsdb.t is pending activation (retryable)") {
		t.Fatalf("a Pending INSERT must pass the agent unsigned and be refused retryably: err=%v\nout: %s", err, out)
	}
	// No table is Active, so the engine knows the protected databases only
	// from reserved_databases: a direct write into hg_unsafe is refused by the
	// rewriter (a RejectedError, code 403) and lands no row.
	out, err = testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO hg_unsafe.tsdb__t FORMAT CSV", "0123456789abcdef0123456789abcdef,1,eu\n")
	if err == nil || !strings.Contains(out, "Code: 403.") || !strings.Contains(out, "storage-integrity physical table hg_unsafe.tsdb__t is not directly addressable") {
		t.Fatalf("a direct hg_unsafe INSERT must be refused while the Active set is empty: err=%v\nout: %s", err, out)
	}
	var unsafeRows uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM hg_unsafe.tsdb__t").Scan(&unsafeRows); err != nil || unsafeRows != 0 {
		t.Fatalf("hg_unsafe.tsdb__t rows = %d err=%v, want the refused INSERT to land nothing", unsafeRows, err)
	}
	if got := mustRun("EXISTS TABLE tsdb.t"); got != "1" {
		t.Fatalf("EXISTS of a Pending table must read the ordinary table, got %q", got)
	}
	// Refusals end only the query: clickhouse-client keeps its connection
	// across a 733 and a 392 (plan ruling R3).
	out, _ = testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, "",
		"SELECT connectionId(); SELECT count() FROM tsdb.t; ALTER TABLE tsdb.t ADD COLUMN x UInt8; SELECT connectionId()")
	if !strings.Contains(out, "Code: 733.") || !strings.Contains(out, "Code: 392.") || !strings.Contains(out, "ALTER and RENAME are not supported") {
		t.Fatalf("multiquery must show both refusals\nout: %s", out)
	}
	requireSameConnection(t, out)

	// 2. The table becomes Active and a signed INSERT is admitted over the
	// registry schema.
	state.Set(activeT)
	if out, err := testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", "INSERT INTO tsdb.t FORMAT CSV", "1,eu\n"); err != nil {
		t.Fatalf("signed INSERT into the Active table: %v\nout: %s", err, out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	if len(consumer.seen) != 1 || consumer.seen[0].SchemaHash != activeT.SchemaHash || consumer.seen[0].TableID != "tsdb.t" {
		consumer.mu.Unlock()
		t.Fatalf("admissions = %+v, want one over the registry schema", consumer.seen)
	}
	consumer.mu.Unlock()
	if got := mustRun("SELECT count() FROM tsdb.t"); got != "0" {
		t.Fatalf("an Active read is served from hg_safe, which holds no rows yet; got %q", got)
	}

	// 3. DROP succeeds under contract V2 and drops only the ordinary table;
	// once the host retires the table it is Gone: unknown to reads, and a
	// same-name CREATE is retryable.
	mustRun("DROP TABLE tsdb.t")
	var ordinary uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = '"+phys+"' AND name = 'tsdb.t'").Scan(&ordinary); err != nil || ordinary != 0 {
		t.Fatalf("the ordinary physical table must be gone: n=%d err=%v", ordinary, err)
	}
	var protocol uint64
	if err := seed.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database IN ('hg_safe', 'hg_unsafe') AND name = 'tsdb__t'").Scan(&protocol); err != nil || protocol != 2 {
		t.Fatalf("hg_* must stay for the data plane to purge: n=%d err=%v", protocol, err)
	}
	gone := activeT
	gone.Status = sitable.Gone
	state.Set(gone)
	mustRefuse("SELECT count() FROM tsdb.t", "60", "Table tsdb.t does not exist")
	mustRefuse(create, "733", "storage_integrity: table tsdb.t is still being purged; retry CREATE later (retryable)")
	out, _ = testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, "",
		"SELECT connectionId(); SELECT count() FROM tsdb.t; SELECT connectionId()")
	if !strings.Contains(out, "Code: 60.") {
		t.Fatalf("multiquery must show the Gone refusal\nout: %s", out)
	}
	requireSameConnection(t, out)

	// 4. Purged: the name is unrecorded again (default deny: Pending), and
	// the same-name CREATE succeeds.
	state.Set(sitable.Table{ID: "tsdb.t", Status: sitable.Pending})
	mustRun(create)
	if err := seed.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = '"+phys+"' AND name = 'tsdb.t'").Scan(&ordinary); err != nil || ordinary != 1 {
		t.Fatalf("the Purged CREATE must create the ordinary physical table %s.\"tsdb.t\" again: n=%d err=%v", phys, ordinary, err)
	}
}
