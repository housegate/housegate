package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)

// The peer reserved-name guard tests (spec 2026-10-14 §14) run each remote()
// leg shape the rewriter emits directly on ClickHouse, capture from
// system.query_log the secondary queries ClickHouse sends for it, and replay
// each one on HouseGate's internal listener, which pre-flags every session
// peer-trusted and non-forwarded exactly like a rewriter remote() loopback.
const (
	peerReservedPhys = "phys_pr"
	peerReservedDB   = "prdb"
)

// seedPeerReserved creates, directly on ClickHouse, the physical tables the
// legs read: prdb.o (the remote table) and prdb.l (the local side). Both are
// Ordinary in the host's table state.
func seedPeerReserved(t *testing.T) clickhouse.Conn {
	t.Helper()
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"DROP DATABASE IF EXISTS " + peerReservedPhys,
		"CREATE DATABASE " + peerReservedPhys,
		"CREATE TABLE " + peerReservedPhys + ".`prdb.o` (id UInt64, s String, x Float64) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE " + peerReservedPhys + ".`prdb.l` (id UInt64) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO " + peerReservedPhys + ".`prdb.o` VALUES (1, 'a1', 0.5), (2, 'b22', 1.5), (3, 'c333', 2.5)",
		"INSERT INTO " + peerReservedPhys + ".`prdb.l` VALUES (1), (3)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(context.Background(), "DROP DATABASE IF EXISTS "+peerReservedPhys) })
	return seed
}

// startPeerReservedInternal starts an SI-enabled server on the native engine
// (as TestOpenWrites_PeerGuardRefusesGovernedPhysicalReads does) and returns a
// connection to its internal listener. Only the untouched prdb.si is Active,
// so sipeerguard admits every leg below and any 392 is sireserved's.
func startPeerReservedInternal(t *testing.T) clickhouse.Conn {
	t.Helper()
	lib := requireNativeLib(t)
	schema := payloadexec.TableSchema{TableID: peerReservedDB + ".si", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{
		ID: peerReservedDB + ".si", Status: sitable.Active, Schema: schema, SchemaHash: payloadexec.TableSchemaHash("itest-pr", schema),
	})
	_, internal := startServerWithInternal(t, chEnv.Addr,
		testenv.WithExtraDatabases(peerReservedDB),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = peerReservedPhys
			// Storage integrity refuses the transport fail-open switch.
			cfg.Rewriter.FailOpenOnUnavailable = false
			cfg.StorageIntegrity.Enabled = &enabled
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityTableState = state },
	)
	return openConnDB(t, internal, peerReservedPhys)
}

// peerLegQueryID returns a query id unique to this run for one shape.
func peerLegQueryID(shape string) string {
	return fmt.Sprintf("peer-reserved-%s-%d", strings.ReplaceAll(shape, " ", "-"), time.Now().UnixNano())
}

// peerReservedRemote renders the rewriter's remote() leg for one table of the
// physical database on this same ClickHouse (its in-container native port).
func peerReservedRemote(table string) string {
	return fmt.Sprintf("remote('127.0.0.1:9000', '%s', '%s', '%s', '%s')", peerReservedPhys, table, chEnv.User, chEnv.Password)
}

// captureSecondaryQueries runs sql directly on ClickHouse under queryID and
// returns, in start order, the text of every secondary query it caused: the
// system.query_log rows with is_initial_query = 0 for that initial query id.
// Every remote() below names this server, and the shapes set
// prefer_localhost_replica = 0 so the leg crosses the native protocol as it
// does towards another host. The shape's own error is returned; a failure to
// read the log is fatal.
func captureSecondaryQueries(t *testing.T, conn clickhouse.Conn, queryID, sql string) ([]string, error) {
	t.Helper()
	if err := conn.Exec(clickhouse.Context(context.Background(), clickhouse.WithQueryID(queryID)), sql); err != nil {
		return nil, err
	}
	ctx := context.Background()
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("SYSTEM FLUSH LOGS: %v", err)
	}
	rows, err := conn.Query(ctx, "SELECT query FROM system.query_log WHERE type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = '"+queryID+"' ORDER BY event_time_microseconds")
	if err != nil {
		t.Fatalf("read system.query_log: %v", err)
	}
	defer rows.Close()
	var legs []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			t.Fatalf("scan system.query_log: %v", err)
		}
		legs = append(legs, q)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.query_log rows: %v", err)
	}
	return legs, nil
}

// TestPeerReservedGuard_MeasuredRemoteSecondaryQueriesPass is spec 2026-10-14
// §14's regression test: every secondary query ClickHouse sends for a remote()
// leg the rewriter emits — rewriter-go nodes.go ActionRemote in FROM, JOIN and
// IN-operand position, dblevel.go SHOW TABLES and the structure probe, plus a
// nested remote() that ClickHouse pushes into a leg — passes the peer
// reserved-name rule and is admitted on a peer-trusted, non-forwarded session.
// A shape this one server does not send a leg for is logged, not required. The
// captured text is logged for the PR record.
func TestPeerReservedGuard_MeasuredRemoteSecondaryQueriesPass(t *testing.T) {
	seed := seedPeerReserved(t)
	internal := startPeerReservedInternal(t)
	ctx := context.Background()
	var version string
	if err := seed.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	remoteO, remoteL := peerReservedRemote("prdb.o"), peerReservedRemote("prdb.l")
	remoteTables := fmt.Sprintf("remote('127.0.0.1:9000', system, tables, '%s', '%s')", chEnv.User, chEnv.Password)
	local := peerReservedPhys + ".`prdb.l`"
	const viaNetwork = " SETTINGS prefer_localhost_replica = 0"
	for _, shape := range []struct {
		name     string
		sql      string
		required bool // must execute and send at least one leg
	}{
		{"count", "SELECT count() FROM " + remoteO + viaNetwork, true},
		{"filtered star under the qualified alias", "SELECT * FROM " + remoteO + " AS `prdb.o` WHERE id IN (1, 2)" + viaNetwork, true},
		{"backslash literal", `SELECT extract(s, '\\d+') FROM ` + remoteO + viaNetwork, true},
		{"aggregation", "SELECT s, sum(x) FROM " + remoteO + " GROUP BY s ORDER BY s LIMIT 10" + viaNetwork, true},
		{"global join with a local left side", "SELECT l.id, r.s FROM " + local + " AS l GLOBAL JOIN " + remoteO + " AS r USING id" + viaNetwork, true},
		{"global in with a local subquery", "SELECT count() FROM " + remoteO + " WHERE id GLOBAL IN (SELECT id FROM " + local + ")" + viaNetwork, true},
		{"show tables of a remote logical", "SELECT multiIf(startsWith(name, 'prdb.'), substring(name, 6), name) AS name FROM (SELECT name FROM " + remoteTables + " WHERE database = '" + peerReservedPhys + "' AND startsWith(name, 'prdb.'))" + viaNetwork, true},
		// A remote()-left JOIN of another remote() and an IN subquery over
		// another remote() inside a remote outer: ClickHouse pushes the inner
		// remote() into the leg, so a legitimate leg carries a carrier
		// (measured on 25.8.28.1; query_log masks the nested password as
		// '[HIDDEN]', which the guard does not read).
		{"remote JOIN of another remote", "SELECT count() FROM " + remoteO + " AS a JOIN " + remoteL + " AS b ON a.id = b.id" + viaNetwork, true},
		{"remote IN subquery of another remote", "SELECT count() FROM " + remoteO + " WHERE id IN (SELECT id FROM " + remoteL + ")" + viaNetwork, true},
		// rewriter-go renders a remote IN operand as a bare `x IN remote(...)`;
		// ClickHouse 25.8.28.1 and 26.3.37.3 refuse that at the initiator (code
		// 46, a table function is not an IN operand), so no leg is sent:
		// measured, not required.
		{"remote IN operand inside a remote outer", "SELECT count() FROM " + remoteO + " WHERE id IN " + remoteL + viaNetwork, false},
		// ClickHouse reads a local shard's structure without sending its
		// DESC TABLE probe, so against this one server no leg is sent:
		// measured, not required.
		{"describe a remote table", "DESCRIBE TABLE " + remoteO + viaNetwork, false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			legs, err := captureSecondaryQueries(t, seed, peerLegQueryID(shape.name), shape.sql)
			if err != nil {
				if shape.required {
					t.Fatalf("ClickHouse %s refused %q: %v", version, shape.sql, err)
				}
				t.Logf("ClickHouse %s does not execute %q: %v", version, shape.sql, err)
				return
			}
			if len(legs) == 0 {
				if shape.required {
					t.Fatalf("ClickHouse %s sent no secondary query for %q", version, shape.sql)
				}
				t.Logf("ClickHouse %s sent no secondary query for %q", version, shape.sql)
				return
			}
			for _, leg := range legs {
				t.Logf("ClickHouse %s leg: %s", version, leg)
				if name, err := sireserved.PeerReservedNamespaceViolation(leg, sitable.ReservedDatabases(), rewriter.DefaultReservedRowIDColumn); err != nil || name != "" {
					t.Fatalf("leg %q: PeerReservedNamespaceViolation = %q, %v; want no violation", leg, name, err)
				}
				replayErr := internal.Exec(ctx, leg)
				if replayErr != nil && (strings.Contains(replayErr.Error(), "code: 392") || strings.Contains(replayErr.Error(), "storage_integrity:")) {
					t.Fatalf("leg %q refused on a peer-trusted session: %v", leg, replayErr)
				}
				if replayErr != nil {
					// A leg that reads an external table (_data_*) has none
					// outside its own connection, and a nested remote()
					// captured from query_log carries a masked password;
					// ClickHouse, not HouseGate, answers those.
					t.Logf("leg %q replayed with ClickHouse's error: %v", leg, replayErr)
				}
			}
		})
	}
}

// TestPeerReservedGuard_RemoteLeftJoinOfALocalSITableIsRefused pins the one
// known leg that names a reserved database (rewriter-go global.go LIMITATION):
// a JOIN whose left operand is remote() cannot be made GLOBAL, so when
// ClickHouse pushes the JOIN into the leg the peer receives the origin's
// hg_safe read of its local SI table. It never worked — the peer read its own
// table of that name, or failed — and the guard now refuses it with 392.
// Whether this ClickHouse pushes the JOIN is measured and logged.
func TestPeerReservedGuard_RemoteLeftJoinOfALocalSITableIsRefused(t *testing.T) {
	seed := seedPeerReserved(t)
	ctx := context.Background()
	const localSI = sitable.SafeDatabase + ".prdb__l"
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + sitable.SafeDatabase,
		"CREATE TABLE IF NOT EXISTS " + localSI + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO " + localSI + " VALUES (1), (2)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(context.Background(), "DROP TABLE IF EXISTS "+localSI) })
	internal := startPeerReservedInternal(t)
	var version string
	if err := seed.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	const want = "storage_integrity: reserved name hg_safe is not addressable on a peer connection; the peer guard refuses any mention, string literals included"
	refused := func(t *testing.T, leg string) {
		t.Helper()
		if name, err := sireserved.PeerReservedNamespaceViolation(leg, sitable.ReservedDatabases(), rewriter.DefaultReservedRowIDColumn); err != nil || name != sitable.SafeDatabase {
			t.Fatalf("leg %q: PeerReservedNamespaceViolation = %q, %v; want hg_safe", leg, name, err)
		}
		err := internal.Exec(ctx, leg)
		if err == nil || !strings.Contains(err.Error(), "code: 392") || !strings.Contains(err.Error(), want) {
			t.Fatalf("leg %q: err = %v, want the 392 peer reserved-name refusal", leg, err)
		}
	}
	// The leg as it reaches the peer when the JOIN is pushed: always refused.
	refused(t, "SELECT count() AS `count()` FROM `"+peerReservedPhys+"`.`prdb.o` AS `__table1` ALL INNER JOIN `hg_safe`.`prdb__l` AS `__table2` ON `__table1`.`id` = `__table2`.`id`")
	// Measured: what this ClickHouse sends for the rewriter's shape.
	legs, err := captureSecondaryQueries(t, seed, peerLegQueryID("remote left join"),
		"SELECT count() FROM "+peerReservedRemote("prdb.o")+" AS a JOIN "+localSI+" AS b ON a.id = b.id SETTINGS prefer_localhost_replica = 0")
	if err != nil {
		t.Fatalf("ClickHouse %s refused the remote-left JOIN: %v", version, err)
	}
	if len(legs) == 0 {
		t.Fatalf("ClickHouse %s sent no secondary query for the remote-left JOIN", version)
	}
	pushed := false
	for _, leg := range legs {
		t.Logf("ClickHouse %s leg: %s", version, leg)
		if name, _ := sireserved.PeerReservedNamespaceViolation(leg, sitable.ReservedDatabases(), rewriter.DefaultReservedRowIDColumn); name == "" {
			continue
		}
		pushed = true
		refused(t, leg)
	}
	t.Logf("ClickHouse %s pushes the remote-left JOIN of a local SI table into the peer leg: %v", version, pushed)
}
