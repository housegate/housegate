package integration

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sitable"
)

// The replica-read tests (spec 2026-10-14 §11, Plan S3-C4 Task 43) run a
// member host: indexer 1, storage integrity on the native engine, whose table
// state serves rrdb.a and rrdb.b from member copies in hg_replica. rrdb is
// owned by indexer 2, whose advertised HouseGate address refuses connections,
// so without serving a hello on rrdb fails and an ordinary table of rrdb is
// unreachable.
const (
	replicaReadsPhys  = "phys_rr"
	replicaReadsDB    = "rrdb"
	replicaReadsOwner = uint64(2)
	replicaReadsCopyA = "rrdb__a__i1"
	replicaReadsCopyB = "rrdb__b__i1"
)

// replicaReadsServed is the table state of a member host serving rrdb.
func replicaReadsServed() []sitable.Table {
	return []sitable.Table{
		{ID: replicaReadsDB + ".a", Status: sitable.Replica, CopyTable: replicaReadsCopyA, ReplicaOf: replicaReadsOwner},
		{ID: replicaReadsDB + ".b", Status: sitable.Replica, CopyTable: replicaReadsCopyB, ReplicaOf: replicaReadsOwner},
	}
}

// seedReplicaReads creates the two copies in hg_replica, as a member installs
// them (with the reserved row-id column), and a decoy physical table
// phys_rr.`rrdb.o` that a local read of rrdb's ordinary table o would find.
func seedReplicaReads(t *testing.T) clickhouse.Conn {
	t.Helper()
	ctx := context.Background()
	seed := openConnNoDB(t, chEnv.Addr)
	for _, q := range []string{
		"DROP DATABASE IF EXISTS " + replicaReadsPhys,
		"CREATE DATABASE " + replicaReadsPhys,
		"CREATE DATABASE IF NOT EXISTS " + sitable.ReplicaDatabase,
		"DROP TABLE IF EXISTS " + sitable.ReplicaDatabase + "." + replicaReadsCopyA,
		"DROP TABLE IF EXISTS " + sitable.ReplicaDatabase + "." + replicaReadsCopyB,
		"CREATE TABLE " + sitable.ReplicaDatabase + "." + replicaReadsCopyA + " (_hg_row_id FixedString(32), id UInt64, v String) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE " + sitable.ReplicaDatabase + "." + replicaReadsCopyB + " (_hg_row_id FixedString(32), id UInt64, w UInt32) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO " + sitable.ReplicaDatabase + "." + replicaReadsCopyA + " VALUES (repeat('a', 32), 1, 'a1'), (repeat('b', 32), 2, 'a2'), (repeat('c', 32), 3, 'a3')",
		"INSERT INTO " + sitable.ReplicaDatabase + "." + replicaReadsCopyB + " VALUES (repeat('d', 32), 1, 10), (repeat('e', 32), 3, 30)",
		"CREATE TABLE " + replicaReadsPhys + ".`rrdb.o` (id UInt64) ENGINE = MergeTree ORDER BY id",
		"INSERT INTO " + replicaReadsPhys + ".`rrdb.o` VALUES (1), (2), (3), (4), (5)",
	} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_ = seed.Exec(context.Background(), "DROP DATABASE IF EXISTS "+sitable.ReplicaDatabase)
		_ = seed.Exec(context.Background(), "DROP DATABASE IF EXISTS "+replicaReadsPhys)
	})
	return seed
}

// downAddress returns a loopback address nothing listens on.
func downAddress(t *testing.T) (string, uint16) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	_ = ln.Close()
	return "127.0.0.1", port
}

// startReplicaReadsHost starts the member host (indexer 1) with state as its
// table state, the signed lane on (as on every SI host, so the rewriter
// passes writes to sitablestate) and the promotion port wired, and indexer 2
// advertised at an address that refuses connections. extra options are applied
// after these.
func startReplicaReadsHost(t *testing.T, state sitable.TableState, extra ...testenv.ProxyOption) *testenv.TestProxy {
	t.Helper()
	lib := requireNativeLib(t)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ownerHost, ownerPort := downAddress(t)
	opts := []testenv.ProxyOption{
		testenv.WithIndexerID(1),
		testenv.WithLogicalDatabaseAt(chEnv.Database, 1),
		testenv.WithLogicalDatabaseAt(replicaReadsDB, replicaReadsOwner),
		testenv.WithStorageIntegrityReadState(&siReadStateStub{parts: map[string][]string{}}),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = replicaReadsPhys
			cfg.Rewriter.FailOpenOnUnavailable = false
			cfg.StorageIntegrity.Enabled = &enabled
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = "itest-replica-reads"
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
		}),
		func(_ *config.Config, opts *housegate.Options) {
			ns := opts.NetworkState.(*network.InMemoryNetworkState)
			ns.IndexerInfos[replicaReadsOwner] = network.IndexerInfo{IndexerId: replicaReadsOwner, IndexerUrl: ownerHost, ClickhouseProxyPort: ownerPort}
			opts.StorageIntegrityTableState = state
			opts.StorageIntegrityAdmissionConsumer = &capturingConsumer{}
		},
	}
	return testenv.StartServerProxy(t, chEnv.Addr, append(opts, extra...)...)
}

func queryUint64(t *testing.T, conn clickhouse.Conn, sql string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// TestReplicaReads_JoinOfTwoReplicaServedTablesReadsTheCopies: a client of the
// quarantined owner's database, connected to the member host by hello or by
// USE, reads a JOIN of two served tables from the copies, unqualified and
// qualified, in safe mode. The copies are reachable only through their logical
// names, and an unsafe_latest read is refused with the safe-mode text.
func TestReplicaReads_JoinOfTwoReplicaServedTablesReadsTheCopies(t *testing.T) {
	seedReplicaReads(t)
	host := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...))
	ctx := context.Background()

	byHello := openConnDB(t, host.Addr, replicaReadsDB)
	if got := queryUint64(t, byHello, "SELECT count() FROM a JOIN b ON a.id = b.id"); got != 2 {
		t.Fatalf("JOIN count = %d, want 2", got)
	}
	if got := queryUint64(t, byHello, "SELECT sum(id) FROM rrdb.a"); got != 6 {
		t.Fatalf("sum(id) of rrdb.a = %d, want 6", got)
	}
	rows, err := byHello.Query(ctx, "SELECT a.v, b.w FROM a JOIN b ON a.id = b.id ORDER BY a.id")
	if err != nil {
		t.Fatalf("JOIN rows: %v", err)
	}
	var got []string
	for rows.Next() {
		var v string
		var w uint32
		if err := rows.Scan(&v, &w); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s:%d", v, w))
	}
	rows.Close()
	if strings.Join(got, ",") != "a1:10,a3:30" {
		t.Fatalf("JOIN rows = %v, want a1 with 10 and a3 with 30", got)
	}

	byUse := openConnDB(t, host.Addr, chEnv.Database)
	if err := byUse.Exec(ctx, "USE "+replicaReadsDB); err != nil {
		t.Fatalf("USE rrdb: %v", err)
	}
	if got := queryUint64(t, byUse, "SELECT count() FROM a JOIN b ON a.id = b.id"); got != 2 {
		t.Fatalf("JOIN count after USE = %d, want 2", got)
	}

	// tablerefguard runs before forward and rewrite on this ordinary session.
	if err := byHello.Exec(ctx, "SELECT count() FROM "+sitable.ReplicaDatabase+"."+replicaReadsCopyA); err == nil || !strings.Contains(err.Error(), "reserved database hg_replica is not addressable") {
		t.Fatalf("a direct read of the copy = %v, want the table-reference guard's reserved-name refusal", err)
	}
	unsafe := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"SQL_x_read_mode": clickhouse.CustomSetting{Value: "unsafe_latest"}}))
	var n uint64
	err = byHello.QueryRow(unsafe, "SELECT count() FROM a").Scan(&n)
	const safeMode = "storage_integrity read mode unsafe_latest is not available for rrdb.a: it is served read-only from a copy of indexer 2's published data; read it with SQL_x_read_mode='safe'"
	if err == nil || !strings.Contains(err.Error(), safeMode) {
		t.Fatalf("unsafe_latest read = %d, %v; want the refusal %q", n, err, safeMode)
	}
}

// replicaReadsWrites is every write and DDL form the replica-read tests send
// to a served table.
var replicaReadsWrites = []string{
	"INSERT INTO a (id, v) VALUES (9, 'x')",
	"INSERT INTO rrdb.a (id, v) VALUES (9, 'x')",
	"INSERT INTO a (id, v) SELECT id, toString(w) FROM b",
	"ALTER TABLE a DELETE WHERE 1",
	"ALTER TABLE a ADD COLUMN z UInt8",
	"DELETE FROM a WHERE 1",
	"TRUNCATE TABLE a",
	"OPTIMIZE TABLE a FINAL",
	"RENAME TABLE a TO c",
	"DROP TABLE a",
	"CREATE TABLE a (id UInt64) ENGINE = MergeTree ORDER BY id",
	"CREATE TABLE a ENGINE = MergeTree ORDER BY id AS SELECT 1 AS id",
	"CREATE MATERIALIZED VIEW rrdb.mv TO a AS SELECT id, toString(w) AS v FROM b",
}

// TestReplicaReads_WritesAndDDLAreRefusedNamingTheOwner: every write and DDL
// that targets a served table is refused before it reaches ClickHouse, and
// the copy and the catalog are unchanged.
//
//   - On a session kept local for rrdb, forward.Plugin sends each statement to
//     the owner, which is down here, so each is refused with the one 392
//     naming the owner and saying it cannot be reached; the session keeps
//     reading the copy.
//   - On a session of the host's own database, a qualified statement is not
//     routed by the session: the pinned engines refuse each themselves (403:
//     a write target that maps to a remote upstream, or a storage-integrity
//     table written outside the signed lane), and sitablestate's Replica row,
//     the 392 naming the owner (Task 42 TestDecide_ReplicaRows), is the second
//     layer for a statement an engine passes.
//
// Each refusal is logged for the PR record.
func TestReplicaReads_WritesAndDDLAreRefusedNamingTheOwner(t *testing.T) {
	seed := seedReplicaReads(t)
	host := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...))
	served := openConnDB(t, host.Addr, replicaReadsDB)
	const unreachable = "storage_integrity: database rrdb is served read-only from a copy on this host and its owner, indexer 2, cannot be reached; write through its owner"
	for _, sql := range replicaReadsWrites {
		t.Run("served session/"+sql, func(t *testing.T) {
			err := served.Exec(context.Background(), sql)
			if err == nil || !strings.Contains(err.Error(), "code: 392") || !strings.Contains(err.Error(), unreachable) {
				t.Fatalf("%s: err = %v, want the 392 %q", sql, err, unreachable)
			}
			t.Logf("%s refused: %v", sql, err)
		})
	}
	if got := queryUint64(t, served, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("served read after the refused writes = %d, want 3 from the copy", got)
	}

	own := openConnDB(t, host.Addr, chEnv.Database)
	const gateRefusal = "storage_integrity: table rrdb.a is served read-only from a copy of indexer 2's published data; write through its owner"
	for _, sql := range []string{
		"INSERT INTO rrdb.a (id, v) VALUES (9, 'x')",
		"INSERT INTO rrdb.a (id, v) SELECT id, toString(w) FROM rrdb.b",
		"ALTER TABLE rrdb.a DELETE WHERE 1",
		"TRUNCATE TABLE rrdb.a",
		"DROP TABLE rrdb.a",
	} {
		t.Run("own session/"+sql, func(t *testing.T) {
			err := own.Exec(context.Background(), sql)
			engine := err != nil && strings.Contains(err.Error(), "code: 403") && strings.Contains(err.Error(), "rewriter rejected SQL")
			gate := err != nil && strings.Contains(err.Error(), "code: 392") && strings.Contains(err.Error(), gateRefusal)
			if !engine && !gate {
				t.Fatalf("%s: err = %v, want the engine's 403 refusal or the 392 %q", sql, err, gateRefusal)
			}
			t.Logf("%s refused: %v", sql, err)
		})
	}
	if got := queryUint64(t, seed, "SELECT count() FROM "+sitable.ReplicaDatabase+"."+replicaReadsCopyA); got != 3 {
		t.Fatalf("copy rows = %d, want the 3 installed rows", got)
	}
	if got := queryUint64(t, seed, "SELECT count() FROM system.tables WHERE database IN ('"+sitable.ReplicaDatabase+"', '"+replicaReadsPhys+"')"); got != 3 {
		t.Fatalf("tables in hg_replica and phys_rr = %d, want the two copies and the decoy only", got)
	}
}

// startQuarantinedOwnerHost starts rrdb's owner (indexer 2) as an SI host
// whose SI indexer is quarantined: its table state reports rrdb.a Refused
// with the host's code indexer_quarantined, as the devnet2 P3 drill's
// indexer-c did. Auth admits the member host's forwarded __peer__ hello and
// the client's queries, both signed with authTestKey1, which may write rrdb.
func startQuarantinedOwnerHost(t *testing.T) *testenv.TestProxy {
	t.Helper()
	lib := requireNativeLib(t)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{
		ID: replicaReadsDB + ".a", Status: sitable.Refused,
		RefusedCode: "indexer_quarantined", RefusedReason: "storage-integrity indexer 2 is quarantined",
	})
	return testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), replicaReadsDB, registry.DbAuthWrite),
		testenv.WithCredentialReplace(),
		testenv.WithIndexerID(replicaReadsOwner),
		testenv.WithLogicalDatabaseAt(replicaReadsDB, replicaReadsOwner),
		testenv.WithStorageIntegrityReadState(&siReadStateStub{parts: map[string][]string{}}),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = replicaReadsPhys
			cfg.Rewriter.FailOpenOnUnavailable = false
			cfg.StorageIntegrity.Enabled = &enabled
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.NetworkID = "itest-replica-reads-owner"
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityTableState = state
			opts.StorageIntegrityAdmissionConsumer = &capturingConsumer{}
		},
	)
}

// TestReplicaReads_AWriteReachesTheQuarantinedOwner reproduces the devnet2 P3
// drill (Plan S3-C4 Task 48): an INSERT into a quarantined owner's table got
// the replica host's engine 403 when the client's session landed on a member
// host, and the owner's 392 when it landed on the owner. A session kept local
// for rrdb still reads the copy, and its INSERT is sent to the owner, so the
// client gets exactly the owner's quarantine refusal on either path, and
// nothing is written.
func TestReplicaReads_AWriteReachesTheQuarantinedOwner(t *testing.T) {
	seed := seedReplicaReads(t)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	owner := startQuarantinedOwnerHost(t)
	member := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...),
		testenv.WithRelayKey(authTestKey1),
		testenv.WithPeerAt(replicaReadsOwner, owner),
	)
	const insert = "INSERT INTO a (id, v) VALUES (9, 'x')"
	const quarantined = "storage_integrity: table rrdb.a was refused: indexer_quarantined: storage-integrity indexer 2 is quarantined"

	direct := openSignedConnPinnedDB(t, owner.Addr, signer, replicaReadsDB)
	directErr := direct.Exec(context.Background(), insert)
	t.Logf("INSERT on the owner: %v", directErr)
	if directErr == nil || !strings.Contains(directErr.Error(), "code: 392") || !strings.Contains(directErr.Error(), quarantined) {
		t.Fatalf("INSERT on the owner = %v, want the 392 %q", directErr, quarantined)
	}

	viaMember := openSignedConnPinnedDB(t, member.Addr, signer, replicaReadsDB)
	if got := queryUint64(t, viaMember, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("read on the member host = %d, want 3 from the copy", got)
	}
	memberErr := viaMember.Exec(context.Background(), insert)
	t.Logf("INSERT on the member host: %v", memberErr)
	if memberErr == nil || memberErr.Error() != directErr.Error() {
		t.Fatalf("INSERT on the member host = %v, want the owner's refusal %v", memberErr, directErr)
	}

	if got := queryUint64(t, seed, "SELECT count() FROM "+sitable.ReplicaDatabase+"."+replicaReadsCopyA); got != 3 {
		t.Fatalf("copy rows = %d, want the 3 installed rows", got)
	}
}

// TestReplicaReads_OrdinaryTablesOfTheDatabaseStillGoToTheOwner: rrdb keeps
// its remote mapping, so its ordinary table o is read through remote() from
// the owner, which is down: the read fails rather than answering the local
// decoy phys_rr.`rrdb.o`, while the served tables still read from the copies.
func TestReplicaReads_OrdinaryTablesOfTheDatabaseStillGoToTheOwner(t *testing.T) {
	seedReplicaReads(t)
	host := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...))
	conn := openConnDB(t, host.Addr, replicaReadsDB)
	var n uint64
	err := conn.QueryRow(context.Background(), "SELECT count() FROM o").Scan(&n)
	if err == nil {
		t.Fatalf("ordinary rrdb.o answered %d from this host, want the owner's remote() read to fail", n)
	}
	t.Logf("ordinary rrdb.o while the owner is down: %v", err)
	if got := queryUint64(t, conn, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("served rrdb.a = %d after the ordinary read, want 3", got)
	}
}

// TestReplicaReads_ServingEndedMidSessionAsksToReconnect: when the member
// stops serving rrdb, the next statement on a session kept local for it is
// refused with the retryable 733, a new hello on rrdb pivots to the owner
// (down here, so it fails), and the old session reads again once serving
// resumes.
func TestReplicaReads_ServingEndedMidSessionAsksToReconnect(t *testing.T) {
	seedReplicaReads(t)
	state := sitable.NewFake(sitable.Ordinary, replicaReadsServed()...)
	host := startReplicaReadsHost(t, state)
	conn := openConnDB(t, host.Addr, replicaReadsDB)
	if got := queryUint64(t, conn, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("served read = %d, want 3", got)
	}

	state.Set()
	var n uint64
	err := conn.QueryRow(context.Background(), "SELECT count() FROM a").Scan(&n)
	const want = "storage_integrity: database rrdb is no longer served from a copy on this host; reconnect to reach its owner (retryable)"
	if err == nil || !strings.Contains(err.Error(), "code: 733") || !strings.Contains(err.Error(), want) {
		t.Fatalf("read after serving ended = %d, %v; want 733 %q", n, err, want)
	}
	fresh := openConnDB(t, host.Addr, replicaReadsDB)
	if err := fresh.Ping(context.Background()); err == nil {
		t.Fatal("a new hello on rrdb must pivot to its owner, which is down")
	}

	state.Set(replicaReadsServed()...)
	if got := queryUint64(t, conn, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("read once serving resumed = %d, want 3", got)
	}
}

// replicaReadsStrangerKey signs for an account that holds no permission on
// rrdb; authTestKey1 is the SI ingress signer and authTestKey2 the permitted
// reader.
const replicaReadsStrangerKey = "3333333333333333333333333333333333333333333333333333333333333333"

// TestReplicaReads_AnAccountWithoutTheDatabasePermissionIsRefused: with auth
// on, a served copy stays readable only by rrdb's permitted accounts (K48).
// The table state lists the copy for every session, so the engine's
// database_map check is what refuses another tenant on the member host,
// qualified or after a hello on rrdb, before ClickHouse is contacted.
func TestReplicaReads_AnAccountWithoutTheDatabasePermissionIsRefused(t *testing.T) {
	seedReplicaReads(t)
	permitted, err := auth.NewRelaySigner(authTestKey2)
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := auth.NewRelaySigner(replicaReadsStrangerKey)
	if err != nil {
		t.Fatal(err)
	}
	host := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...),
		authProxyConfig([]string{permitted.Address(), stranger.Address()}, false),
		testenv.WithDatabasePermission(permitted.Address(), replicaReadsDB, registry.DbAuthRead),
		testenv.WithDatabasePermission(permitted.Address(), chEnv.Database, registry.DbAuthRead),
		testenv.WithDatabasePermission(stranger.Address(), chEnv.Database, registry.DbAuthRead),
	)
	ctx := context.Background()

	if got := queryUint64(t, openSignedConn(t, host.Addr, permitted), "SELECT sum(id) FROM rrdb.a"); got != 6 {
		t.Fatalf("permitted sum(id) of rrdb.a = %d, want 6 from the copy", got)
	}

	const refusal = "storage-integrity logical database rrdb is not authorized by database_map"
	qualified := openSignedConn(t, host.Addr, stranger)
	for _, sql := range []string{
		"SELECT sum(id) FROM rrdb.a",
		"SELECT count() FROM rrdb.a AS x JOIN rrdb.b AS y ON x.id = y.id",
		"WITH c AS (SELECT id FROM rrdb.a) SELECT count() FROM c WHERE id IN (SELECT id FROM rrdb.b)",
	} {
		var n uint64
		err := qualified.QueryRow(ctx, sql).Scan(&n)
		t.Logf("stranger: %s -> %d, %v", sql, n, err)
		if err == nil || !strings.Contains(err.Error(), refusal) {
			t.Fatalf("stranger: %s = %d, %v; want the refusal %q", sql, n, err, refusal)
		}
	}

	byHello := openSignedConnPinnedDB(t, host.Addr, stranger, replicaReadsDB)
	var n uint64
	err = byHello.QueryRow(ctx, "SELECT sum(id) FROM a").Scan(&n)
	t.Logf("stranger after a hello on rrdb: SELECT sum(id) FROM a -> %d, %v", n, err)
	if err == nil || !strings.Contains(err.Error(), refusal) {
		t.Fatalf("stranger after a hello on rrdb = %d, %v; want the refusal %q", n, err, refusal)
	}
}
