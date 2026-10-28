package rewriter

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sitable"
)

// The replica parity tests pin spec 2026-10-14 §11's routing claim for both
// rewrite engines: a table the host serves from its member's copy resolves to
// hg_replica.<copy> before the remote mapping of its database, which keeps
// sending the database's ordinary tables to the owner. The host is indexer 1;
// rrdb is owned by indexer 2 and its tables a and b are Replica.

const (
	replicaParityCopyA = "rrdb__a__i3"
	replicaParityCopyB = "rrdb__b__i5"
)

// replicaParityState serves rrdb.a and rrdb.b from the member's copies.
func replicaParityState() *sitable.Fake {
	return sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "rrdb.a", Status: sitable.Replica, CopyTable: replicaParityCopyA, ReplicaOf: 2},
		sitable.Table{ID: "rrdb.b", Status: sitable.Replica, CopyTable: replicaParityCopyB, ReplicaOf: 2},
	)
}

// replicaParityNetwork registers rrdb on indexer 2 and both indexers' proxies,
// so buildRemoteUpstreams maps rrdb to the owner.
func replicaParityNetwork() *network.InMemoryNetworkState {
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["rrdb"] = network.DatabaseInfo{DatabaseId: "rrdb", IndexerId: 2}
	st.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "self.internal", ClickhouseProxyPort: 9000}
	st.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "owner.internal", ClickhouseProxyPort: 9001}
	return st
}

// replicaParityOptions is the SI read surface of a member host: safe reads by
// default, the promotion port wired (as on an SI host with its SNode), the
// signed lane on.
func replicaParityOptions() StorageIntegrityOptions {
	return StorageIntegrityOptions{
		Enabled:           true,
		TableState:        replicaParityState(),
		DefaultReadMode:   ReadModeSafe,
		ReadState:         &fakeReadState{},
		InsertLaneEnabled: true,
	}
}

// TestReplicaTableResolvesBeforeRemoteMapping: the request names the copy for
// the Replica tables while rrdb keeps its remote upstream, so only the
// engine's lookup order decides; the engine-gated tests below pin that order.
func TestReplicaTableResolvesBeforeRemoteMapping(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT})}
	f := &SentioNetworkFactory{
		options:  Options{PhysicalDatabase: "phys", StorageIntegrity: replicaParityOptions()},
		registry: replicaParityNetwork(),
		backend:  be,
	}
	f.SetGetIndexerId(func() uint64 { return 1 })
	if _, err := f.NewRewriter(&fakeSession{logical: "rrdb", physical: "phys"}).Rewrite(context.Background(), "SELECT count() FROM a", ""); err != nil {
		t.Fatal(err)
	}
	dyn := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs()
	for id, copyTable := range map[string]string{"rrdb.a": sitable.ReplicaDatabase + "." + replicaParityCopyA, "rrdb.b": sitable.ReplicaDatabase + "." + replicaParityCopyB} {
		tbl := dyn.GetStorageIntegrity().GetTables()[id]
		if tbl.GetSafeTable() != copyTable || tbl.GetUnsafeTable() != copyTable {
			t.Fatalf("%s = %v, want %s on both surfaces", id, tbl, copyTable)
		}
	}
	key := dyn.GetLogicalDatabaseToRemoteUpstreamIndex()["rrdb"]
	up := dyn.GetRemoteUpstreams()[key]
	if key != "2" || up == nil || !strings.Contains(up.GetUser(), "owner.internal:9001") {
		t.Fatalf("rrdb remote mapping = %q -> %v, want the owner's upstream kept", key, up)
	}
	if dyn.GetUpstreamLogicalDatabaseInContext() != "rrdb" {
		t.Fatalf("logical context = %q, want rrdb", dyn.GetUpstreamLogicalDatabaseInContext())
	}
}

// TestUseOfAReplicaServedDatabaseIsRewrittenLocally: forward.Plugin keeps a
// USE of a served database on this host, so the rewrite of that USE drops the
// database's remote mapping (both engines refuse a USE of a remote-mapped
// database); every other statement, and a USE of a database that is not
// served, keeps the mapping.
func TestUseOfAReplicaServedDatabaseIsRewrittenLocally(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT})}
	reg := replicaParityNetwork()
	reg.DatabaseInfos["other"] = network.DatabaseInfo{DatabaseId: "other", IndexerId: 2}
	f := &SentioNetworkFactory{
		options:  Options{PhysicalDatabase: "phys", StorageIntegrity: replicaParityOptions()},
		registry: reg,
		backend:  be,
	}
	f.SetGetIndexerId(func() uint64 { return 1 })
	for _, tc := range []struct {
		sql         string
		rrdb, other bool
	}{
		{sql: "USE rrdb", rrdb: false, other: true},
		{sql: "use `rrdb`;", rrdb: false, other: true},
		{sql: "USE other", rrdb: true, other: true},
		{sql: "SELECT count() FROM rrdb.o", rrdb: true, other: true},
		{sql: "USE rrdb SETTINGS max_threads = 1", rrdb: true, other: true},
	} {
		if _, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), tc.sql, ""); err != nil {
			t.Fatalf("Rewrite(%q): %v", tc.sql, err)
		}
		remote := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetLogicalDatabaseToRemoteUpstreamIndex()
		_, rrdb := remote["rrdb"]
		_, other := remote["other"]
		if rrdb != tc.rrdb || other != tc.other {
			t.Fatalf("%q: remote mapping = %v, want rrdb mapped %v and other mapped %v", tc.sql, remote, tc.rrdb, tc.other)
		}
	}
}

// requireReplicaParity drives one real engine through the member-host cases.
func requireReplicaParity(t *testing.T, f *SentioNetworkFactory) {
	t.Helper()
	f.SetGetIndexerId(func() uint64 { return 1 })
	ctx := context.Background()
	rewrite := func(t *testing.T, sess *fakeSession, ctx context.Context, sql string) RewriteResult {
		t.Helper()
		res, err := f.NewRewriter(sess).Rewrite(ctx, sql, "")
		if err != nil {
			t.Fatalf("Rewrite(%q): %v", sql, err)
		}
		t.Logf("%s -> %s", sql, res.SQL)
		return res
	}
	copyA := sitable.ReplicaDatabase + "." + replicaParityCopyA
	copyB := sitable.ReplicaDatabase + "." + replicaParityCopyB
	reads := func(sql string, copies ...string) {
		t.Helper()
		for _, sess := range []*fakeSession{{}, {logical: "rrdb", physical: "phys"}} {
			stmt := sql
			if sess.logical != "" {
				stmt = strings.ReplaceAll(sql, "rrdb.", "")
			}
			res := rewrite(t, sess, ctx, stmt)
			for _, c := range copies {
				if !strings.Contains(res.SQL, c) {
					t.Fatalf("%q (logical %q) -> %q, want it to read %s", stmt, sess.logical, res.SQL, c)
				}
			}
			if strings.Contains(strings.ToLower(res.SQL), "remote(") {
				t.Fatalf("%q (logical %q) -> %q, want no remote() leg for a Replica table", stmt, sess.logical, res.SQL)
			}
		}
	}
	reads("SELECT count() FROM rrdb.a", copyA)
	reads("SELECT count() FROM rrdb.a AS x JOIN rrdb.b AS y ON x.id = y.id", copyA, copyB)

	// A USE of the served database, kept local by forward.Plugin, is a local
	// USE: it moves the session to the physical database and records rrdb.
	useSess := &fakeSession{}
	if res := rewrite(t, useSess, ctx, "USE rrdb"); !strings.Contains(res.SQL, "phys") || strings.Join(useSess.setLogical, ",") != "rrdb" {
		t.Fatalf("USE rrdb -> %q, logical %v; want a local USE of the physical database recording rrdb", res.SQL, useSess.setLogical)
	}

	// An ordinary table of the same database still goes to its owner.
	if res := rewrite(t, &fakeSession{}, ctx, "SELECT count() FROM rrdb.o"); !strings.Contains(strings.ToLower(res.SQL), "remote(") || strings.Contains(res.SQL, sitable.ReplicaDatabase) {
		t.Fatalf("ordinary rrdb.o -> %q, want a remote() leg to the owner", res.SQL)
	}

	// A computed reserved name on an ordinary session is refused (K48).
	if _, err := f.NewRewriter(&fakeSession{}).Rewrite(ctx, "SELECT * FROM merge(concat('hg_', 'replica'), '.*')", ""); !errors.As(err, new(*RejectedError)) {
		t.Fatalf("merge(concat('hg_', 'replica'), '.*') = %v, want a RejectedError", err)
	}

	// unsafe_latest of a Replica table is refused with the safe-mode text.
	const want = "storage_integrity read mode unsafe_latest is not available for rrdb.a: it is served read-only from a copy of indexer 2's published data; read it with SQL_x_read_mode='safe'"
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(WithReadMode(ctx, ReadModeUnsafeLatest), "SELECT count() FROM rrdb.a", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Message != want {
		t.Fatalf("unsafe_latest read of rrdb.a = %v, want %q", err, want)
	}
}

// TestNativeEngineReplicaTableResolvesBeforeRemoteMapping runs the parity
// cases on the in-process rewriter-go engine. Skips without the FFI library;
// run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestNativeEngineReplicaTableResolvesBeforeRemoteMapping \
//	  --test_env=POLYGLOT_SQL_FFI_PATH=<libpolyglot_sql_ffi path>
func TestNativeEngineReplicaTableResolvesBeforeRemoteMapping(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	f, err := NewSentioNetworkFactory(Options{
		Engine:           EngineNative,
		PhysicalDatabase: "phys",
		Listen:           ":9000",
		StorageIntegrity: replicaParityOptions(),
	}, replicaParityNetwork())
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(native): %v", err)
	}
	defer f.Close()
	requireReplicaParity(t, f)
}

// TestGRPCEngineReplicaTableResolvesBeforeRemoteMapping runs the parity cases
// on a rewriter-grpc service, the devnet2 housegate-rewriter image. Skips
// without its address; run locally against a container of the pinned image
// with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestGRPCEngineReplicaTableResolvesBeforeRemoteMapping \
//	  --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50551
func TestGRPCEngineReplicaTableResolvesBeforeRemoteMapping(t *testing.T) {
	addr := os.Getenv("HOUSEGATE_TEST_REWRITER_GRPC_ADDR")
	if addr == "" {
		t.Skip("HOUSEGATE_TEST_REWRITER_GRPC_ADDR not set; rewriter-grpc service unavailable")
	}
	f, err := NewSentioNetworkFactory(Options{
		Engine:           EngineGRPC,
		ServiceAddr:      addr,
		Timeout:          10 * time.Second,
		PhysicalDatabase: "phys",
		Listen:           ":9000",
		StorageIntegrity: replicaParityOptions(),
	}, replicaParityNetwork())
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(grpc): %v", err)
	}
	defer f.Close()
	requireReplicaParity(t, f)
}

// The permission tests pin K48's "each database stays readable only by its
// permitted accounts" for a served copy. buildStorageIntegrityArgs lists every
// served table for every session, whatever the account, while
// buildDatabaseMap keeps only the account's permitted databases, so the
// engine is what keeps a copy from another tenant: it must check the
// database_map before it resolves an SI table to its physical surface. The
// host's own Active table owndb.t pins the same check for a table that
// predates replica serving.

const (
	replicaPermitted = "0xpermitted" // Read on rrdb and owndb
	replicaStranger  = "0xstranger"  // Read on otherdb only
)

// replicaPermissionFactory is the member host of the parity tests with auth
// on, an own Active table owndb.t, an ordinary local database otherdb and
// the two accounts above.
func replicaPermissionFactory(t *testing.T, o Options) *SentioNetworkFactory {
	t.Helper()
	st := replicaParityNetwork()
	st.DatabaseInfos["owndb"] = network.DatabaseInfo{DatabaseId: "owndb", IndexerId: 1}
	st.DatabaseInfos["otherdb"] = network.DatabaseInfo{DatabaseId: "otherdb", IndexerId: 1}
	st.DatabasePermissions[replicaPermitted] = network.DatabasePermissions{"rrdb": registry.DbAuthRead, "owndb": registry.DbAuthRead}
	st.DatabasePermissions[replicaStranger] = network.DatabasePermissions{"otherdb": registry.DbAuthRead}
	si := replicaParityOptions()
	si.TableState = sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "rrdb.a", Status: sitable.Replica, CopyTable: replicaParityCopyA, ReplicaOf: 2},
		sitable.Table{ID: "rrdb.b", Status: sitable.Replica, CopyTable: replicaParityCopyB, ReplicaOf: 2},
		sitable.Table{ID: "owndb.t", Status: sitable.Active},
	)
	o.PhysicalDatabase = "phys"
	o.Listen = ":9000"
	o.AuthEnabled = true
	o.StorageIntegrity = si
	f, err := NewSentioNetworkFactory(o, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(%s): %v", o.Engine, err)
	}
	f.SetGetIndexerId(func() uint64 { return 1 })
	return f
}

// requireReplicaTableNeedsTheDatabasePermission drives one real engine: the
// permitted account reads the copy, and every statement of an account
// without a permission on rrdb (or of no account) that reaches a served
// table is refused by the engine's database_map check, with no SQL.
func requireReplicaTableNeedsTheDatabasePermission(t *testing.T, f *SentioNetworkFactory) {
	t.Helper()
	ctx := context.Background()
	copyA := sitable.ReplicaDatabase + "." + replicaParityCopyA

	res, err := f.NewRewriter(&fakeSession{}).Rewrite(ctx, "SELECT count() FROM rrdb.a", replicaPermitted)
	if err != nil || !strings.Contains(res.SQL, copyA) {
		t.Fatalf("permitted read of rrdb.a -> %q, %v; want it to read %s", res.SQL, err, copyA)
	}

	for _, tc := range []struct {
		account, logical, sql, db string
	}{
		{replicaStranger, "", "SELECT count() FROM rrdb.a", "rrdb"},
		{replicaStranger, "rrdb", "SELECT count() FROM a", "rrdb"},
		{replicaStranger, "", "SELECT * FROM rrdb.a AS x JOIN rrdb.b AS y ON x.id = y.id", "rrdb"},
		{replicaStranger, "", "SELECT * FROM otherdb.x JOIN rrdb.a AS y ON x.id = y.id", "rrdb"},
		{replicaStranger, "", "SELECT * FROM otherdb.x WHERE id IN (SELECT id FROM rrdb.a)", "rrdb"},
		{replicaStranger, "", "WITH c AS (SELECT * FROM rrdb.a) SELECT * FROM c", "rrdb"},
		{replicaStranger, "", "SELECT id FROM otherdb.x UNION ALL SELECT id FROM rrdb.a", "rrdb"},
		{"", "", "SELECT count() FROM rrdb.a", "rrdb"},
		{replicaStranger, "", "SELECT count() FROM owndb.t", "owndb"},
	} {
		sess := &fakeSession{logical: tc.logical}
		if tc.logical != "" {
			sess.physical = "phys"
		}
		res, err := f.NewRewriter(sess).Rewrite(ctx, tc.sql, tc.account)
		t.Logf("account %q, logical %q: %s -> %q, %v", tc.account, tc.logical, tc.sql, res.SQL, err)
		var rej *RejectedError
		want := "storage-integrity logical database " + tc.db + " is not authorized by database_map"
		if !errors.As(err, &rej) || !strings.Contains(rej.Message, want) || res.SQL != "" {
			t.Fatalf("account %q, logical %q: %s -> %q, %v; want the refusal %q and no SQL", tc.account, tc.logical, tc.sql, res.SQL, err, want)
		}
	}

	// The permitted account still reads its own database's copy after the
	// refusals: the engine keeps no per-statement state across accounts.
	if res, err := f.NewRewriter(&fakeSession{}).Rewrite(ctx, "SELECT count() FROM rrdb.a", replicaPermitted); err != nil || !strings.Contains(res.SQL, copyA) {
		t.Fatalf("permitted read of rrdb.a after the refusals -> %q, %v; want it to read %s", res.SQL, err, copyA)
	}
}

// TestNativeEngineReplicaTableNeedsTheDatabasePermission runs the permission
// cases on the in-process rewriter-go engine. Skips without the FFI library;
// run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestNativeEngineReplicaTableNeedsTheDatabasePermission \
//	  --test_env=POLYGLOT_SQL_FFI_PATH=<libpolyglot_sql_ffi path>
func TestNativeEngineReplicaTableNeedsTheDatabasePermission(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	f := replicaPermissionFactory(t, Options{Engine: EngineNative})
	defer f.Close()
	requireReplicaTableNeedsTheDatabasePermission(t, f)
}

// TestGRPCEngineReplicaTableNeedsTheDatabasePermission runs the permission
// cases on a rewriter-grpc service, the devnet2 housegate-rewriter image.
// Skips without its address; run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestGRPCEngineReplicaTableNeedsTheDatabasePermission \
//	  --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50551
func TestGRPCEngineReplicaTableNeedsTheDatabasePermission(t *testing.T) {
	addr := os.Getenv("HOUSEGATE_TEST_REWRITER_GRPC_ADDR")
	if addr == "" {
		t.Skip("HOUSEGATE_TEST_REWRITER_GRPC_ADDR not set; rewriter-grpc service unavailable")
	}
	f := replicaPermissionFactory(t, Options{Engine: EngineGRPC, ServiceAddr: addr, Timeout: 10 * time.Second})
	defer f.Close()
	requireReplicaTableNeedsTheDatabasePermission(t, f)
}
