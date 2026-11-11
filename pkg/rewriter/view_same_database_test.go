package rewriter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/ffifetch/ffitest"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// The view same-database tests (housegate#330) drive a real engine and feed
// its AccessedTables to the permission observer, so they pin that every read
// position of a view body, a materialized view's TO target, and both names of
// a RENAME / EXCHANGE reach the observer's same-database rules. The account owns db1 and db2 on this indexer
// and db3 on another, so only the rule can refuse.

const viewRuleAccount = "0xviewowner"

func viewRuleFactory(t *testing.T, o Options) *SentioNetworkFactory {
	t.Helper()
	st := network.NewInMemoryNetworkState()
	st.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "self.internal", ClickhouseProxyPort: 9000}
	st.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "peer.internal", ClickhouseProxyPort: 9001}
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1", IndexerId: 1}
	st.DatabaseInfos["db2"] = network.DatabaseInfo{DatabaseId: "db2", IndexerId: 1}
	st.DatabaseInfos["db3"] = network.DatabaseInfo{DatabaseId: "db3", IndexerId: 2}
	st.DatabasePermissions[viewRuleAccount] = network.DatabasePermissions{
		"db1": registry.DbAuthOwner, "db2": registry.DbAuthOwner, "db3": registry.DbAuthOwner,
	}
	o.PhysicalDatabase = "phys"
	o.Listen = ":9000"
	o.AuthEnabled = true
	f, err := NewSentioNetworkFactory(o, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(%s): %v", o.Engine, err)
	}
	f.SetGetIndexerId(func() uint64 { return 1 })
	return f
}

func requireViewSameDatabaseRule(t *testing.T, f *SentioNetworkFactory) {
	t.Helper()
	ctx := context.Background()
	observer := network.NewPermissionCommitGateObserver(f.registry)
	for _, tc := range []struct {
		sql     string
		foreign string // "" = allowed
	}{
		{"CREATE VIEW db1.v AS SELECT a FROM db1.t", ""},
		{"CREATE VIEW db1.v (`a` UInt64 COMMENT 'a') AS (SELECT a FROM db1.t JOIN db1.u USING (a)) COMMENT 'v'", ""},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.dst AS (SELECT a FROM db1.src) COMMENT 'mv'", ""},
		{"CREATE VIEW db1.v AS SELECT a FROM db2.t", "db2"},
		{"CREATE VIEW db1.v AS SELECT x.a FROM db1.t AS x JOIN db2.t AS y ON x.a = y.a", "db2"},
		{"CREATE VIEW db1.v AS SELECT a FROM db1.t WHERE a IN (SELECT a FROM db2.t)", "db2"},
		{"CREATE VIEW db1.v AS SELECT a, (SELECT max(a) FROM db2.t) AS m FROM db1.t", "db2"},
		{"CREATE VIEW db1.v AS WITH c AS (SELECT a FROM db2.t) SELECT a FROM c", "db2"},
		{"CREATE VIEW db1.v AS SELECT a FROM db1.t UNION ALL SELECT a FROM db2.t", "db2"},
		{"CREATE VIEW db1.v AS SELECT a FROM view(SELECT a FROM db2.t)", "db2"},
		{"CREATE MATERIALIZED VIEW db1.mv TO db1.dst AS SELECT a FROM db2.src", "db2"},
		{"CREATE MATERIALIZED VIEW db1.mv TO db2.dst AS SELECT a FROM db1.src", "db2"},
		{"CREATE MATERIALIZED VIEW db1.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM db2.src", "db2"},
		// db3 is hosted by indexer 2: the body read would become a remote()
		// call carrying a peer-relay JWS, stored in the view's DDL.
		{"CREATE VIEW db1.v AS SELECT a FROM db3.t", "db3"},
		// A view created over its own database must not be moved into
		// another one, where it would keep reading the first.
		{"RENAME TABLE db1.v TO db1.w", ""},
		{"EXCHANGE TABLES db1.v AND db1.w", ""},
		{"RENAME TABLE db1.v TO db2.v", "db2"},
		{"RENAME TABLE db1.a TO db1.b, db2.c TO db1.d", "db2"},
		{"EXCHANGE TABLES db1.v AND db2.w", "db2"},
	} {
		res, err := f.NewRewriter(&fakeSession{}).Rewrite(ctx, tc.sql, viewRuleAccount)
		if err != nil {
			t.Fatalf("%s: rewrite: %v", tc.sql, err)
		}
		if tc.foreign == "db3" && !strings.Contains(res.SQL, "remote(") {
			t.Errorf("%s -> %q; want the body read rewritten into remote(), the credentials the rule keeps out of stored DDL", tc.sql, res.SQL)
		}
		ev := &commitgate.Event{Type: res.StatementType, User: viewRuleAccount, AccessedTables: res.AccessedTables}
		gateErr := observer.BeforeStatement(ctx, ev)
		t.Logf("%s -> accessed %+v, gate %v", tc.sql, res.AccessedTables, gateErr)
		if tc.foreign == "" {
			if gateErr != nil {
				t.Errorf("%s: want allowed, got %v", tc.sql, gateErr)
			}
			continue
		}
		want := "may only reference its own database"
		if res.StatementType == sqlmeta.StatementTypeRenameTable {
			want = "cannot move a table or view to another database"
		}
		if gateErr == nil || !strings.Contains(gateErr.Error(), `"`+tc.foreign+`"`) || !strings.Contains(gateErr.Error(), want) {
			t.Errorf("%s: want the refusal %q naming %q, got %v", tc.sql, want, tc.foreign, gateErr)
		}
	}
}

// TestNativeEngineViewSameDatabaseRule runs the cases on the in-process
// rewriter-go engine. Skips without the FFI library; run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestNativeEngineViewSameDatabaseRule \
//	  --test_env=POLYGLOT_SQL_FFI_PATH=<libpolyglot_sql_ffi path>
func TestNativeEngineViewSameDatabaseRule(t *testing.T) {
	ffitest.LibOrSkip(t)
	f := viewRuleFactory(t, Options{Engine: EngineNative})
	defer f.Close()
	requireViewSameDatabaseRule(t, f)
}

// TestGRPCEngineViewSameDatabaseRule runs the cases on a rewriter-grpc
// service. Skips unless HOUSEGATE_TEST_REWRITER_GRPC_ADDR is set:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestGRPCEngineViewSameDatabaseRule \
//	  --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50551
func TestGRPCEngineViewSameDatabaseRule(t *testing.T) {
	addr := os.Getenv("HOUSEGATE_TEST_REWRITER_GRPC_ADDR")
	if addr == "" {
		t.Skip("HOUSEGATE_TEST_REWRITER_GRPC_ADDR not set; rewriter-grpc service unavailable")
	}
	f := viewRuleFactory(t, Options{Engine: EngineGRPC, ServiceAddr: addr, Timeout: 10 * time.Second})
	defer f.Close()
	requireViewSameDatabaseRule(t, f)
}
