package rewriter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/network"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

type scriptedProbeBackend struct {
	fakeBackend
	responses map[string]*pb.RewriteSQLResponse
	requests  []*pb.RewriteSQLRequest
	deadlines []bool
}

func (b *scriptedProbeBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.lastReq = req
	b.requests = append(b.requests, req)
	_, hasDeadline := ctx.Deadline()
	b.deadlines = append(b.deadlines, hasDeadline)
	resp, ok := b.responses[probeKey(req)]
	if !ok {
		return nil, fmt.Errorf("unexpected storage-integrity probe SQL %q", req.GetSql())
	}
	return resp, nil
}

func TestProbeStorageIntegrityBuild(t *testing.T) {
	t.Run("correct build passes", func(t *testing.T) {
		be := &scriptedProbeBackend{responses: conformingProbeResponses()}
		f := newSIFactory(be, nil, true)
		if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("probe: %v", err)
		}
		if got := strings.Join(probeSQLs(be.requests), " | "); got != strings.Join([]string{
			storageIntegrityProbeSQL,
			"SYSTEM RELOAD CONFIG",
			"SYSTEM START MERGES hg_unsafe.db1__t",
			"TRUNCATE DATABASE hg_safe",
			storageIntegrityProbeHeredocSQL,
			"DROP TABLE db1.t",
			"SYSTEM RELOAD CONFIG (empty table map)",
			"SELECT * FROM hg_safe.db1__t (empty table map)",
		}, " | ") {
			t.Fatalf("probe SQLs = %s", got)
		}
		si := be.requests[3].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if si.GetContractVersion() != StorageIntegrityContractV2 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
			t.Fatalf("probe request did not carry the fixed SI args: %v", si)
		}
		empty := be.requests[6].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if empty.GetContractVersion() != StorageIntegrityContractV2 || empty.GetTables() == nil || len(empty.GetTables()) != 0 {
			t.Fatalf("empty-map probe args = %v, want V2 with an empty table map", empty)
		}
		for i, req := range be.requests {
			dyn := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs()
			si := dyn.GetStorageIntegrity()
			if strings.Join(si.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
				t.Fatalf("probe request %d reserved databases = %v, want the production list", i, si.GetReservedDatabases())
			}
			// Production requests carry the protected namespace whatever
			// storage_integrity.enabled says; the probe must prove the
			// engine's answers under that same request shape.
			if got := strings.Join(dyn.GetProtectedDatabases(), ","); got != "phys,hg_safe,hg_unsafe,hg_promote" {
				t.Fatalf("probe request %d protected_databases = %s, want the production list", i, got)
			}
		}
		for i, hasDeadline := range be.deadlines {
			if !hasDeadline {
				t.Fatalf("probe request %d had no deadline", i)
			}
		}
	})

	// A pre-Spec-N engine answers every Spec I probe correctly and then forwards
	// the tagged heredoc as Success — which is exactly the shape rewriter-go
	// v0.9.0 has. Without this case the probe would pass a build in which any
	// authenticated user can read hg_safe through merge($tag$hg_safe$tag$, ...).
	t.Run("pre-Spec-N build is refused on the heredoc probe", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses[storageIntegrityProbeHeredocSQL] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
			SqlAfterRewrite: "SELECT * FROM merge('hg_safe', 'db1__t')",
		})
		be := &scriptedProbeBackend{responses: responses}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil {
			t.Fatal("a build that forwards a tagged heredoc into hg_safe must fail the probe")
		}
		if !strings.Contains(err.Error(), "tagged-heredoc-namespace") {
			t.Fatalf("err = %v, want the heredoc probe named", err)
		}
		if len(be.requests) != 5 {
			t.Fatalf("the heredoc probe must run fifth, after all four Spec I probes; got %v", probeSQLs(be.requests))
		}
	})

	// A V1-only build (rewriter-go < v0.13.0, the release that shipped
	// contract V2; the floor is now v0.17.0 through the table-reference
	// probe) acknowledges V1, never V2; and a build that acknowledges V2 but
	// still rejects the SI DROP or activates the catch-all by table count is
	// refused on the matching V2 probe.
	t.Run("V1 acknowledgement is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		for _, resp := range responses {
			resp.StorageIntegrityContractVersion = StorageIntegrityContractV1
		}
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=describe-fingerprint") || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want the first probe refused on its V1 acknowledgement", err)
		}
	})

	t.Run("V1 DROP rejection is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["DROP TABLE db1.t"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "DROP TABLE db1.t",
			Message:         "storage-integrity table db1.t accepts writes only through the signed statement lane",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-si-drop-ordinary-physical") {
			t.Fatalf("err = %v, want the V2 DROP probe named", err)
		}
	})

	// A build that ignores reserved_databases forwards a direct hg_safe read
	// while no table map entry names hg_safe.
	t.Run("ignored reserved databases are refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["SELECT * FROM hg_safe.db1__t (empty table map)"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
			SqlAfterRewrite: "SELECT * FROM hg_safe.db1__t",
			Message:         "success",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-empty-map-reserved-database") {
			t.Fatalf("err = %v, want the reserved-database probe named", err)
		}
	})

	// The DROP fingerprint is exact per engine: the native engine quotes the
	// physical table with double quotes, the gRPC engine with backticks.
	t.Run("DROP fingerprint follows the engine", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["DROP TABLE db1.t"].SqlAfterRewrite = StorageIntegrityProbeDropExpectedSQLNative
		grpc := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true)
		if err := grpc.ProbeStorageIntegrityBuild(context.Background()); err == nil || !strings.Contains(err.Error(), "probe=v2-si-drop-ordinary-physical") {
			t.Fatalf("grpc engine given the native spelling: err = %v, want the DROP probe refused", err)
		}
		native := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true)
		native.options.Engine = EngineNative
		if err := native.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("native engine given the native spelling: %v", err)
		}
	})

	t.Run("table-count catch-all is refused", func(t *testing.T) {
		responses := conformingProbeResponses()
		responses["SYSTEM RELOAD CONFIG (empty table map)"] = acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
		})
		err := newSIFactory(&scriptedProbeBackend{responses: responses}, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=v2-empty-map-catch-all") {
			t.Fatalf("err = %v, want the empty-map catch-all probe named", err)
		}
	})

	t.Run("old build is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:          pb.RewriteCode_Success,
			StatementType: pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: "SELECT name, type, default_type, default_expression, comment, " +
				"codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' " +
				"AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position",
		})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "storage-integrity engine probe") {
			t.Fatalf("err = %v, want a build-probe refusal", err)
		}
	})

	t.Run("missing acknowledgement is refused", func(t *testing.T) {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{
			Code: pb.RewriteCode_Success, SqlAfterRewrite: StorageIntegrityProbeExpectedSQL}}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want an acknowledgement refusal", err)
		}
	})

	t.Run("rejected probe is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code: pb.RewriteCode_UnsupportedStatement, Message: "nope"})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "UnsupportedStatement") {
			t.Fatalf("err = %v, want a rejected-probe refusal", err)
		}
	})
}

func TestProbeStorageIntegrityBuildRefusesIncompleteSpecIBehavior(t *testing.T) {
	for _, tc := range []struct {
		name      string
		probeSQL  string
		probeName string
		mutate    func(*pb.RewriteSQLResponse)
	}{
		{
			name:      "wrong DESCRIBE success message",
			probeSQL:  storageIntegrityProbeSQL,
			probeName: "describe-fingerprint",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Message = ""
			},
		},
		{
			name:      "old catch-all success despite matching DESCRIBE",
			probeSQL:  "SYSTEM RELOAD CONFIG",
			probeName: "unmodelled-catch-all",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Code = pb.RewriteCode_Success
				resp.Message = ""
			},
		},
		{
			name:      "old physical SYSTEM target success",
			probeSQL:  "SYSTEM START MERGES hg_unsafe.db1__t",
			probeName: "protected-physical-system-target",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Code = pb.RewriteCode_Success
				resp.Message = ""
			},
		},
		{
			name:      "stub physical database rejection",
			probeSQL:  "TRUNCATE DATABASE hg_safe",
			probeName: "protected-physical-database",
			mutate: func(resp *pb.RewriteSQLResponse) {
				resp.Message = "unsupported target hg_safe from hg_unsafe"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			responses := conformingProbeResponses()
			tc.mutate(responses[tc.probeSQL])
			be := &scriptedProbeBackend{responses: responses}
			err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
			if err == nil {
				t.Fatal("probe passed an incomplete Spec I backend")
			}
			for _, want := range []string{"storage-integrity engine probe", "engine=grpc", "probe=" + tc.probeName, storageIntegrityProbeRequiredBuild} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %q, want %q", err, want)
				}
			}
			for _, protectedName := range []string{"hg_safe", "hg_unsafe", "db1__t"} {
				if strings.Contains(err.Error(), protectedName) {
					t.Fatalf("err leaked protocol-owned name %q: %v", protectedName, err)
				}
			}
		})
	}
}

// TestReleasedGRPCStorageIntegrityProbeSmoke drives the real gRPC transport
// through the same startup conformance suite. It is opt-in because CI does not
// run a rewriter-grpc service; release validation supplies the address with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestReleasedGRPCStorageIntegrityProbeSmoke \
//	  --test_env=HOUSEGATE_TEST_REWRITER_GRPC_ADDR=127.0.0.1:50051
func TestReleasedGRPCStorageIntegrityProbeSmoke(t *testing.T) {
	addr := os.Getenv("HOUSEGATE_TEST_REWRITER_GRPC_ADDR")
	if addr == "" {
		t.Skip("HOUSEGATE_TEST_REWRITER_GRPC_ADDR not set; released gRPC engine unavailable")
	}
	f, err := NewSentioNetworkFactory(Options{
		Engine:      EngineGRPC,
		ServiceAddr: addr,
		Timeout:     10 * time.Second,
	}, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(grpc): %v", err)
	}
	defer f.Close()
	if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
		t.Fatalf("released gRPC storage-integrity probe: %v", err)
	}
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("released gRPC table-reference probe: %v", err)
	}
}

func conformingProbeResponses() map[string]*pb.RewriteSQLResponse {
	return map[string]*pb.RewriteSQLResponse{
		storageIntegrityProbeSQL: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: StorageIntegrityProbeExpectedSQL,
			Message:         "success",
		}),
		"SYSTEM RELOAD CONFIG": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded",
		}),
		"SYSTEM START MERGES hg_unsafe.db1__t": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM START MERGES hg_unsafe.db1__t",
			Message:         "storage-integrity physical table hg_unsafe.db1__t is not directly addressable",
		}),
		"TRUNCATE DATABASE hg_safe": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "TRUNCATE DATABASE hg_safe",
			Message:         "storage-integrity physical database hg_safe is not directly addressable",
		}),
		storageIntegrityProbeHeredocSQL: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_RewriteError,
			SqlAfterRewrite: storageIntegrityProbeHeredocSQL,
			Message:         storageIntegrityProbeHeredocMessage,
		}),
		"DROP TABLE db1.t": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DROP_TABLE,
			SqlAfterRewrite: StorageIntegrityProbeDropExpectedSQLGRPC,
			Message:         "success",
		}),
		"SYSTEM RELOAD CONFIG (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_UnsupportedStatement,
			SqlAfterRewrite: "SYSTEM RELOAD CONFIG",
			Message:         storageIntegrityProbeEmptyMapMessage,
		}),
		"SELECT * FROM hg_safe.db1__t (empty table map)": acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_RewriteError,
			StatementType:   pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
			SqlAfterRewrite: "SELECT * FROM hg_safe.db1__t",
			Message:         storageIntegrityProbeReservedMessage,
		}),
	}
}

// probeKey distinguishes the two SYSTEM RELOAD CONFIG probes by whether the
// request carried an empty table map.
func probeKey(req *pb.RewriteSQLRequest) string {
	si := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if len(si.GetTables()) == 0 {
		return req.GetSql() + " (empty table map)"
	}
	return req.GetSql()
}

func probeSQLs(reqs []*pb.RewriteSQLRequest) []string {
	sqls := make([]string, 0, len(reqs))
	for _, req := range reqs {
		sqls = append(sqls, probeKey(req))
	}
	return sqls
}

// policyProbeBackend answers every table-reference probe as a conforming
// engine of its kind, except the SQL in override.
type policyProbeBackend struct {
	fakeBackend
	engine    string
	override  map[string]*pb.RewriteSQLResponse
	requests  []*pb.RewriteSQLRequest
	deadlines []bool
}

func (b *policyProbeBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.requests = append(b.requests, req)
	_, hasDeadline := ctx.Deadline()
	b.deadlines = append(b.deadlines, hasDeadline)
	if resp, ok := b.override[req.GetSql()]; ok {
		return resp, nil
	}
	resp, ok := TableReferenceProbeAnswer(req, b.engine)
	if !ok {
		return nil, fmt.Errorf("unexpected table-reference probe request %q", req.GetSql())
	}
	return resp, nil
}

func newPolicyProbeFactory(be *policyProbeBackend) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.Engine = be.engine
	return f
}

func TestProbeTableReferencePolicy(t *testing.T) {
	for _, engine := range []string{EngineGRPC, EngineNative} {
		t.Run("conforming "+engine+" build passes", func(t *testing.T) {
			be := &policyProbeBackend{engine: engine}
			if err := newPolicyProbeFactory(be).ProbeTableReferencePolicy(context.Background()); err != nil {
				t.Fatalf("probe: %v", err)
			}
			if len(be.requests) != len(tableReferenceProbes) {
				t.Fatalf("requests = %d, want %d", len(be.requests), len(tableReferenceProbes))
			}
			for i, req := range be.requests {
				dyn := req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs()
				if got := strings.Join(dyn.GetProtectedDatabases(), ","); got != "phys,hg_safe,hg_unsafe,hg_promote" {
					t.Fatalf("request %d protected_databases = %s", i, got)
				}
				if dyn.GetStorageIntegrity() != nil {
					t.Fatalf("request %d carries StorageIntegrityArgs; the policy probe models storage integrity disabled", i)
				}
				if dyn.GetUpstreamLogicalDatabaseInContext() != "db1" || dyn.UpstreamPhysicalDatabaseInContext != nil {
					t.Fatalf("request %d context = %q / %v, want logical db1 and no physical context", i,
						dyn.GetUpstreamLogicalDatabaseInContext(), dyn.UpstreamPhysicalDatabaseInContext)
				}
				if !be.deadlines[i] {
					t.Fatalf("request %d had no deadline", i)
				}
			}
		})
	}

	refused := func(t *testing.T, be *policyProbeBackend, probeName string, extra ...string) {
		t.Helper()
		err := newPolicyProbeFactory(be).ProbeTableReferencePolicy(context.Background())
		if err == nil {
			t.Fatalf("probe passed, want probe=%s refused", probeName)
		}
		for _, want := range append([]string{"rewriter table-reference probe", "probe=" + probeName, TableReferenceProbeRequiredBuild}, extra...) {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %q, want %q", err, want)
			}
		}
		for _, protectedName := range []string{"hg_promote", "db2.x", "phys."} {
			if strings.Contains(err.Error(), protectedName) {
				t.Fatalf("err leaked probe SQL or engine output %q: %v", protectedName, err)
			}
		}
	}
	success := func(sql string) *pb.RewriteSQLResponse {
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, SqlAfterRewrite: sql, Message: "success"}
	}
	// Measured: rewriter-go v0.16.0 answers exactly this when the request
	// omits protected_databases.
	t.Run("an engine that ignores protected_databases is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM hg_promote.`db2.x`": success(`SELECT * FROM hg_promote."db2.x"`),
		}}, "protected-reserved-database")
	})
	// Measured: rewriter-go v0.15.0 answers both §13 discriminators Success.
	t.Run("rewriter-go v0.15.0 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM db1.o SETTINGS allow_experimental_analyzer = 0": success(`SELECT * FROM phys."db1.o" "db1.o" SETTINGS allow_experimental_analyzer = 0`),
			"CREATE TABLE db1.n (d Date, n UInt8) ENGINE = MergeTree(d, (SELECT max(n) FROM db1.o), 8192)": {Code: pb.RewriteCode_Success,
				StatementType: pb.StatementType_STATEMENT_TYPE_CREATE_TABLE, Message: "success",
				SqlAfterRewrite: `CREATE TABLE phys."db1.n" (d DATE, n UInt8) ENGINE=MergeTree(d, (SELECT max(n) FROM db1.o), 8192)`},
		}}, "analyzer-off-refused")
	})
	t.Run("an engine-argument read is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"CREATE TABLE db1.n (d Date, n UInt8) ENGINE = MergeTree(d, (SELECT max(n) FROM db1.o), 8192)": {Code: pb.RewriteCode_Success,
				StatementType: pb.StatementType_STATEMENT_TYPE_CREATE_TABLE, Message: "success",
				SqlAfterRewrite: `CREATE TABLE phys."db1.n" (d DATE, n UInt8) ENGINE=MergeTree(d, (SELECT max(n) FROM db1.o), 8192)`},
		}}, "engine-argument-read-refused")
	})
	notSupported := func(sql string) *pb.RewriteSQLResponse {
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, SqlAfterRewrite: sql, Message: "statement is not supported"}
	}
	// Measured 2026-10-02 (native, FFI v0.16.0): rewriter-go v0.16.0 refuses
	// both Sentio-driver shapes it cannot regenerate faithfully.
	t.Run("rewriter-go v0.16.0 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"SELECT a FROM db1.o WHERE startsWith(a, 'x')": notSupported("SELECT a FROM db1.o WHERE startsWith(a, 'x')"),
			"CREATE VIEW db1.v (`a` String COMMENT 'c') AS (SELECT a FROM db1.o) COMMENT 'v'": notSupported(
				"CREATE VIEW db1.v (`a` String COMMENT 'c') AS (SELECT a FROM db1.o) COMMENT 'v'"),
		}}, "driver-function-spelling", "code=UnsupportedStatement")
	})
	t.Run("a refused driver view is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineNative, override: map[string]*pb.RewriteSQLResponse{
			"CREATE VIEW db1.v (`a` String COMMENT 'c') AS (SELECT a FROM db1.o) COMMENT 'v'": notSupported(
				"CREATE VIEW db1.v (`a` String COMMENT 'c') AS (SELECT a FROM db1.o) COMMENT 'v'"),
		}}, "driver-view-comment", "code=UnsupportedStatement")
	})
	// rewriter-grpc v0.16.0 (c24ed33) pins this exact SQL and request as
	// UnsupportedStatement in TableRefFidelity.MeaningChangingLiteralFoldsAreRefused.
	t.Run("rewriter-grpc v0.16.0 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT a FROM db1.o WHERE a IN ((1, 2))": notSupported("SELECT a FROM db1.o WHERE a IN ((1, 2))"),
		}}, "driver-grouped-literal-tuple", "code=UnsupportedStatement")
	})
	t.Run("an unmodelled-class pass-through is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SYSTEM RELOAD CONFIG": success("SYSTEM RELOAD CONFIG"),
		}}, "unmodelled-class-refused")
	})
	t.Run("T5 answered before T3 is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM merge('phys', 'db2')": {Code: pb.RewriteCode_UnsupportedStatement, SqlAfterRewrite: "SELECT * FROM merge('phys', 'db2')", Message: "table function merge is not accepted"},
		}}, "protected-carrier-argument", "code=UnsupportedStatement")
	})
	t.Run("the other engine's quoting is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM db1.o WHERE a IN db1.p": success(`SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`),
		}}, "in-operand-rewritten", "SQL fingerprint mismatch")
	})
	t.Run("a storage-integrity acknowledgement is refused", func(t *testing.T) {
		refused(t, &policyProbeBackend{engine: EngineGRPC, override: map[string]*pb.RewriteSQLResponse{
			"SELECT * FROM {p:Identifier}": {Code: pb.RewriteCode_InvalidRewriteRequest, SqlAfterRewrite: "SELECT * FROM {p:Identifier}",
				Message: "query parameters are not supported in a database or table position", StorageIntegrityContractVersion: StorageIntegrityContractV2},
		}}, "param-table-position", "acknowledgement")
	})
	t.Run("a transport error is refused", func(t *testing.T) {
		be := &scriptedProbeBackend{}
		err := newFakeFactory(be).ProbeTableReferencePolicy(context.Background())
		if err == nil || !strings.Contains(err.Error(), "probe=param-table-position") || !strings.Contains(err.Error(), TableReferenceProbeRequiredBuild) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no backend", func(t *testing.T) {
		if err := newFakeFactory(nil).ProbeTableReferencePolicy(context.Background()); err == nil || !strings.Contains(err.Error(), "no rewrite backend") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestTableReferenceProbeAnswer(t *testing.T) {
	probe := &pb.RewriteSQLRequest{Sql: "USE phys", Options: []*pb.RewriteOption{rewriteOption(tableReferenceProbeArgs())}}
	resp, ok := TableReferenceProbeAnswer(probe, EngineGRPC)
	if !ok || resp.GetCode() != pb.RewriteCode_InvalidRewriteRequest || resp.GetSqlAfterRewrite() != "USE phys" ||
		resp.GetMessage() != "protected database phys is not addressable" {
		t.Fatalf("answer = %v, %v", resp, ok)
	}
	native, ok := TableReferenceProbeAnswer(&pb.RewriteSQLRequest{Sql: "INSERT INTO db1.o SELECT * FROM db1.p", Options: probe.GetOptions()}, EngineNative)
	if !ok || native.GetSqlAfterRewrite() != `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.p" "db1.p"` {
		t.Fatalf("native answer = %v, %v", native, ok)
	}
	production := &pb.RewriteSQLRequest{Sql: "USE phys", Options: []*pb.RewriteOption{rewriteOption(
		buildDynamicArgs(map[string]string{"db1": "phys"}, []string{"phys"}, protectedDatabases("phys"), "db1", "phys", "_", nil, nil, nil))}}
	if _, ok := TableReferenceProbeAnswer(production, EngineGRPC); ok {
		t.Fatal("a production request (physical context set) must not be taken for a probe")
	}
	if _, ok := TableReferenceProbeAnswer(&pb.RewriteSQLRequest{Sql: "SELECT 1", Options: probe.GetOptions()}, EngineGRPC); ok {
		t.Fatal("SQL outside the probe table must not be answered")
	}
	si := &pb.RewriteSQLRequest{Sql: "SYSTEM RELOAD CONFIG", Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs(true))}}
	if _, ok := TableReferenceProbeAnswer(si, EngineGRPC); ok {
		t.Fatal("a storage-integrity probe request must not be answered as a policy probe")
	}
}
