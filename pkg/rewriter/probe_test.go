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

	// A V1-only build (rewriter-go < v0.13.0) acknowledges V1, never V2; and a
	// build that acknowledges V2 but still rejects the SI DROP or activates the
	// catch-all by table count is refused on the matching V2 probe.
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
