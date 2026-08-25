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
	resp, ok := b.responses[req.GetSql()]
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
		if len(be.requests) != 4 || be.requests[0].GetSql() != storageIntegrityProbeSQL ||
			be.requests[1].GetSql() != "SYSTEM RELOAD CONFIG" ||
			be.requests[2].GetSql() != "SYSTEM START MERGES hg_unsafe.db1__t" ||
			be.requests[3].GetSql() != "TRUNCATE DATABASE hg_safe" {
			t.Fatalf("probe SQLs = %v, want DESCRIBE, generic SYSTEM, protected physical SYSTEM, protected physical database", probeSQLs(be.requests))
		}
		si := be.requests[3].GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if si.GetContractVersion() != StorageIntegrityContractV1 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
			t.Fatalf("probe request did not carry the fixed SI args: %v", si)
		}
		for i, hasDeadline := range be.deadlines {
			if !hasDeadline {
				t.Fatalf("probe request %d had no deadline", i)
			}
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
	}
}

func probeSQLs(reqs []*pb.RewriteSQLRequest) []string {
	sqls := make([]string, 0, len(reqs))
	for _, req := range reqs {
		sqls = append(sqls, req.GetSql())
	}
	return sqls
}
