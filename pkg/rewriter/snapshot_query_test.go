package rewriter

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const snapshotTestProfile = "0x1111111111111111111111111111111111111111111111111111111111111111"

type blockedSnapshotQueryServer struct {
	pb.UnimplementedRewriterServiceServer
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func (s *blockedSnapshotQueryServer) block(ctx context.Context) error {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	<-s.release
	close(s.returned)
	return ctx.Err()
}

func (s *blockedSnapshotQueryServer) AnalyzeSnapshotQuery(ctx context.Context, _ *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, s.block(ctx)
}

func (s *blockedSnapshotQueryServer) PrepareSnapshotQuery(ctx context.Context, _ *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	return nil, s.block(ctx)
}

func snapshotTestCatalog() []*pb.SnapshotQueryCatalogTable {
	return []*pb.SnapshotQueryCatalogTable{
		{
			Database: "tenant", Table: "copy",
			TableId:    "0x0000000000000000000000000000000000000000000000000000000000000001",
			SchemaHash: "0x0000000000000000000000000000000000000000000000000000000000000065",
			Columns:    []*pb.SnapshotQueryColumn{{Name: "value", Type: "Int64", Generation: pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY}},
		},
		{
			Database: "tenant", Table: "events",
			TableId:    "0x0000000000000000000000000000000000000000000000000000000000000003",
			SchemaHash: "0x0000000000000000000000000000000000000000000000000000000000000067",
			Columns:    []*pb.SnapshotQueryColumn{{Name: "value", Type: "Int64", Generation: pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY}},
		},
	}
}

func snapshotTestRequest(sql string) *pb.AnalyzeSnapshotQueryRequest {
	return &pb.AnalyzeSnapshotQueryRequest{
		ContractVersion: 1, QueryProfileId: snapshotTestProfile, Sql: sql,
		LogicalDatabase: "tenant", Catalog: snapshotTestCatalog(),
	}
}

func snapshotSuccess(req *pb.AnalyzeSnapshotQueryRequest) *pb.AnalyzeSnapshotQueryResponse {
	return &pb.AnalyzeSnapshotQueryResponse{
		ContractVersion: 1, QueryProfileId: req.GetQueryProfileId(), Code: pb.SnapshotQueryCode_SUCCESS,
		SqlAfterMaterialization: strings.TrimSuffix(req.GetSql(), ";"),
		TargetTableId:           req.GetCatalog()[0].GetTableId(), TargetColumns: []string{"value"},
	}
}

func TestSnapshotQueryAnalyzeRejectsEveryUnacknowledgedChannel(t *testing.T) {
	req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7;")
	cases := map[string]func(*pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error){
		"nil response": func(*pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) { return nil, nil },
		"transport": func(*pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			return nil, errors.New("transport down")
		},
		"unknown version": func(r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			out := snapshotSuccess(r)
			out.ContractVersion = 2
			return out, nil
		},
		"wrong profile": func(r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			out := snapshotSuccess(r)
			out.QueryProfileId = strings.Repeat("0", 66)
			return out, nil
		},
		"profile unavailable": func(r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.GetQueryProfileId(), Code: pb.SnapshotQueryCode_PROFILE_UNAVAILABLE}, nil
		},
		"unsupported": func(r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.GetQueryProfileId(), Code: pb.SnapshotQueryCode_UNSUPPORTED}, nil
		},
		"ordinary": func(r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.GetQueryProfileId(), Code: pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY}, nil
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				return fn(r)
			}})
			if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("expected fail-closed analysis rejection")
			}
		})
	}
}

func TestSnapshotQueryAnalyzeValidatesInputBoundsAndMetadata(t *testing.T) {
	called := false
	a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
		called = true
		return snapshotSuccess(r), nil
	}})

	cases := map[string]*pb.AnalyzeSnapshotQueryRequest{
		"nil": nil,
		"wrong contract": func() *pb.AnalyzeSnapshotQueryRequest {
			r := snapshotTestRequest("SELECT 1")
			r.ContractVersion = 2
			return r
		}(),
		"empty profile": func() *pb.AnalyzeSnapshotQueryRequest {
			r := snapshotTestRequest("SELECT 1")
			r.QueryProfileId = ""
			return r
		}(),
		"large sql": snapshotTestRequest(strings.Repeat("x", snapshotQueryMaxSQLBytes+1)),
		"large descriptor": func() *pb.AnalyzeSnapshotQueryRequest {
			r := snapshotTestRequest("SELECT 1")
			r.Catalog[0].Columns[0].Type = strings.Repeat("x", snapshotQueryMaxDescriptorBytes+1)
			return r
		}(),
		"missing table": func() *pb.AnalyzeSnapshotQueryRequest {
			r := snapshotTestRequest("SELECT 1")
			r.Catalog[0].Table = ""
			return r
		}(),
		"nil column": func() *pb.AnalyzeSnapshotQueryRequest {
			r := snapshotTestRequest("SELECT 1")
			r.Catalog[0].Columns[0] = nil
			return r
		}(),
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			called = false
			if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("expected input rejection")
			}
			if called {
				t.Fatal("backend called for locally invalid request")
			}
		})
	}
}

func TestSnapshotQueryKnownIneligibleAndContradictoryMetadataReachBackend(t *testing.T) {
	for name, mutate := range map[string]func(*pb.SnapshotQueryColumn){
		"default": func(column *pb.SnapshotQueryColumn) {
			column.Generation = pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT
			column.DefaultExpression = "7"
		},
		"ordinary with expression": func(column *pb.SnapshotQueryColumn) {
			column.Generation = pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY
			column.DefaultExpression = "7"
		},
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
			mutate(req.Catalog[0].Columns[0])
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				called = true
				return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}, nil
			}})
			if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("accepted ineligible metadata")
			}
			if !called {
				t.Fatal("known metadata was rejected before backend eligibility analysis")
			}
		})
	}
}

func TestSnapshotQueryClassifyOnlyAdmitsEmptyOrdinary(t *testing.T) {
	req := snapshotTestRequest("SELECT value FROM tenant.events")
	ordinary := func(r *pb.AnalyzeSnapshotQueryRequest) *pb.AnalyzeSnapshotQueryResponse {
		return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.GetQueryProfileId(), Code: pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY}
	}
	a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
		return ordinary(r), nil
	}})
	if _, err := a.ClassifySnapshotQuery(context.Background(), req); err != nil {
		t.Fatalf("ordinary classification: %v", err)
	}

	malformed := []func(*pb.AnalyzeSnapshotQueryResponse){
		func(r *pb.AnalyzeSnapshotQueryResponse) { r.SqlAfterMaterialization = "SELECT 1" },
		func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetTableId = snapshotTestCatalog()[0].GetTableId() },
		func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetColumns = []string{"value"} },
		func(r *pb.AnalyzeSnapshotQueryResponse) {
			r.ReadTableIds = []string{snapshotTestCatalog()[1].GetTableId()}
		},
		func(r *pb.AnalyzeSnapshotQueryResponse) { r.Message = "ordinary" },
		func(r *pb.AnalyzeSnapshotQueryResponse) { r.ContractVersion = 0 },
	}
	for i, mutate := range malformed {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				out := ordinary(r)
				mutate(out)
				return out, nil
			}})
			if _, err := a.ClassifySnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("accepted malformed ordinary response")
			}
		})
	}
	for name, result := range map[string]struct {
		resp *pb.AnalyzeSnapshotQueryResponse
		err  error
	}{
		"unsupported": {resp: &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_UNSUPPORTED}},
		"transport":   {err: errors.New("transport")},
		"parse":       {resp: &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_INVALID_INPUT}},
	} {
		t.Run(name, func(t *testing.T) {
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				return result.resp, result.err
			}})
			if _, err := a.ClassifySnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("mapped failure to ordinary")
			}
		})
	}
}

func TestSnapshotQueryPrepareRequiresAcknowledgedSuccess(t *testing.T) {
	analysis := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.events")
	exact := &pb.PrepareSnapshotQueryRequest{Analysis: analysis, Bindings: []*pb.SnapshotScratchBinding{{
		TableId: analysis.Catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events",
	}}}
	extra := &pb.PrepareSnapshotQueryRequest{Analysis: analysis, Bindings: append(append([]*pb.SnapshotScratchBinding{}, exact.Bindings...), &pb.SnapshotScratchBinding{
		TableId: analysis.Catalog[0].TableId, ScratchDatabase: "scratch", ScratchTable: "copy",
	})}
	missing := &pb.PrepareSnapshotQueryRequest{Analysis: analysis}
	for name, testCase := range map[string]struct {
		req      *pb.PrepareSnapshotQueryRequest
		response *pb.PrepareSnapshotQueryResponse
	}{
		"nil":             {req: exact},
		"ordinary":        {req: exact, response: &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY}},
		"wrong profile":   {req: exact, response: &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: "wrong", Code: pb.SnapshotQueryCode_SUCCESS}},
		"extra binding":   {req: extra, response: &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_INVALID_INPUT}},
		"missing binding": {req: missing, response: &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_INVALID_INPUT}},
	} {
		t.Run(name, func(t *testing.T) {
			a := newSnapshotQueryAnalyzer(&fakeBackend{prepareFn: func(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
				return testCase.response, nil
			}})
			if _, err := a.PrepareSnapshotQuery(context.Background(), testCase.req); err == nil {
				t.Fatal("accepted unacknowledged preparation")
			}
		})
	}
}

func TestSnapshotQueryPrepareAcceptsExactSuccessfulBinding(t *testing.T) {
	analysis := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.events")
	req := &pb.PrepareSnapshotQueryRequest{Analysis: analysis, Bindings: []*pb.SnapshotScratchBinding{{
		TableId: analysis.Catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events",
	}}}
	a := newSnapshotQueryAnalyzer(&fakeBackend{prepareFn: func(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
		return &pb.PrepareSnapshotQueryResponse{
			ContractVersion: 1, QueryProfileId: snapshotTestProfile, Code: pb.SnapshotQueryCode_SUCCESS,
			SelectSql: "SELECT value FROM scratch.events", TargetTableId: analysis.Catalog[0].TableId,
			TargetColumns: []string{"value"}, ReadTableIds: []string{analysis.Catalog[1].TableId},
		}, nil
	}})
	if _, err := a.PrepareSnapshotQuery(context.Background(), req); err != nil {
		t.Fatalf("PrepareSnapshotQuery: %v", err)
	}
}

func TestSnapshotQueryPrepareTransportFailureIsTyped(t *testing.T) {
	analysis := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.events")
	a := newSnapshotQueryAnalyzer(&fakeBackend{prepareFn: func(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
		return nil, errors.New("transport down")
	}})
	_, err := a.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{
		Analysis: analysis,
		Bindings: []*pb.SnapshotScratchBinding{{
			TableId: analysis.Catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events",
		}},
	})
	var typed *SnapshotQueryError
	if !errors.As(err, &typed) || !strings.Contains(err.Error(), "backend call failed") {
		t.Fatalf("err = %v, want typed transport failure", err)
	}
}

func TestSnapshotQueryAnalyzeRejectsFabricatedOutputIdentity(t *testing.T) {
	req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
	for name, mutate := range map[string]func(*pb.AnalyzeSnapshotQueryResponse){
		"unknown target": func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetTableId = strings.Repeat("f", 66) },
		"unknown column": func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetColumns = []string{"secret"} },
		"unknown read":   func(r *pb.AnalyzeSnapshotQueryResponse) { r.ReadTableIds = []string{strings.Repeat("f", 66)} },
		"unsorted read": func(r *pb.AnalyzeSnapshotQueryResponse) {
			r.ReadTableIds = []string{req.Catalog[1].TableId, req.Catalog[0].TableId}
		},
		"duplicate read": func(r *pb.AnalyzeSnapshotQueryResponse) {
			r.ReadTableIds = []string{req.Catalog[1].TableId, req.Catalog[1].TableId}
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				out := snapshotSuccess(r)
				mutate(out)
				return out, nil
			}})
			if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("accepted fabricated output identity")
			}
		})
	}
}

func TestSnapshotQueryAnalyzeRejectsOversizedResponse(t *testing.T) {
	req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
	a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
		out := snapshotSuccess(r)
		out.SqlAfterMaterialization = strings.Repeat("x", snapshotQueryMaxSQLBytes+1)
		return out, nil
	}})
	if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
		t.Fatal("accepted oversized response")
	}
}

func TestSnapshotQueryExplicitFinalAnalyzeAfterClassification(t *testing.T) {
	var calls atomic.Int32
	be := &fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
		calls.Add(1)
		return snapshotSuccess(r), nil
	}}
	a := newSnapshotQueryAnalyzer(be)
	req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
	if _, err := a.ClassifySnapshotQuery(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("backend calls = %d, want preliminary and explicit final calls", calls.Load())
	}
}

func TestSnapshotQueryCloseIsIdempotentConcurrentAndRejectsLaterCalls(t *testing.T) {
	var closes atomic.Int32
	a := newSnapshotQueryAnalyzer(&fakeBackend{closeFn: func() error { closes.Add(1); return nil }})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	if closes.Load() != 1 {
		t.Fatalf("backend closes = %d, want 1", closes.Load())
	}
	if _, err := a.AnalyzeSnapshotQuery(context.Background(), snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")); !errors.Is(err, ErrSnapshotQueryAnalyzerClosed) {
		t.Fatalf("post-close error = %v", err)
	}
}

func TestSnapshotQueryDeadlineReleasesBlockedCallForClose(t *testing.T) {
	for name, invoke := range map[string]func(context.Context, SnapshotQueryAnalyzer) error{
		"analyze": func(ctx context.Context, analyzer SnapshotQueryAnalyzer) error {
			_, err := analyzer.AnalyzeSnapshotQuery(ctx, snapshotTestRequest("INSERT INTO tenant.copy SELECT 7"))
			return err
		},
		"prepare": func(ctx context.Context, analyzer SnapshotQueryAnalyzer) error {
			analysis := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.events")
			_, err := analyzer.PrepareSnapshotQuery(ctx, &pb.PrepareSnapshotQueryRequest{
				Analysis: analysis,
				Bindings: []*pb.SnapshotScratchBinding{{
					TableId: analysis.Catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events",
				}},
			})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			blocked := &blockedSnapshotQueryServer{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
			var releaseOnce sync.Once
			releaseHandler := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			pb.RegisterRewriterServiceServer(server, blocked)
			go func() { _ = server.Serve(listener) }()
			callCtx, callCancel := context.WithCancel(context.Background())
			var analyzer SnapshotQueryAnalyzer
			t.Cleanup(func() {
				callCancel()
				releaseHandler()
				server.Stop()
				if analyzer != nil {
					_ = analyzer.Close()
				}
			})

			analyzer, err = NewSnapshotQueryAnalyzer(Options{
				Engine: EngineGRPC, ServiceAddr: listener.Addr().String(), Timeout: 80 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			callDone := make(chan error, 1)
			go func() { callDone <- invoke(callCtx, analyzer) }()
			select {
			case <-blocked.entered:
			case err := <-callDone:
				t.Fatalf("call completed before server handler entered: %v", err)
			case <-time.After(time.Second):
				t.Fatal("server handler did not enter before timeout")
			}
			closeDone := make(chan error, 1)
			go func() { closeDone <- analyzer.Close() }()
			select {
			case err := <-callDone:
				var typed *SnapshotQueryError
				if !errors.As(err, &typed) || status.Code(typed.Cause) != codes.DeadlineExceeded {
					t.Fatalf("call error = %v, want typed gRPC deadline", err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked call ignored configured timeout")
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close remained blocked after call deadline")
			}
			releaseHandler()
			select {
			case <-blocked.returned:
			case <-time.After(time.Second):
				t.Fatal("server handler did not return after release")
			}
		})
	}
}

func TestSnapshotQueryUnknownEngineRejected(t *testing.T) {
	if _, err := NewSnapshotQueryAnalyzer(Options{Engine: "carrier-pigeon"}); err == nil {
		t.Fatal("accepted unknown engine")
	}
}

func TestSnapshotQueryNativeRequiresExplicitMeasuredPaths(t *testing.T) {
	if _, err := NewSnapshotQueryAnalyzer(Options{Engine: EngineNative}); err == nil || !strings.Contains(err.Error(), "explicit") {
		t.Fatalf("err = %v, want explicit measured path rejection", err)
	}
}

func TestSnapshotQueryProbeChecksBehaviorAndFinalCall(t *testing.T) {
	var ordinaryCalls, missingCalls int
	be := &fakeBackend{analyzeFn: func(_ context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
		ack := &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId}
		switch {
		case strings.HasPrefix(req.Sql, "SELECT "):
			ordinaryCalls++
			ack.Code = pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY
		case strings.Contains(req.Sql, "ordinary.secret"), strings.Contains(req.Sql, "LIMIT 1"):
			ack.Code = pb.SnapshotQueryCode_UNSUPPORTED
		case strings.Contains(req.Sql, "rand()"):
			ack.Code = pb.SnapshotQueryCode_MATERIALIZATION_FAILED
		case req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED:
			missingCalls++
			ack.Code = pb.SnapshotQueryCode_UNSUPPORTED
		case req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT || req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED:
			ack.Code = pb.SnapshotQueryCode_UNSUPPORTED
		default:
			return snapshotSuccess(req), nil
		}
		return ack, nil
	}}
	a := newSnapshotQueryAnalyzer(be)
	if err := ProbeSnapshotQuery(context.Background(), a, snapshotTestProfile, snapshotTestCatalog()); err != nil {
		t.Fatalf("ProbeSnapshotQuery: %v", err)
	}
	if missingCalls != 1 {
		t.Fatalf("missing-generation backend calls=%d, want 1", missingCalls)
	}
	if ordinaryCalls != 2 {
		t.Fatalf("ordinary backend calls = %d, want classification plus explicit final call", ordinaryCalls)
	}
}

func TestSnapshotQueryProbeRejectsResidualSuccessAndMalformedOrdinary(t *testing.T) {
	for name, mutate := range map[string]func(*pb.AnalyzeSnapshotQueryRequest, *pb.AnalyzeSnapshotQueryResponse){
		"wrong exact Q": func(req *pb.AnalyzeSnapshotQueryRequest, resp *pb.AnalyzeSnapshotQueryResponse) {
			resp.QueryProfileId = strings.Repeat("0", 66)
		},
		"residual success": func(req *pb.AnalyzeSnapshotQueryRequest, resp *pb.AnalyzeSnapshotQueryResponse) {
			if strings.Contains(req.Sql, "rand()") {
				*resp = *snapshotSuccess(req)
			}
		},
		"malformed ordinary": func(req *pb.AnalyzeSnapshotQueryRequest, resp *pb.AnalyzeSnapshotQueryResponse) {
			if strings.HasPrefix(req.Sql, "SELECT ") {
				resp.SqlAfterMaterialization = "SELECT value FROM tenant.events"
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			be := &fakeBackend{analyzeFn: func(_ context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				resp := &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId}
				switch {
				case strings.HasPrefix(req.Sql, "SELECT "):
					resp.Code = pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY
				case strings.Contains(req.Sql, "ordinary.secret"), strings.Contains(req.Sql, "LIMIT 1"):
					resp.Code = pb.SnapshotQueryCode_UNSUPPORTED
				case strings.Contains(req.Sql, "rand()"):
					resp.Code = pb.SnapshotQueryCode_MATERIALIZATION_FAILED
				case req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT || req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED:
					resp.Code = pb.SnapshotQueryCode_UNSUPPORTED
				default:
					resp = snapshotSuccess(req)
				}
				mutate(req, resp)
				return resp, nil
			}}
			if err := ProbeSnapshotQuery(context.Background(), newSnapshotQueryAnalyzer(be), snapshotTestProfile, snapshotTestCatalog()); err == nil {
				t.Fatal("probe accepted incorrect backend behavior")
			}
		})
	}
}

func TestSnapshotQueryProbeRequiresExactOrdinaryClassificationAndFinalRefusal(t *testing.T) {
	for name, analyze := range map[string]func(int, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error){
		"success classification": func(call int, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			if strings.HasPrefix(req.Sql, "SELECT ") && call == 1 {
				return snapshotSuccess(req), nil
			}
			return nil, errors.New("transport after misclassification")
		},
		"final transport": func(call int, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			if strings.HasPrefix(req.Sql, "SELECT ") && call == 1 {
				return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY}, nil
			}
			return nil, errors.New("transport on final call")
		},
		"final wrong profile": func(call int, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			resp := &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY}
			if strings.HasPrefix(req.Sql, "SELECT ") && call == 2 {
				resp.QueryProfileId = strings.Repeat("0", 66)
			}
			return resp, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			var ordinaryCalls int
			backend := &fakeBackend{analyzeFn: func(_ context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				if strings.HasPrefix(req.Sql, "SELECT ") {
					ordinaryCalls++
					return analyze(ordinaryCalls, req)
				}
				switch {
				case strings.Contains(req.Sql, "ordinary.secret"), strings.Contains(req.Sql, "LIMIT 1"):
					return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}, nil
				case strings.Contains(req.Sql, "rand()"):
					return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_MATERIALIZATION_FAILED}, nil
				case req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT || req.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED:
					return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}, nil
				default:
					return snapshotSuccess(req), nil
				}
			}}
			if err := ProbeSnapshotQuery(context.Background(), newSnapshotQueryAnalyzer(backend), snapshotTestProfile, snapshotTestCatalog()); err == nil {
				t.Fatal("probe credited an inexact ordinary contract")
			}
		})
	}
}

func TestSnapshotQueryMeasuredNativeAndGRPC(t *testing.T) {
	profilePath := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_PROFILE_PATH")
	profileID := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_PROFILE_ID")
	ffiPath := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_FFI_PATH")
	grpcAddr := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_GRPC_ADDR")
	if profilePath == "" || profileID == "" || ffiPath == "" || grpcAddr == "" {
		t.Skip("set HOUSEGATE_SNAPSHOT_QUERY_PROFILE_PATH, HOUSEGATE_SNAPSHOT_QUERY_PROFILE_ID, HOUSEGATE_SNAPSHOT_QUERY_FFI_PATH, and HOUSEGATE_SNAPSHOT_QUERY_GRPC_ADDR")
	}
	for name, opts := range map[string]Options{
		"native": {Engine: EngineNative, NativeLibraryPath: ffiPath, SnapshotQueryProfilePath: profilePath},
		"grpc":   {Engine: EngineGRPC, ServiceAddr: grpcAddr},
	} {
		t.Run(name, func(t *testing.T) {
			analyzer, err := NewSnapshotQueryAnalyzer(opts)
			if err != nil {
				t.Fatalf("NewSnapshotQueryAnalyzer: %v", err)
			}
			t.Cleanup(func() {
				if err := analyzer.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			checkSnapshotMeasuredCatalogSemantics(t, analyzer, profileID)
			catalog := snapshotTestCatalog()
			if err := ProbeSnapshotQuery(context.Background(), analyzer, profileID, catalog); err != nil {
				t.Fatalf("ProbeSnapshotQuery: %v", err)
			}
			analysisReq := &pb.AnalyzeSnapshotQueryRequest{
				ContractVersion: 1, QueryProfileId: profileID,
				Sql:             "INSERT INTO tenant.copy SELECT value FROM tenant.events",
				LogicalDatabase: "tenant", Catalog: catalog,
			}
			analysis, err := analyzer.AnalyzeSnapshotQuery(context.Background(), analysisReq)
			if err != nil {
				t.Fatalf("AnalyzeSnapshotQuery: %v", err)
			}
			if len(analysis.GetReadTableIds()) != 1 {
				t.Fatalf("read ids = %v, want one", analysis.GetReadTableIds())
			}
			prepared, err := analyzer.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{
				Analysis: analysisReq,
				Bindings: []*pb.SnapshotScratchBinding{{
					TableId: analysis.GetReadTableIds()[0], ScratchDatabase: "scratch", ScratchTable: "events",
				}},
			})
			if err != nil {
				t.Fatalf("PrepareSnapshotQuery: %v", err)
			}
			if prepared.GetSelectSql() == "" {
				t.Fatal("PrepareSnapshotQuery returned empty SQL")
			}
		})
	}
}

func TestSnapshotQueryMeasuredNativeRefusesOldProfile(t *testing.T) {
	profilePath := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_OLD_PROFILE_PATH")
	profileID := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_OLD_PROFILE_ID")
	ffiPath := os.Getenv("HOUSEGATE_SNAPSHOT_QUERY_FFI_PATH")
	if profilePath == "" || profileID == "" || ffiPath == "" {
		t.Skip("set HOUSEGATE_SNAPSHOT_QUERY_OLD_PROFILE_PATH, HOUSEGATE_SNAPSHOT_QUERY_OLD_PROFILE_ID, and HOUSEGATE_SNAPSHOT_QUERY_FFI_PATH")
	}
	analyzer, err := NewSnapshotQueryAnalyzer(Options{
		Engine: EngineNative, NativeLibraryPath: ffiPath, SnapshotQueryProfilePath: profilePath,
	})
	if err != nil {
		t.Fatalf("NewSnapshotQueryAnalyzer: %v", err)
	}
	defer analyzer.Close()
	resp, err := analyzer.AnalyzeSnapshotQuery(context.Background(), &pb.AnalyzeSnapshotQueryRequest{
		ContractVersion: 1, QueryProfileId: profileID, Sql: "INSERT INTO tenant.copy SELECT 7",
		LogicalDatabase: "tenant", Catalog: snapshotTestCatalog(),
	})
	if resp != nil {
		t.Fatalf("old profile response = %+v, want no success payload", resp)
	}
	var typed *SnapshotQueryError
	if !errors.As(err, &typed) || typed.Code != pb.SnapshotQueryCode_UNSPECIFIED || !strings.Contains(err.Error(), "did not acknowledge") {
		t.Fatalf("old profile error = %v, want unacknowledged refusal for nonmember HG executable", err)
	}
}

func TestSnapshotQueryCatalogSemantics(t *testing.T) {
	for _, semantic := range []bool{false, true} {
		for _, self := range []bool{false, true} {
			name := "digest"
			if semantic {
				name = "semantic"
			}
			if self {
				name += " self insert"
			}
			t.Run(name, func(t *testing.T) {
				req := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.events")
				if !semantic {
					req.Catalog[0].TableId = "0x" + strings.Repeat("1", 64)
					req.Catalog[1].TableId = "0x" + strings.Repeat("3", 64)
				}
				if semantic {
					req.Catalog[0].TableId = "copy"
					req.Catalog[1].TableId = "events"
				}
				readID := req.Catalog[1].TableId
				if self {
					req.Sql = "INSERT INTO tenant.copy SELECT value FROM tenant.copy"
					readID = req.Catalog[0].TableId
				}
				a := newSnapshotQueryAnalyzer(&fakeBackend{
					analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
						out := snapshotSuccess(r)
						out.ReadTableIds = []string{readID}
						return out, nil
					},
					prepareFn: func(_ context.Context, r *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
						return &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.Analysis.QueryProfileId, Code: pb.SnapshotQueryCode_SUCCESS, TargetTableId: req.Catalog[0].TableId, TargetColumns: []string{"value"}, ReadTableIds: []string{readID}, SelectSql: "SELECT value FROM scratch.source"}, nil
					},
				})
				for _, call := range []func(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error){a.AnalyzeSnapshotQuery, a.ClassifySnapshotQuery} {
					out, err := call(context.Background(), req)
					if err != nil {
						t.Errorf("analysis: %v", err)
					} else if len(out.ReadTableIds) != 1 || out.ReadTableIds[0] != readID {
						t.Errorf("lost read closure: %v", out.ReadTableIds)
					}
				}
				out, err := a.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{Analysis: req, Bindings: []*pb.SnapshotScratchBinding{{TableId: readID, ScratchDatabase: "scratch", ScratchTable: "source"}}})
				if err != nil {
					t.Errorf("prepare: %v", err)
				} else if len(out.ReadTableIds) != 1 || out.ReadTableIds[0] != readID {
					t.Errorf("lost prepared read closure: %v", out.ReadTableIds)
				}
			})
		}
	}
}

func TestSnapshotQueryGenerationEligibilityBelongsToBackend(t *testing.T) {
	for name, column := range map[string]*pb.SnapshotQueryColumn{
		"missing":     {Name: "value", Type: "Int64"},
		"unspecified": {Name: "value", Type: "Int64", Generation: pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED},
		"unknown":     {Name: "value", Type: "Int64", Generation: pb.SnapshotQueryColumnGeneration(99)},
	} {
		for _, touched := range []bool{false, true} {
			scope := " untouched"
			if touched {
				scope = " touched"
			}
			t.Run(name+scope, func(t *testing.T) {
				req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
				index := 1
				if touched {
					index = 0
				}
				req.Catalog[index].Columns[0] = column
				calls := 0
				a := newSnapshotQueryAnalyzer(&fakeBackend{
					analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
						calls++
						if len(r.Catalog) != 2 || r.Catalog[index].Columns[0].Generation != column.Generation {
							t.Fatal("catalog changed before backend")
						}
						if touched {
							return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}, nil
						}
						return snapshotSuccess(r), nil
					},
					prepareFn: func(_ context.Context, r *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
						calls++
						if len(r.Analysis.Catalog) != 2 || r.Analysis.Catalog[index].Columns[0].Generation != column.Generation {
							t.Fatal("prepare catalog changed before backend")
						}
						out := &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.Analysis.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}
						if !touched {
							out.Code = pb.SnapshotQueryCode_SUCCESS
							out.SelectSql = "SELECT 7"
							out.TargetTableId = req.Catalog[0].TableId
							out.TargetColumns = []string{"value"}
						}
						return out, nil
					},
				})
				check := func(err error) {
					t.Helper()
					var typed *SnapshotQueryError
					if touched {
						if !errors.As(err, &typed) || typed.Code != pb.SnapshotQueryCode_UNSUPPORTED {
							t.Errorf("want acknowledged UNSUPPORTED: %v", err)
						}
					} else if err != nil {
						t.Errorf("untouched U rejected: %v", err)
					}
				}
				_, err := a.AnalyzeSnapshotQuery(context.Background(), req)
				check(err)
				_, err = a.ClassifySnapshotQuery(context.Background(), req)
				check(err)
				_, err = a.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{Analysis: req})
				check(err)
				if calls != 3 {
					t.Errorf("backend calls=%d, want 3", calls)
				}
			})
		}
	}
}

func TestSnapshotQueryStructuralIdentityGuards(t *testing.T) {
	for name, mutate := range map[string]func(*pb.AnalyzeSnapshotQueryRequest){
		"empty ID":          func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].TableId = "" },
		"blank ID":          func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].TableId = " " },
		"duplicate ID":      func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[1].TableId = r.Catalog[0].TableId },
		"bad schema digest": func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].SchemaHash = "schema" },
		"bad Q digest":      func(r *pb.AnalyzeSnapshotQueryRequest) { r.QueryProfileId = "profile" },
		"missing name":      func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].Columns[0].Name = "" },
		"missing type":      func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].Columns[0].Type = "" },
		"duplicate column": func(r *pb.AnalyzeSnapshotQueryRequest) {
			r.Catalog[0].Columns = append(r.Catalog[0].Columns, r.Catalog[0].Columns[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
			mutate(req)
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				t.Fatal("backend called on invalid structure")
				return nil, nil
			}})
			if _, err := a.AnalyzeSnapshotQuery(context.Background(), req); err == nil {
				t.Fatal("accepted invalid structure")
			}
		})
	}
	catalog := snapshotTestCatalog()
	copyID, eventsID := catalog[0].TableId, catalog[1].TableId
	for name, ids := range map[string][]string{"empty": {""}, "unknown": {"absent"}, "duplicate": {copyID, copyID}, "unsorted": {eventsID, copyID}, "missing": {copyID}, "extra": {copyID, eventsID, "other"}} {
		t.Run("bindings "+name, func(t *testing.T) {
			req := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.copy UNION ALL SELECT value FROM tenant.events")
			prepare := &pb.PrepareSnapshotQueryRequest{Analysis: req}
			for i, id := range ids {
				prepare.Bindings = append(prepare.Bindings, &pb.SnapshotScratchBinding{TableId: id, ScratchDatabase: "scratch", ScratchTable: strings.Repeat("x", i+1)})
			}
			a := newSnapshotQueryAnalyzer(&fakeBackend{prepareFn: func(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
				return &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_SUCCESS, SelectSql: "SELECT 7", TargetTableId: copyID, TargetColumns: []string{"value"}, ReadTableIds: []string{copyID, eventsID}}, nil
			}})
			if _, err := a.PrepareSnapshotQuery(context.Background(), prepare); err == nil {
				t.Fatal("accepted invalid bindings")
			}
		})
	}
}

func TestSnapshotQueryProbeRejectsIncorrectMissingGenerationBackend(t *testing.T) {
	for name, mutate := range map[string]func(*pb.AnalyzeSnapshotQueryRequest, *pb.AnalyzeSnapshotQueryResponse){
		"success": func(r *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			*out = *snapshotSuccess(r)
		},
		"wrong code": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.Code = pb.SnapshotQueryCode_INVALID_INPUT
		},
		"wrong version": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) { out.ContractVersion = 2 },
		"wrong Q": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.QueryProfileId = "other"
		},
		"SQL output": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.SqlAfterMaterialization = "SELECT 7"
		},
		"target output": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.TargetTableId = "copy"
		},
		"columns output": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.TargetColumns = []string{"value"}
		},
		"reads output": func(_ *pb.AnalyzeSnapshotQueryRequest, out *pb.AnalyzeSnapshotQueryResponse) {
			out.ReadTableIds = []string{"events"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			missingCalls := 0
			a := newSnapshotQueryAnalyzer(&fakeBackend{analyzeFn: func(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
				out := &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}
				switch {
				case r.Catalog[0].Columns[0].Generation == pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED:
					missingCalls++
					mutate(r, out)
				case strings.Contains(r.Sql, "rand()"):
					out.Code = pb.SnapshotQueryCode_MATERIALIZATION_FAILED
				case strings.Contains(r.Sql, "ordinary.secret"), strings.Contains(r.Sql, "LIMIT 1"):
				default:
					out = snapshotSuccess(r)
				}
				return out, nil
			}})
			err := ProbeSnapshotQuery(context.Background(), a, snapshotTestProfile, snapshotTestCatalog())
			if err == nil || !strings.Contains(err.Error(), "missing generation") || missingCalls != 1 {
				t.Fatalf("err=%v missing calls=%d", err, missingCalls)
			}
		})
	}
}

type localSnapshotRefusal struct{ SnapshotQueryAnalyzer }

func (localSnapshotRefusal) AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, &SnapshotQueryError{Code: pb.SnapshotQueryCode_UNSUPPORTED}
}
func TestSnapshotQueryProbeDoesNotCreditLocalTypedError(t *testing.T) {
	if err := expectSnapshotProbeRejection(context.Background(), localSnapshotRefusal{}, snapshotTestRequest("INSERT INTO tenant.copy SELECT 7"), pb.SnapshotQueryCode_UNSUPPORTED); err == nil {
		t.Fatal("credited a local typed error as a backend acknowledgement")
	}
}

// These assertions run against both freshly measured native and actual gRPC engines.
func checkSnapshotMeasuredCatalogSemantics(t *testing.T, analyzer SnapshotQueryAnalyzer, profileID string) {
	t.Helper()
	t.Run("self_insert", func(t *testing.T) {
		req := snapshotTestRequest("INSERT INTO tenant.copy SELECT value FROM tenant.copy")
		req.QueryProfileId = profileID
		analyzed, err := analyzer.AnalyzeSnapshotQuery(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if analyzed.TargetTableId != req.Catalog[0].TableId || len(analyzed.ReadTableIds) != 1 || analyzed.ReadTableIds[0] != req.Catalog[0].TableId {
			t.Fatalf("self-insert closure: %v", analyzed)
		}
		prepared, err := analyzer.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{Analysis: req, Bindings: []*pb.SnapshotScratchBinding{{TableId: req.Catalog[0].TableId, ScratchDatabase: "scratch", ScratchTable: "old_copy"}}})
		if err != nil {
			t.Fatal(err)
		}
		if prepared.TargetTableId != req.Catalog[0].TableId || len(prepared.ReadTableIds) != 1 || prepared.ReadTableIds[0] != req.Catalog[0].TableId || !strings.Contains(prepared.SelectSql, "old_copy") {
			t.Fatalf("self-insert preparation: %v", prepared)
		}
	})
	for name, generation := range map[string]pb.SnapshotQueryColumnGeneration{"missing": 0, "unknown": 99} {
		for _, scope := range []string{"target", "read", "untouched"} {
			t.Run(name+"_"+scope, func(t *testing.T) {
				req := snapshotTestRequest("INSERT INTO tenant.copy SELECT 7")
				req.QueryProfileId = profileID
				index := 1
				if scope == "target" {
					index = 0
				}
				if scope == "read" {
					req.Sql = "INSERT INTO tenant.copy SELECT value FROM tenant.events"
				}
				req.Catalog[index].Columns[0].Generation = generation
				analyzed, err := analyzer.AnalyzeSnapshotQuery(context.Background(), req)
				if scope == "untouched" {
					if err != nil {
						t.Fatal(err)
					}
					if analyzed.TargetTableId != req.Catalog[0].TableId || len(analyzed.ReadTableIds) != 0 {
						t.Fatalf("untouched analysis: %v", analyzed)
					}
				} else {
					var typed *SnapshotQueryError
					if analyzed != nil || !errors.As(err, &typed) || !typed.acknowledged || typed.Code != pb.SnapshotQueryCode_UNSUPPORTED {
						t.Fatalf("touched generation: %v, %v", analyzed, err)
					}
				}
				bindings := []*pb.SnapshotScratchBinding(nil)
				if scope == "read" {
					bindings = []*pb.SnapshotScratchBinding{{TableId: req.Catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events"}}
				}
				prepared, err := analyzer.PrepareSnapshotQuery(context.Background(), &pb.PrepareSnapshotQueryRequest{Analysis: req, Bindings: bindings})
				if scope == "untouched" {
					if err != nil || prepared.SelectSql == "" || prepared.TargetTableId != req.Catalog[0].TableId || len(prepared.ReadTableIds) != 0 {
						t.Fatalf("untouched prepare: %v, %v", prepared, err)
					}
				} else {
					var typed *SnapshotQueryError
					if prepared != nil || !errors.As(err, &typed) || typed.Code != pb.SnapshotQueryCode_UNSUPPORTED {
						t.Fatalf("touched prepare: %v, %v", prepared, err)
					}
				}
			})
		}
	}
}

// Exercise the actual measured-case driver without crediting a fake as engine qualification.
func TestSnapshotMeasuredFixtureContract(t *testing.T) {
	catalog := snapshotTestCatalog()
	seen := map[string]bool{}
	for _, table := range catalog {
		if !snapshotQueryDigest(table.TableId) || !snapshotQueryDigest(table.SchemaHash) {
			t.Errorf("measured catalog identity must be a lowercase digest: %s.%s ID=%q schema=%q", table.Database, table.Table, table.TableId, table.SchemaHash)
		}
		if seen[table.TableId] {
			t.Errorf("duplicate measured table ID: %q", table.TableId)
		}
		seen[table.TableId] = true
	}
	// JSON compares exported contract fields, excluding protobuf runtime caches.
	equal := func(a, b any) bool {
		left, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		right, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		return string(left) == string(right)
	}
	calls := 0
	check := func(req *pb.AnalyzeSnapshotQueryRequest) *pb.AnalyzeSnapshotQueryResponse {
		calls++
		want := snapshotTestRequest(req.Sql)
		mutations := 0
		touched := false
		for i, table := range req.Catalog {
			generation := table.Columns[0].Generation
			if generation != pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY {
				if generation != 0 && generation != 99 {
					t.Errorf("unexpected generation: %v", generation)
				}
				mutations++
				want.Catalog[i].Columns[0].Generation = generation
				touched = i == 0 || strings.Contains(req.Sql, "FROM tenant.events")
			}
		}
		self := req.Sql == "INSERT INTO tenant.copy SELECT value FROM tenant.copy"
		if (self && mutations != 0) || (!self && mutations != 1) || !equal(req, want) {
			t.Errorf("measured generation control changed more than generation: %v", req)
		}
		out := snapshotSuccess(req)
		if touched {
			return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: req.QueryProfileId, Code: pb.SnapshotQueryCode_UNSUPPORTED}
		}
		if self {
			out.ReadTableIds = []string{catalog[0].TableId}
		}
		return out
	}
	analyzer := newSnapshotQueryAnalyzer(&fakeBackend{
		analyzeFn: func(_ context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
			return check(req), nil
		},
		prepareFn: func(_ context.Context, req *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
			var want []*pb.SnapshotScratchBinding
			if strings.Contains(req.Analysis.Sql, "FROM tenant.copy") {
				want = []*pb.SnapshotScratchBinding{{TableId: catalog[0].TableId, ScratchDatabase: "scratch", ScratchTable: "old_copy"}}
			} else if strings.Contains(req.Analysis.Sql, "FROM tenant.events") {
				want = []*pb.SnapshotScratchBinding{{TableId: catalog[1].TableId, ScratchDatabase: "scratch", ScratchTable: "events"}}
			}
			if !equal(req.Bindings, want) {
				t.Errorf("measured bindings do not match catalog: got %v, want %v", req.Bindings, want)
			}
			out := check(req.Analysis)
			prepared := &pb.PrepareSnapshotQueryResponse{ContractVersion: out.ContractVersion, QueryProfileId: out.QueryProfileId, Code: out.Code, TargetTableId: out.TargetTableId, TargetColumns: out.TargetColumns, ReadTableIds: out.ReadTableIds}
			if out.Code == pb.SnapshotQueryCode_SUCCESS {
				prepared.SelectSql = "SELECT value FROM scratch.old_copy"
			}
			return prepared, nil
		},
	})
	checkSnapshotMeasuredCatalogSemantics(t, analyzer, snapshotTestProfile)
	if calls != 14 {
		t.Fatalf("measured fixture calls=%d, want 7 cases x analyze/prepare", calls)
	}
}

type probeFailingAnalyzer struct{ err error }

func (a *probeFailingAnalyzer) AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) ClassifySnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) PrepareSnapshotQuery(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	return nil, a.err
}
func (a *probeFailingAnalyzer) Close() error { return nil }

func TestExpectSnapshotProbeRejectionWrapsCause(t *testing.T) {
	sentinel := errors.New("transport down")
	err := expectSnapshotProbeRejection(context.Background(), &probeFailingAnalyzer{err: sentinel}, &pb.AnalyzeSnapshotQueryRequest{}, pb.SnapshotQueryCode_INVALID_INPUT)
	if !errors.Is(err, sentinel) {
		t.Fatalf("probe error does not wrap its cause: %v", err)
	}
}
