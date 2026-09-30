package rewriter

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/network"
)

// These tests pin review M1 against the real gRPC backend: only a failure
// before the request reached the engine (connect failure, a deadline that
// expired before the request was sent) is "unavailable" and may fail open
// under fail_open_on_unavailable. Any failure after the request was sent
// (an engine status error, a crash mid-call, a deadline that expired while
// the engine worked) is statement-dependent and always a rejection.

// grpcFailureServer answers Rewrite through a scripted function.
type grpcFailureServer struct {
	pb.UnimplementedRewriterServiceServer
	handle func(ctx context.Context) (*pb.RewriteSQLResponse, error)
}

func (s *grpcFailureServer) Rewrite(ctx context.Context, _ *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	return s.handle(ctx)
}

func startGRPCFailureServer(t *testing.T, handle func(ctx context.Context) (*pb.RewriteSQLResponse, error)) (*grpc.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterRewriterServiceServer(gs, &grpcFailureServer{handle: handle})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return gs, lis.Addr().String()
}

func newGRPCFailureFactory(t *testing.T, addr string, failOpen bool, timeout time.Duration) *SentioNetworkFactory {
	t.Helper()
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}
	f, err := NewSentioNetworkFactory(Options{
		Engine:                EngineGRPC,
		ServiceAddr:           addr,
		Timeout:               timeout,
		PhysicalDatabase:      "phys",
		FailOpenOnUnavailable: failOpen,
	}, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestGRPCBackend_EngineStatusErrorsAreRejectionsEvenUnderTheSwitch(t *testing.T) {
	for _, code := range []codes.Code{codes.Unknown, codes.Internal, codes.Unavailable, codes.InvalidArgument, codes.Unimplemented, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			_, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
				return nil, status.Error(code, "engine failed on this statement")
			})
			f := newGRPCFailureFactory(t, addr, true, 2*time.Second)
			_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("err = %v, want a rejection: the engine received the statement", err)
			}
		})
	}
}

func TestGRPCBackend_EngineCrashMidCallIsARejectionEvenUnderTheSwitch(t *testing.T) {
	received := make(chan struct{})
	var gs *grpc.Server
	gs, addr := startGRPCFailureServer(t, func(ctx context.Context) (*pb.RewriteSQLResponse, error) {
		close(received)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	f := newGRPCFailureFactory(t, addr, true, 5*time.Second)
	go func() {
		<-received
		gs.Stop() // the engine dies while it holds the statement
	}()
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want a rejection: the engine received the statement", err)
	}
}

func TestGRPCBackend_DeadlineAfterSendIsARejectionEvenUnderTheSwitch(t *testing.T) {
	_, addr := startGRPCFailureServer(t, func(ctx context.Context) (*pb.RewriteSQLResponse, error) {
		<-ctx.Done() // a statement that stalls the engine
		return nil, ctx.Err()
	})
	f := newGRPCFailureFactory(t, addr, true, 200*time.Millisecond)
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want a rejection: the deadline expired while the engine held the statement", err)
	}
}

func TestGRPCBackend_ConnectFailureFollowsTheSwitch(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		gs, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
			return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}, nil
		})
		f := newGRPCFailureFactory(t, addr, failOpen, 2*time.Second)
		gs.Stop() // the service is gone before the statement is sent
		waitUntilNotReady(t, f)
		_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
		var rej *RejectedError
		var unavailable *UnavailableError
		if failOpen {
			if errors.As(err, &rej) || !errors.As(err, &unavailable) {
				t.Fatalf("switch on: err = %v, want an *UnavailableError", err)
			}
			continue
		}
		if !errors.As(err, &rej) || rej.Message != "rewriter unavailable" {
			t.Fatalf("switch off: err = %v, want RejectedError(rewriter unavailable)", err)
		}
		if strings.Contains(rej.Error(), addr) {
			t.Fatalf("client message %q leaks the rewriter address", rej.Error())
		}
	}
}

func TestGRPCBackend_DeadlineBeforeSendFollowsTheSwitch(t *testing.T) {
	_, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}, nil
	})
	f := newGRPCFailureFactory(t, addr, true, 2*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(ctx, "SELECT 1", "")
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("err = %v, want an *UnavailableError: the request was never sent", err)
	}
}

// waitUntilNotReady blocks until the factory's gRPC connection has noticed
// the server is gone. A call issued in the instant before that is written to
// a dying transport, which counts as sent (and so as a rejection); the
// outage this test models is the steady state after the service went away.
func waitUntilNotReady(t *testing.T, f *SentioNetworkFactory) {
	t.Helper()
	conn := f.backend.(*grpcBackend).conn
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for state := conn.GetState(); state == connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatal("gRPC connection still READY after the server stopped")
		}
	}
}

// invalidUTF8SQL carries a lone 0xff/0xfe inside a string literal: ClickHouse
// accepts the bytes, but a proto3 string field cannot carry them.
const invalidUTF8SQL = "SELECT '\xff\xfe' FROM phys.`db2.secret`"

// TestGRPCBackend_PreSendMarshalFailureIsNotUnavailable pins re-review R1 at
// the backend seam: grpc-go refuses to marshal invalid UTF-8 with Internal
// before anything is sent. That failure depends on the statement, so it must
// not be classified as unavailable even though the request was never sent.
func TestGRPCBackend_PreSendMarshalFailureIsNotUnavailable(t *testing.T) {
	calls := 0
	_, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
		calls++
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}, nil
	})
	f := newGRPCFailureFactory(t, addr, true, 2*time.Second)
	_, err := f.backend.Rewrite(context.Background(), &pb.RewriteSQLRequest{Sql: invalidUTF8SQL})
	if err == nil {
		t.Fatal("marshalling invalid UTF-8 unexpectedly succeeded")
	}
	var unavailable *UnavailableError
	if errors.As(err, &unavailable) {
		t.Fatalf("err = %v classified as unavailable; a marshalling failure depends on the statement", err)
	}
	if calls != 0 {
		t.Fatalf("engine calls = %d, want 0", calls)
	}
}

// TestGRPCBackend_InvalidUTF8IsRefusedEvenUnderTheSwitch pins re-review R1
// end to end through the rewriter: the statement is refused up front with a
// clear message and never reaches the engine.
func TestGRPCBackend_InvalidUTF8IsRefusedEvenUnderTheSwitch(t *testing.T) {
	calls := 0
	_, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
		calls++
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}, nil
	})
	f := newGRPCFailureFactory(t, addr, true, 2*time.Second)
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), invalidUTF8SQL, "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "not valid UTF-8") {
		t.Fatalf("err = %v, want a RejectedError naming invalid UTF-8", err)
	}
	if calls != 0 {
		t.Fatalf("engine calls = %d, want 0", calls)
	}
}
