package rewriter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/log"
)

// Engine values for Options.Engine, selecting which backend
// NewSentioNetworkFactory constructs. Empty means EngineGRPC.
const (
	EngineGRPC   = "grpc"
	EngineNative = "native"
)

// backend abstracts the rewrite transport: the remote sql-rewriter gRPC
// service or the in-process rewriter-go engine. Both speak the same proto
// contract; sentioRewriter cannot tell them apart. All per-session logic
// (dynamic args, USE mirroring, the fail-closed rejection policy) lives
// above this seam and is shared by both implementations. Rewrite returns an
// *UnavailableError only for a transport-level failure before the request
// was sent (see grpcBackend.Rewrite); the native engine is in-process, so
// every error it returns came from the engine and is unmarked.
// The ctx deadline is fully honored by the grpc implementation; under
// the native engine it is advisory — an FFI call cannot be interrupted
// mid-flight, but calls are local and fast, so deadlines effectively
// never fire there.
type backend interface {
	Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
	RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
	MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)
	AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)
	PrepareSnapshotQuery(ctx context.Context, req *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error)
	Close() error
}

// grpcBackend is the historical default: a shared client connection to
// the external sql-rewriter service.
type grpcBackend struct {
	conn   *grpc.ClientConn
	client pb.RewriterServiceClient
}

// newGRPCBackend dials the sql-rewriter service synchronously and fails
// fast if it cannot connect rather than retrying forever; buildServer then
// refuses startup unless rewriter.fail_open_on_unavailable allows running
// without the rewriter.
func newGRPCBackend(opts Options) (*grpcBackend, error) {
	if opts.ServiceAddr == "" {
		return nil, fmt.Errorf("rewriter service_addr is required when rewriter engine is %q", EngineGRPC)
	}
	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	connectTimeout := opts.Timeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel()

	conn, err := grpc.DialContext(connectCtx, opts.ServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithStatsHandler(requestSentTracker{}),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rewriter service at %s: %w", opts.ServiceAddr, err)
	}
	log.Infow("connected to rewriter service", "service_addr", opts.ServiceAddr)
	return &grpcBackend{conn: conn, client: pb.NewRewriterServiceClient(conn)}, nil
}

// Rewrite classifies a failure at the transport boundary (spec 2026-09-26
// T8, review M1, re-review R1). A failure is returned as an *UnavailableError
// only when both hold: the request message was never handed to the transport,
// and the gRPC status is one of the transport codes (Unavailable,
// DeadlineExceeded, Canceled) — a connect failure, or a deadline or
// cancellation that fired before sending. Every other failure is returned
// unmarked and the caller treats it as a rejection: a pre-send failure with
// another code (a marshalling error such as invalid UTF-8, reported as
// Internal, or a client-side ResourceExhausted) depends on the statement, and
// so does any failure after sending (an engine status, a crash that drops the
// connection mid-call, a deadline that expired while the engine worked).
func (b *grpcBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	sent := new(atomic.Bool)
	resp, err := b.client.Rewrite(context.WithValue(ctx, requestSentKey{}, sent), req)
	if err != nil && !sent.Load() && isTransportStatus(err) {
		return nil, &UnavailableError{Cause: err}
	}
	return resp, err
}

// isTransportStatus reports whether err carries a gRPC status that describes
// the connection or the call's own deadline rather than the request content.
func isTransportStatus(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

// requestSentKey carries the per-call flag requestSentTracker sets.
type requestSentKey struct{}

// requestSentTracker is a client stats handler that records, per call,
// whether the request message reached the transport. gRPC reports
// OutPayload only after the message was written, so a call that failed with
// the flag unset never delivered the statement to the engine.
type requestSentTracker struct{}

func (requestSentTracker) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (requestSentTracker) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if out, ok := s.(*stats.OutPayload); ok && out.IsClient() {
		if sent, ok := ctx.Value(requestSentKey{}).(*atomic.Bool); ok {
			sent.Store(true)
		}
	}
}

func (requestSentTracker) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (requestSentTracker) HandleConn(context.Context, stats.ConnStats) {}

func (b *grpcBackend) RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	return b.client.RewriteErrorMessage(ctx, req)
}

func (b *grpcBackend) MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	return b.client.MaterializeSQL(ctx, req)
}

func (b *grpcBackend) AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return b.client.AnalyzeSnapshotQuery(ctx, req)
}

func (b *grpcBackend) PrepareSnapshotQuery(ctx context.Context, req *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	return b.client.PrepareSnapshotQuery(ctx, req)
}

func (b *grpcBackend) Close() error { return b.conn.Close() }

// newNativeBackend loads the in-process rewriter-go engine.
// *rewritergo.Service satisfies backend directly (the signatures mirror
// the gRPC client by construction). The FFI library resolution order is
// opts.NativeLibraryPath, then POLYGLOT_SQL_FFI_PATH, then the system
// default locations.
func newNativeBackend(opts Options) (backend, error) {
	svc, err := rewritergo.NewService(opts.NativeLibraryPath)
	if err != nil {
		return nil, fmt.Errorf("load native rewriter (lib=%q): %w", opts.NativeLibraryPath, err)
	}
	log.Infow("native rewriter engine loaded", "lib", opts.NativeLibraryPath)
	return svc, nil
}

// newBackend dispatches on Options.Engine ("" defaults to grpc).
func newBackend(opts Options) (backend, error) {
	switch opts.Engine {
	case "", EngineGRPC:
		return newGRPCBackend(opts)
	case EngineNative:
		return newNativeBackend(opts)
	default:
		return nil, fmt.Errorf("unknown rewriter engine %q (want %q or %q)", opts.Engine, EngineGRPC, EngineNative)
	}
}
