package rewriter

import (
	"context"
	"fmt"
	"time"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-go/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"housegate/housegate/pkg/log"
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
// (dynamic args, USE mirroring, the fail-open code trichotomy) lives
// above this seam and is shared by both implementations.
// The ctx deadline is fully honored by the grpc implementation; under
// the native engine it is advisory — an FFI call cannot be interrupted
// mid-flight, but calls are local and fast, so deadlines effectively
// never fire there.
type backend interface {
	Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
	RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
	MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error)
	Close() error
}

// grpcBackend is the historical default: a shared client connection to
// the external sql-rewriter service.
type grpcBackend struct {
	conn   *grpc.ClientConn
	client pb.RewriterServiceClient
}

// newGRPCBackend dials the sql-rewriter service synchronously and fails
// fast if it cannot connect — the proxy treats a missing rewriter as
// "rewriting disabled" rather than retrying forever.
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
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rewriter service at %s: %w", opts.ServiceAddr, err)
	}
	log.Infow("connected to rewriter service", "service_addr", opts.ServiceAddr)
	return &grpcBackend{conn: conn, client: pb.NewRewriterServiceClient(conn)}, nil
}

func (b *grpcBackend) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	return b.client.Rewrite(ctx, req)
}

func (b *grpcBackend) RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	return b.client.RewriteErrorMessage(ctx, req)
}

func (b *grpcBackend) MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	return b.client.MaterializeSQL(ctx, req)
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
