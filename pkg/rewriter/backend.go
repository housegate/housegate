package rewriter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/log"
)

// Engine values for Options.Engine, selecting which backend
// NewSentioNetworkFactory constructs. Empty means EngineGRPC.
//
// EngineAuto picks one of the other two once, at construction: the gRPC
// service when it becomes reachable within Options.AutoGRPCWait, otherwise
// the native engine loaded from the library Options.ResolveNativeLibrary
// returns. The choice is fixed for the factory's lifetime; a later gRPC
// outage fails closed like under EngineGRPC and never switches engines.
const (
	EngineGRPC   = "grpc"
	EngineNative = "native"
	EngineAuto   = "auto"
)

// DefaultAutoGRPCWait bounds how long EngineAuto waits for the gRPC service
// before it falls back to the native engine.
const DefaultAutoGRPCWait = 30 * time.Second

// autoGRPCMaxBackoff caps the reconnect delay while EngineAuto waits, so a
// sidecar that comes up during the wait is found within about a second.
const autoGRPCMaxBackoff = time.Second

// EngineVersionUnknown labels an engine whose build cannot be read: the gRPC
// contract carries no version, and a native library given by path has no
// release tag.
const EngineVersionUnknown = "unknown"

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
	connectTimeout := opts.Timeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	return dialGRPCBackend(opts, EngineGRPC, connectTimeout)
}

// dialGRPCBackend blocks until the service at opts.ServiceAddr is connected
// or connectTimeout expires. Under EngineAuto the reconnect backoff is capped
// at autoGRPCMaxBackoff so the wait notices a late sidecar promptly.
func dialGRPCBackend(opts Options, engine string, connectTimeout time.Duration) (*grpcBackend, error) {
	if opts.ServiceAddr == "" {
		return nil, fmt.Errorf("rewriter service_addr is required when rewriter engine is %q", engine)
	}
	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithStatsHandler(requestSentTracker{}),
		grpc.WithBlock(),
	}
	if engine == EngineAuto {
		bo := backoff.DefaultConfig
		bo.BaseDelay = 100 * time.Millisecond
		bo.MaxDelay = autoGRPCMaxBackoff
		dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{Backoff: bo}))
	}
	conn, err := grpc.DialContext(connectCtx, opts.ServiceAddr, dialOpts...)
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

// loadNativeBackend loads the native engine; tests replace it to cover the
// EngineAuto fallback without the FFI library.
var loadNativeBackend = newNativeBackend

// selectedBackend is a constructed backend and the engine that serves it:
// EngineGRPC or EngineNative, never EngineAuto.
type selectedBackend struct {
	backend backend
	engine  string
	version string
}

// newBackend dispatches on Options.Engine ("" defaults to grpc).
func newBackend(opts Options) (selectedBackend, error) {
	switch opts.Engine {
	case "", EngineGRPC:
		be, err := newGRPCBackend(opts)
		if err != nil {
			return selectedBackend{}, err
		}
		return selectedBackend{backend: be, engine: EngineGRPC, version: EngineVersionUnknown}, nil
	case EngineNative:
		be, err := loadNativeBackend(opts)
		if err != nil {
			return selectedBackend{}, err
		}
		return selectedBackend{backend: be, engine: EngineNative, version: nativeVersion(opts)}, nil
	case EngineAuto:
		return newAutoBackend(opts)
	default:
		return selectedBackend{}, fmt.Errorf("unknown rewriter engine %q (want %q, %q or %q)", opts.Engine, EngineGRPC, EngineNative, EngineAuto)
	}
}

// newAutoBackend selects the engine once. The gRPC service wins when it is
// reachable within the wait; otherwise the native library is resolved and
// loaded. A native resolution or load failure is returned with the gRPC
// failure so the operator sees both, and nothing is constructed.
func newAutoBackend(opts Options) (selectedBackend, error) {
	wait := opts.AutoGRPCWait
	if wait <= 0 {
		wait = DefaultAutoGRPCWait
	}
	grpcBe, grpcErr := dialGRPCBackend(opts, EngineAuto, wait)
	if grpcErr == nil {
		log.Infow("rewriter engine auto: selected grpc", "service_addr", opts.ServiceAddr)
		return selectedBackend{backend: grpcBe, engine: EngineGRPC, version: EngineVersionUnknown}, nil
	}
	log.Warnw("rewriter engine auto: grpc service unreachable, falling back to the pinned native engine",
		"service_addr", opts.ServiceAddr, "grpc_wait", wait.String(), "err", grpcErr)
	if opts.ResolveNativeLibrary == nil {
		return selectedBackend{}, fmt.Errorf("rewriter engine %q: %w; no pinned native library to fall back to", EngineAuto, grpcErr)
	}
	path, err := opts.ResolveNativeLibrary(context.Background())
	if err == nil && path == "" {
		err = fmt.Errorf("resolved an empty library path")
	}
	if err != nil {
		return selectedBackend{}, fmt.Errorf("rewriter engine %q: %w; native fallback: resolve pinned library: %v", EngineAuto, grpcErr, err)
	}
	nativeOpts := opts
	nativeOpts.NativeLibraryPath = path
	be, err := loadNativeBackend(nativeOpts)
	if err != nil {
		return selectedBackend{}, fmt.Errorf("rewriter engine %q: %w; native fallback: %v", EngineAuto, grpcErr, err)
	}
	log.Infow("rewriter engine auto: selected native", "lib", path, "version", nativeVersion(opts))
	return selectedBackend{backend: be, engine: EngineNative, version: nativeVersion(opts)}, nil
}

// nativeVersion is the release tag the native library came from, when known.
func nativeVersion(opts Options) string {
	if opts.NativeLibraryVersion != "" {
		return opts.NativeLibraryVersion
	}
	return EngineVersionUnknown
}
