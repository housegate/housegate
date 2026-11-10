package rewriter

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/housegate/housegate/pkg/ffifetch/ffitest"
	"github.com/housegate/housegate/pkg/network"
)

// conformingProbeServer answers every table-reference probe the way a
// conforming gRPC engine does, and passes any other statement through.
type conformingProbeServer struct {
	pb.UnimplementedRewriterServiceServer
}

func (conformingProbeServer) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	if resp, ok := TableReferenceProbeAnswer(req, EngineGRPC); ok {
		return resp, nil
	}
	return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: req.GetSql()}, nil
}

// serveProbeServer serves conformingProbeServer on lis until the test ends.
func serveProbeServer(t *testing.T, lis net.Listener) *grpc.Server {
	t.Helper()
	gs := grpc.NewServer()
	pb.RegisterRewriterServiceServer(gs, conformingProbeServer{})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return gs
}

// unusedAddr returns a loopback address nothing listens on.
func unusedAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// nativeProbeBackend answers the table-reference probes as the native
// engine does, standing in for the FFI library.
type nativeProbeBackend struct{ fakeBackend }

func (nativeProbeBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	if resp, ok := TableReferenceProbeAnswer(req, EngineNative); ok {
		return resp, nil
	}
	return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: req.GetSql()}, nil
}

// stubNativeLoader replaces loadNativeBackend for the test and records the
// library paths it was asked to load.
func stubNativeLoader(t *testing.T, err error) *[]string {
	t.Helper()
	var loaded []string
	prev := loadNativeBackend
	loadNativeBackend = func(opts Options) (backend, error) {
		loaded = append(loaded, opts.NativeLibraryPath)
		if err != nil {
			return nil, err
		}
		return &nativeProbeBackend{}, nil
	}
	t.Cleanup(func() { loadNativeBackend = prev })
	return &loaded
}

// countingResolver returns path (or err) and counts its calls.
func countingResolver(path string, err error) (func(context.Context) (string, error), *atomic.Int32) {
	calls := new(atomic.Int32)
	return func(context.Context) (string, error) {
		calls.Add(1)
		return path, err
	}, calls
}

func newAutoFactory(t *testing.T, addr string, wait time.Duration, resolve func(context.Context) (string, error)) (*SentioNetworkFactory, error) {
	t.Helper()
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}
	f, err := NewSentioNetworkFactory(Options{
		Engine:               EngineAuto,
		ServiceAddr:          addr,
		Timeout:              2 * time.Second,
		AutoGRPCWait:         wait,
		ResolveNativeLibrary: resolve,
		NativeLibraryVersion: "v-pinned",
		PhysicalDatabase:     "phys",
	}, st)
	if f != nil {
		t.Cleanup(func() { _ = f.Close() })
	}
	return f, err
}

func TestAutoEngineSelectsGRPCWhenReachable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveProbeServer(t, lis)
	loaded := stubNativeLoader(t, nil)
	resolve, calls := countingResolver("/pinned/lib.so", nil)

	f, err := newAutoFactory(t, lis.Addr().String(), 5*time.Second, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	if f.Engine() != EngineGRPC || f.EngineVersion() != EngineVersionUnknown {
		t.Fatalf("engine = %s %s, want grpc unknown", f.Engine(), f.EngineVersion())
	}
	if calls.Load() != 0 || len(*loaded) != 0 {
		t.Fatalf("native fallback touched with gRPC reachable: resolver=%d loads=%v", calls.Load(), *loaded)
	}
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("gRPC probe under auto: %v", err)
	}
}

func TestAutoEngineWaitsForASidecarThatStartsDuringTheWait(t *testing.T) {
	addr := unusedAddr(t)
	go func() {
		time.Sleep(700 * time.Millisecond)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("late listen on %s: %v", addr, err)
			return
		}
		serveProbeServer(t, lis)
	}()
	loaded := stubNativeLoader(t, nil)
	resolve, calls := countingResolver("/pinned/lib.so", nil)

	start := time.Now()
	f, err := newAutoFactory(t, addr, 10*time.Second, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	if f.Engine() != EngineGRPC {
		t.Fatalf("engine = %s, want grpc once the sidecar came up", f.Engine())
	}
	if calls.Load() != 0 || len(*loaded) != 0 {
		t.Fatalf("native fallback touched: resolver=%d loads=%v", calls.Load(), *loaded)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("found the late sidecar after %s; the capped backoff should find it within about a second", elapsed)
	}
}

func TestAutoEngineFallsBackToThePinnedNativeEngine(t *testing.T) {
	loaded := stubNativeLoader(t, nil)
	resolve, calls := countingResolver("/pinned/lib.so", nil)

	f, err := newAutoFactory(t, unusedAddr(t), 300*time.Millisecond, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	if f.Engine() != EngineNative || f.EngineVersion() != "v-pinned" {
		t.Fatalf("engine = %s %s, want native v-pinned", f.Engine(), f.EngineVersion())
	}
	if calls.Load() != 1 || len(*loaded) != 1 || (*loaded)[0] != "/pinned/lib.so" {
		t.Fatalf("resolver=%d loads=%v, want one load of the resolved path", calls.Load(), *loaded)
	}
	// The probes must expect the native engine's own output, not gRPC's.
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("native probe under auto: %v", err)
	}
}

// TestAutoEngineFallsBackToTheRealPinnedLibrary drives the auto fallback with
// the real native loader and the FFI library CI fetches, so the native probe
// expectations under auto are held against the engine itself, not the stub.
func TestAutoEngineFallsBackToTheRealPinnedLibrary(t *testing.T) {
	lib := ffitest.LibOrSkip(t)
	resolve, calls := countingResolver(lib, nil)

	f, err := newAutoFactory(t, unusedAddr(t), 200*time.Millisecond, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(auto) with the real library: %v", err)
	}
	if f.Engine() != EngineNative || calls.Load() != 1 {
		t.Fatalf("engine = %s after %d resolver calls, want native after one", f.Engine(), calls.Load())
	}
	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("table-reference probe under auto: %v", err)
	}
	if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
		t.Fatalf("storage-integrity build probe under auto: %v", err)
	}
	rw := f.NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatalf("Rewrite under auto: %v", err)
	}
	if !strings.Contains(res.SQL, "phys.") {
		t.Fatalf("SQL = %q, want it moved into the physical database", res.SQL)
	}
}

func TestAutoEngineNativeFallbackFailureRefusesConstruction(t *testing.T) {
	for name, tc := range map[string]struct {
		resolveErr error
		path       string
		loadErr    error
		noResolver bool
		want       string
	}{
		"fetch or checksum failure": {resolveErr: errors.New("sha256 mismatch"), want: "sha256 mismatch"},
		"empty resolved path":       {path: "", want: "empty library path"},
		"load failure":              {path: "/pinned/lib.so", loadErr: errors.New("dlopen failed"), want: "dlopen failed"},
		"no resolver":               {noResolver: true, want: "no pinned native library"},
	} {
		t.Run(name, func(t *testing.T) {
			loaded := stubNativeLoader(t, tc.loadErr)
			resolve, _ := countingResolver(tc.path, tc.resolveErr)
			if tc.noResolver {
				resolve = nil
			}
			f, err := newAutoFactory(t, unusedAddr(t), 200*time.Millisecond, resolve)
			if err == nil {
				t.Fatalf("constructed a factory (engine %s) with both engines unavailable", f.Engine())
			}
			if !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "failed to connect to rewriter service") {
				t.Fatalf("err = %v, want both the gRPC failure and %q", err, tc.want)
			}
			if tc.resolveErr != nil && len(*loaded) != 0 {
				t.Fatalf("loaded %v after the pinned fetch failed", *loaded)
			}
		})
	}
}

func TestAutoEngineGRPCProbeFailureDoesNotFallBack(t *testing.T) {
	// A reachable sidecar that answers Success to everything fails the
	// probe; selection is already made, so the native engine is never tried.
	_, addr := startGRPCFailureServer(t, func(context.Context) (*pb.RewriteSQLResponse, error) {
		return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}, nil
	})
	loaded := stubNativeLoader(t, nil)
	resolve, calls := countingResolver("/pinned/lib.so", nil)

	f, err := newAutoFactory(t, addr, 5*time.Second, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	if err := f.ProbeTableReferencePolicy(context.Background()); err == nil || !strings.Contains(err.Error(), "engine=grpc") {
		t.Fatalf("probe err = %v, want a gRPC probe refusal", err)
	}
	if f.Engine() != EngineGRPC || calls.Load() != 0 || len(*loaded) != 0 {
		t.Fatalf("engine=%s resolver=%d loads=%v after a failed probe; want grpc and no fallback", f.Engine(), calls.Load(), *loaded)
	}
}

func TestAutoEngineGRPCOutageAfterStartupFailsClosedWithoutSwitching(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := serveProbeServer(t, lis)
	loaded := stubNativeLoader(t, nil)
	resolve, calls := countingResolver("/pinned/lib.so", nil)

	f, err := newAutoFactory(t, lis.Addr().String(), 5*time.Second, resolve)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	gs.Stop()
	conn := f.backend.(*grpcBackend).conn
	for deadline := time.Now().Add(5 * time.Second); conn.GetState() == connectivity.Ready; {
		if time.Now().After(deadline) {
			t.Fatal("gRPC connection still Ready after the server stopped")
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = f.backend.Rewrite(ctx, &pb.RewriteSQLRequest{Sql: "SELECT 1"})
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("Rewrite after the outage = %v, want an UnavailableError (fail closed)", err)
	}
	if f.Engine() != EngineGRPC || calls.Load() != 0 || len(*loaded) != 0 {
		t.Fatalf("engine=%s resolver=%d loads=%v after the outage; want grpc and no switch", f.Engine(), calls.Load(), *loaded)
	}
}

func TestMaterializerRefusesAutoEngine(t *testing.T) {
	if _, err := NewMaterializer(Options{Engine: EngineAuto}, 16, ""); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("err = %v, want auto refused for the materializer", err)
	}
}

func TestReportActiveEngine(t *testing.T) {
	ReportActiveEngine("auto", EngineNative, "v-pinned")
	if got := testutil.ToFloat64(activeEngine.WithLabelValues("auto", EngineNative, "v-pinned")); got != 1 {
		t.Fatalf("active engine gauge = %v, want 1", got)
	}
	ReportActiveEngine("", EngineGRPC, EngineVersionUnknown)
	if n := testutil.CollectAndCount(activeEngine); n != 1 {
		t.Fatalf("series = %d, want only the latest engine", n)
	}
	if got := testutil.ToFloat64(activeEngine.WithLabelValues(EngineGRPC, EngineGRPC, EngineVersionUnknown)); got != 1 {
		t.Fatalf("empty configured engine not reported as grpc: %v", got)
	}
}
