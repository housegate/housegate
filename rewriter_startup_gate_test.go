package housegate

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/rewriter"
)

// The startup half of spec 2026-09-26 T8: a server that should rewrite but
// cannot build its rewriter refuses to start unless
// rewriter.fail_open_on_unavailable is set with storage integrity disabled.

// rewriterStartupFailures are configs whose rewriter factory cannot be built,
// failing fast without any network wait.
var rewriterStartupFailures = map[string]func(t *testing.T) *config.Config{
	"grpc without service_addr": func(t *testing.T) *config.Config {
		cfg := minimalServerCfg(t)
		cfg.Rewriter.Engine = "grpc"
		cfg.Rewriter.ServiceAddr = ""
		return cfg
	},
	"native library fetch failure": func(t *testing.T) *config.Config {
		cfg := minimalServerCfg(t)
		cfg.Rewriter.Engine = "native"
		cfg.Rewriter.NativeLibraryPath = ""
		cfg.Rewriter.NativeLibraryRelease = "v0.0.0-startup-gate-test"
		cfg.Rewriter.NativeLibraryReleaseBaseURL = "http://127.0.0.1:1"
		return cfg
	},
}

func rewritePluginIn(t *testing.T, bs *builtServer) *rewrite.Plugin {
	t.Helper()
	for _, p := range requireExternalChain(t, bs).QueryPlugins {
		if rp, ok := p.(*rewrite.Plugin); ok {
			return rp
		}
	}
	return nil
}

func TestBuildServer_RewriterStartupFailureIsFatalByDefault(t *testing.T) {
	for name, mk := range rewriterStartupFailures {
		t.Run(name, func(t *testing.T) {
			cfg := mk(t)
			bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
			if err == nil {
				bs.teardown()
				t.Fatal("buildServer started without its rewriter; want a startup error")
			}
			if !strings.Contains(err.Error(), "rewriter.fail_open_on_unavailable") {
				t.Fatalf("err = %v, want it to name rewriter.fail_open_on_unavailable", err)
			}
		})
	}
}

func TestBuildServer_RewriterStartupFailureRunsWithoutRewriterUnderTheSwitch(t *testing.T) {
	for name, mk := range rewriterStartupFailures {
		t.Run(name, func(t *testing.T) {
			cfg := mk(t)
			cfg.Rewriter.FailOpenOnUnavailable = true
			bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
			if err != nil {
				t.Fatalf("buildServer with fail_open_on_unavailable: %v", err)
			}
			defer bs.teardown()
			if rewritePluginIn(t, bs) != nil {
				t.Fatal("no rewriter could be built, so no rewrite plugin may be wired")
			}
		})
	}
}

// TestBuildServer_RewriterStartupFailureWithStorageIntegrityIsFatal keeps the
// storage-integrity refusal: the switch cannot relax it.
func TestBuildServer_RewriterStartupFailureWithStorageIntegrityIsFatal(t *testing.T) {
	for _, switchOn := range []bool{false, true} {
		cfg := rewriterStartupFailures["grpc without service_addr"](t)
		cfg.StorageIntegrity.Tables = []string{"tenant.events"}
		cfg.Rewriter.FailOpenOnUnavailable = switchOn
		bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
		if err == nil {
			bs.teardown()
			t.Fatalf("switch=%v: buildServer started a storage-integrity server without its rewriter", switchOn)
		}
		if !strings.Contains(err.Error(), "storage_integrity.enabled requires an available SQL rewriter") {
			t.Fatalf("switch=%v: err = %v", switchOn, err)
		}
	}
}

// TestBuildServer_RouterOnlyIgnoresTheRewriterStartupGate: a router-only
// server (no shard, no upstream) never builds a rewriter by design, so an
// unbuildable rewriter config and the default switch must not stop it.
func TestBuildServer_RouterOnlyIgnoresTheRewriterStartupGate(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.Rewriter.Engine = "grpc"
	cfg.Rewriter.ServiceAddr = ""
	cfg.Rewriter.FailOpenOnUnavailable = false
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("router-only buildServer: %v", err)
	}
	defer bs.teardown()
	if rewritePluginIn(t, bs) != nil {
		t.Fatal("router-only mode must not wire the rewrite plugin")
	}
}

// TestBuildServer_InjectedRewriterBypassesTheStartupGate: a host-injected
// factory is never built here, so the gate does not apply.
func TestBuildServer_InjectedRewriterBypassesTheStartupGate(t *testing.T) {
	cfg := rewriterStartupFailures["grpc without service_addr"](t)
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: stubRewriterFactory{}}, nil)
	if err != nil {
		t.Fatalf("buildServer with an injected rewriter: %v", err)
	}
	defer bs.teardown()
	if rewritePluginIn(t, bs) == nil {
		t.Fatal("the injected rewriter must be wired")
	}
}

// fakeCluster is an injected Options.Cluster that never dials.
type fakeCluster struct{}

func (fakeCluster) GetConnection(context.Context) (*cluster.PooledConn, error) {
	return nil, errors.New("fakeCluster: not dialed in this test")
}
func (fakeCluster) HasReplica(string) bool { return false }

// TestBuildServer_InjectedClusterIsAnUpstreamForTheStartupGate reproduces
// review M2: a host that injects Options.Cluster without shard or upstream
// forwards every session to its local ClickHouse, so it is not router-only
// and an unbuildable rewriter must refuse startup.
func TestBuildServer_InjectedClusterIsAnUpstreamForTheStartupGate(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.Rewriter.Engine = "grpc"
	cfg.Rewriter.ServiceAddr = ""
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Cluster: fakeCluster{}}, nil)
	if err == nil {
		bs.teardown()
		t.Fatal("buildServer ran an injected-cluster server without its rewriter; want a startup error")
	}
	if !strings.Contains(err.Error(), "rewriter.fail_open_on_unavailable") {
		t.Fatalf("err = %v, want it to name rewriter.fail_open_on_unavailable", err)
	}

	cfg.Rewriter.FailOpenOnUnavailable = true
	bs, err = buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Cluster: fakeCluster{}}, nil)
	if err != nil {
		t.Fatalf("with the switch on: %v", err)
	}
	bs.teardown()
}

// TestBuildServer_TypedNilInjectedRewriterIsGated covers review L6: a
// typed-nil Options.Rewriter is treated as no injection, so the config's
// rewriter is built and the startup gate applies.
func TestBuildServer_TypedNilInjectedRewriterIsGated(t *testing.T) {
	cfg := rewriterStartupFailures["grpc without service_addr"](t)
	var typedNil *rewriter.SentioNetworkFactory
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: typedNil}, nil)
	if err == nil {
		bs.teardown()
		t.Fatal("a typed-nil injected rewriter ran the server without rewriting; want a startup error")
	}
	if !strings.Contains(err.Error(), "rewriter.fail_open_on_unavailable") {
		t.Fatalf("err = %v, want it to name rewriter.fail_open_on_unavailable", err)
	}
}

// warnCapture records warn-level messages.
type warnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}
	return nil
}
func (h *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCapture) WithGroup(string) slog.Handler      { return h }

func (h *warnCapture) has(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

// startStubRewriterService serves a gRPC rewriter that accepts everything
// and answers the startup table-reference probe (spec 2026-09-26 T13) as a
// conforming gRPC engine would.
type stubRewriterService struct {
	pb.UnimplementedRewriterServiceServer
}

func (stubRewriterService) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	if resp, ok := rewriter.TableReferenceProbeAnswer(req, rewriter.EngineGRPC); ok {
		return resp, nil
	}
	return &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: req.GetSql()}, nil
}

func startStubRewriterService(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterRewriterServiceServer(gs, stubRewriterService{})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

// TestBuildServer_WarnsWhenRewriterHasNoPhysicalDatabase pins re-review R3:
// with a rewriter and an empty rewriter.physical_database the engine has no
// database map, so every write, USE or EXISTS naming a logical database is
// refused; startup says so.
func TestBuildServer_WarnsWhenRewriterHasNoPhysicalDatabase(t *testing.T) {
	addr := startStubRewriterService(t)
	for _, tc := range []struct {
		physical string
		wantWarn bool
	}{{"", true}, {"phys", false}} {
		capture := &warnCapture{}
		previous := log.Default()
		log.SetDefault(log.New(capture))
		cfg := minimalServerCfg(t)
		cfg.Rewriter.Engine = "grpc"
		cfg.Rewriter.ServiceAddr = addr
		cfg.Rewriter.PhysicalDatabase = tc.physical
		bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
		log.SetDefault(previous)
		if err != nil {
			t.Fatalf("physical=%q: buildServer: %v", tc.physical, err)
		}
		bs.teardown()
		if got := capture.has("rewriter.physical_database is empty"); got != tc.wantWarn {
			t.Fatalf("physical=%q: warning = %v, want %v (warnings %v)", tc.physical, got, tc.wantWarn, capture.msgs)
		}
	}
}

// infoCapture records messages at every level.
type infoCapture struct{ warnCapture }

func (h *infoCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	return nil
}
func (h *infoCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *infoCapture) WithGroup(string) slog.Handler      { return h }

// TestBuildServer_TypedNilInjectedRewriterGetsLibraryWiring pins that a
// typed-nil Options.Rewriter, which review L6 turned into "no injection",
// also gets the wiring a library-built factory gets. The credential-provider
// and peer-signer guards used to test opts.Rewriter == nil, which a typed-nil
// interface fails, so the factory built in its place carried no peer-relay
// signer and cross-indexer remote() clauses went out without their JWS.
func TestBuildServer_TypedNilInjectedRewriterGetsLibraryWiring(t *testing.T) {
	addr := startStubRewriterService(t)
	var typedNil *rewriter.SentioNetworkFactory
	for _, tc := range []struct {
		name      string
		injected  rewriter.Factory
		wantWired bool
	}{
		{"no injection", nil, true},
		{"typed-nil injection", typedNil, true},
		{"real injection", stubRewriterFactory{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &infoCapture{}
			previous := log.Default()
			log.SetDefault(log.New(capture))
			cfg := minimalServerCfg(t)
			cfg.Rewriter.Engine = "grpc"
			cfg.Rewriter.ServiceAddr = addr
			cfg.Rewriter.PhysicalDatabase = "phys"
			cfg.RelayPrivateKeyHex = testRelayKeyHex
			bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: tc.injected}, nil)
			log.SetDefault(previous)
			if err != nil {
				t.Fatalf("buildServer: %v", err)
			}
			bs.teardown()
			if got := capture.has("rewriter peer-relay signer wired"); got != tc.wantWired {
				t.Fatalf("peer-relay signer wired = %v, want %v", got, tc.wantWired)
			}
		})
	}
}
