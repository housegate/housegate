package housegate

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/ffifetch"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/rewriter"
)

// rewriter.engine "auto" at the build layer (issue #216): the selected
// engine passes the startup probes and is reported; a fallback whose pinned
// library cannot be fetched refuses startup.

func autoEngineCfg(t *testing.T, serviceAddr string) *config.Config {
	t.Helper()
	cfg := minimalServerCfg(t)
	cfg.Rewriter.Engine = rewriter.EngineAuto
	cfg.Rewriter.ServiceAddr = serviceAddr
	cfg.Rewriter.PhysicalDatabase = "phys"
	cfg.Rewriter.NativeLibraryRelease = ffifetch.DefaultRelease
	cfg.Rewriter.NativeLibrarySHA256 = strings.Repeat("0", 64)
	cfg.Rewriter.Auto.GRPCWait = config.Duration{Duration: 300 * time.Millisecond}
	return cfg
}

func TestBuildServer_AutoEngineSelectsReachableGRPCAndReportsIt(t *testing.T) {
	cfg := autoEngineCfg(t, startStubRewriterService(t))
	// An unreachable mirror: touching the native fallback would fail startup.
	cfg.Rewriter.NativeLibraryReleaseBaseURL = "http://127.0.0.1:1"
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(bs.teardown)
	want := `
# HELP clickhouse_proxy_rewriter_engine SQL rewriter engine selected at startup (value 1); configured is rewriter.engine, engine the backend serving.
# TYPE clickhouse_proxy_rewriter_engine gauge
clickhouse_proxy_rewriter_engine{configured="auto",engine="grpc",version="unknown"} 1
`
	if err := testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(want), "clickhouse_proxy_rewriter_engine"); err != nil {
		t.Fatalf("active engine series: %v", err)
	}
}

func TestBuildServer_AutoEngineFallbackFetchFailureRefusesStartup(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	cfg := autoEngineCfg(t, addr)
	cfg.Rewriter.NativeLibraryReleaseBaseURL = "http://127.0.0.1:1"
	t.Setenv("XDG_CACHE_HOME", t.TempDir()) // no cached library on linux
	t.Setenv("HOME", t.TempDir())           // nor on darwin
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err == nil {
		bs.teardown()
		t.Fatal("started with the gRPC service unreachable and the pinned native library unfetchable")
	}
	if !strings.Contains(err.Error(), "SQL rewriter unavailable at startup") || !strings.Contains(err.Error(), "native fallback") {
		t.Fatalf("err = %v, want the startup refusal naming the native fallback", err)
	}
}

func TestBuildRewriterFactory_AutoEngineGuardsHostsThatSkipValidate(t *testing.T) {
	for name, mutate := range map[string]func(*config.Config, *rewriter.StorageIntegrityOptions){
		"storage integrity": func(_ *config.Config, si *rewriter.StorageIntegrityOptions) { si.Enabled = true },
		"native library path": func(c *config.Config, _ *rewriter.StorageIntegrityOptions) {
			c.Rewriter.NativeLibraryPath = "/opt/lib.so"
		},
		"unpinned sha256": func(c *config.Config, _ *rewriter.StorageIntegrityOptions) { c.Rewriter.NativeLibrarySHA256 = "" },
		"other release": func(c *config.Config, _ *rewriter.StorageIntegrityOptions) {
			c.Rewriter.NativeLibraryRelease = "v0.0.1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := autoEngineCfg(t, "127.0.0.1:1")
			var si rewriter.StorageIntegrityOptions
			mutate(cfg, &si)
			if f, err := buildRewriterFactory(cfg, nil, si, false); err == nil || !strings.Contains(err.Error(), `rewriter.engine "auto"`) {
				t.Fatalf("buildRewriterFactory = %v, %v; want the auto guard", f, err)
			}
		})
	}
}
