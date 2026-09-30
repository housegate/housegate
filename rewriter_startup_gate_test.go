package housegate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
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
