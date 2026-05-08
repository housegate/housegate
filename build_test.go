package housegate

import (
	"context"
	"testing"

	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/plugins/forward"
	"housegate/housegate/pkg/plugins/rewrite"
	"housegate/housegate/pkg/rewriter"
)

// stubRewriterFactory is a no-op rewriter.Factory that never dials any
// external service. Injected via Options.Rewriter to force rewritePlug
// into the chain without needing a running gRPC server.
type stubRewriterFactory struct{}

func (stubRewriterFactory) NewRewriter(_ rewriter.Session) rewriter.Rewriter {
	return stubRewriter{}
}
func (stubRewriterFactory) Close() error { return nil }

type stubRewriter struct{}

func (stubRewriter) Rewrite(_ context.Context, sql, _ string) (rewriter.RewriteResult, error) {
	return rewriter.RewriteResult{SQL: sql}, nil
}
func (stubRewriter) RewriteErrorMessage(_ context.Context, msg string) (string, error) {
	return msg, nil
}
func (stubRewriter) Close() error { return nil }

// minimalServerCfg returns a server-mode Config sufficient for buildServer
// unit tests. It deliberately omits fields that Config.Validate requires
// (RedisDefaultAddr, CkhManagerConfigPath) and does not call Validate.
// Tests that exercise validation should use pkg/config's helper instead.
func minimalServerCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Upstream = "127.0.0.1:1" // not dialed; satisfies Mode() == ModeServer
	return &cfg
}

// minimalRouterOnlyCfg returns a server-mode Config with neither shard
// nor upstream — the router-only deployment that Phase 5 collapses
// forwarding-only into.
func minimalRouterOnlyCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	return &cfg
}

func TestBuildServer_TwoListenersWhenInternalListenSet(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.Listen = "127.0.0.1:0"
	cfg.InternalListen = "127.0.0.1:0"

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	if got, want := len(bs.listeners), 2; got != want {
		t.Fatalf("listeners: got %d want %d", got, want)
	}

	var external, internal *serverListener
	for i := range bs.listeners {
		switch bs.listeners[i].Label {
		case "external":
			external = &bs.listeners[i]
		case "internal":
			internal = &bs.listeners[i]
		}
	}
	if external == nil || internal == nil {
		t.Fatalf("missing labelled listener: ext=%v int=%v", external, internal)
	}

	if external.Server.PreflagSession != nil {
		t.Errorf("external listener must not preflag")
	}
	if internal.Server.PreflagSession == nil {
		t.Fatalf("internal listener must preflag IsPeerTrusted+IsInternalPort")
	}
	var st chsession.SessionState
	internal.Server.PreflagSession(&st)
	if !st.IsPeerTrusted || !st.IsInternalPort {
		t.Errorf("internal preflag missed flags: peer=%v internal=%v",
			st.IsPeerTrusted, st.IsInternalPort)
	}
}

func TestBuildServer_OneListenerWhenInternalListenEmpty(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.Listen = "127.0.0.1:0"
	cfg.InternalListen = ""

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	if got, want := len(bs.listeners), 1; got != want {
		t.Fatalf("listeners: got %d want %d", got, want)
	}
	if bs.listeners[0].Label != "external" {
		t.Errorf("single listener should be labeled external; got %q", bs.listeners[0].Label)
	}
}

// TestBuildServer_RouterOnly_NoShardNoUpstream proves Phase 5.1 + 5.2:
// a server config with neither Shard nor Upstream still produces a
// usable *builtServer. The rewriter is intentionally NOT wired (no
// CkhManagerConfigPath, no Upstream, no Shard) and the dialer falls
// through to the NetworkState peer-pick path the moment a session
// without a route target hits it.
func TestBuildServer_RouterOnly_NoShardNoUpstream(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer router-only: %v", err)
	}
	defer bs.teardown()

	if got := len(bs.listeners); got != 1 {
		t.Fatalf("listeners: got %d want 1", got)
	}
	if bs.listeners[0].Label != "external" {
		t.Errorf("router-only listener label: got %q want external", bs.listeners[0].Label)
	}

	// The chain must NOT contain a rewrite plugin — router-only sessions
	// never see SQL the proxy rewrites locally.
	chain, ok := bs.listeners[0].Server.Hooks.(*plugin.PluginChain)
	if !ok {
		t.Fatalf("Server.Hooks: %T", bs.listeners[0].Server.Hooks)
	}
	for _, p := range chain.HelloPlugins {
		if _, ok := p.(*rewrite.Plugin); ok {
			t.Error("router-only mode must not wire rewrite.Plugin (no rewriter factory)")
		}
	}
}

func TestBuildServer_ForwardPluginInsertedBeforeRewrite(t *testing.T) {
	cfg := minimalServerCfg(t)
	// Inject a stub rewriter factory so rewritePlug is unconditionally wired
	// into HelloPlugins. Without this, rwIdx stays -1 and the ordering
	// assertion is never exercised.
	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
		Rewriter:     stubRewriterFactory{},
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	var external *serverListener
	for i := range bs.listeners {
		if bs.listeners[i].Label == "external" {
			external = &bs.listeners[i]
		}
	}
	if external == nil {
		t.Fatal("external listener missing")
	}

	chain, ok := external.Server.Hooks.(*plugin.PluginChain)
	if !ok {
		t.Fatalf("Server.Hooks is not *plugin.PluginChain: %T", external.Server.Hooks)
	}

	var fwdIdx, rwIdx int = -1, -1
	for i, p := range chain.HelloPlugins {
		switch p.(type) {
		case *forward.Plugin:
			fwdIdx = i
		case *rewrite.Plugin:
			rwIdx = i
		}
	}
	if fwdIdx < 0 {
		t.Fatal("forward plugin not in HelloPlugins chain")
	}
	if rwIdx < 0 {
		t.Fatal("rewrite plugin not in HelloPlugins chain (stub rewriter was injected — this is a wiring bug)")
	}
	if fwdIdx >= rwIdx {
		t.Fatalf("forward plugin (idx=%d) must run before rewrite (idx=%d)", fwdIdx, rwIdx)
	}

	// Also verify QueryPlugins ordering: forward must precede rewrite.
	var qFwdIdx, qRwIdx int = -1, -1
	for i, p := range chain.QueryPlugins {
		switch p.(type) {
		case *forward.Plugin:
			qFwdIdx = i
		case *rewrite.Plugin:
			qRwIdx = i
		}
	}
	if qFwdIdx < 0 {
		t.Fatal("forward plugin not in QueryPlugins chain")
	}
	if qRwIdx < 0 {
		t.Fatal("rewrite plugin not in QueryPlugins chain (stub rewriter was injected — this is a wiring bug)")
	}
	if qFwdIdx >= qRwIdx {
		t.Fatalf("forward (qIdx=%d) must run before rewrite (qIdx=%d) in QueryPlugins", qFwdIdx, qRwIdx)
	}
}
