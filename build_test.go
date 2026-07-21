package housegate

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/plugins/forward"
	"housegate/housegate/pkg/plugins/rewrite"
	"housegate/housegate/pkg/plugins/storageintegrity"
	"housegate/housegate/pkg/proxy"
	"housegate/housegate/pkg/rewriter"
	"housegate/housegate/pkg/sqlmeta"
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

func requireProxyServer(t *testing.T, listener serverListener) *proxy.Server {
	t.Helper()
	runner, ok := listener.Runner.(*proxyServerRunner)
	if !ok {
		t.Fatalf("listener %q runner type = %T, want *proxyServerRunner", listener.Label, listener.Runner)
	}
	return runner.server
}

func requireExternalChain(t *testing.T, bs *builtServer) *plugin.PluginChain {
	t.Helper()
	for i := range bs.listeners {
		if bs.listeners[i].Label != "external" {
			continue
		}
		server := requireProxyServer(t, bs.listeners[i])
		chain, ok := server.Hooks.(*plugin.PluginChain)
		if !ok {
			t.Fatalf("Server.Hooks is not *plugin.PluginChain: %T", server.Hooks)
		}
		return chain
	}
	t.Fatal("external listener missing")
	return nil
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

	externalServer := requireProxyServer(t, *external)
	internalServer := requireProxyServer(t, *internal)
	if externalServer.PreflagSession != nil {
		t.Errorf("external listener must not preflag")
	}
	if internalServer.PreflagSession == nil {
		t.Fatalf("internal listener must preflag IsPeerTrusted+IsInternalPort")
	}
	var st chsession.SessionState
	internalServer.PreflagSession(&st)
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
	server := requireProxyServer(t, bs.listeners[0])
	chain, ok := server.Hooks.(*plugin.PluginChain)
	if !ok {
		t.Fatalf("Server.Hooks: %T", server.Hooks)
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

	server := requireProxyServer(t, *external)
	chain, ok := server.Hooks.(*plugin.PluginChain)
	if !ok {
		t.Fatalf("Server.Hooks is not *plugin.PluginChain: %T", server.Hooks)
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

func TestBuildServer_StorageIntegrityIngressEnabledWiresRuntimeHooks(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 50 * time.Millisecond
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 7
	consumer := &recordingAdmissionConsumer{}

	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      network.NewInMemoryNetworkState(),
		StorageIntegrityAdmissionConsumer: consumer,
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	chain := requireExternalChain(t, bs)
	ingress := requireStorageIntegrityQueryPlugin(t, chain)
	requireStorageIntegrityStrictDataPlugin(t, chain, ingress)
	requireStorageIntegrityInputCompleteStrictPlugin(t, chain, ingress)
	requireStorageIntegrityInputCompletePlugin(t, chain, ingress)
	requireStorageIntegrityAbortPlugin(t, chain, ingress)
	requireStorageIntegrityClosePlugin(t, chain, ingress)

	qctx := signedStorageIntegrityQuery(t, signer)
	if !chain.RejectUndecodableQuery(qctx.Session) {
		t.Fatal("enabled storage-integrity ingress did not require strict query decoding")
	}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	limit, enforce := chain.ClientDataReadLimit(qctx)
	if !enforce || limit != cfg.StorageIntegrity.Ingress.MaxPayloadBytes {
		t.Fatalf("ClientDataReadLimit = %d/%v, want %d/true", limit, enforce, cfg.StorageIntegrity.Ingress.MaxPayloadBytes)
	}
	if err := chain.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	// The consumer is driven at the pre-splice strict end-of-input boundary; a
	// success returns nil so the terminating block may reach the upstream.
	if err := chain.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	chain.OnQueryInputComplete(context.Background(), qctx)
	admission := consumer.requireOne(t)
	if admission.TableID != "tenant.events" || admission.Signer != signer.Address() {
		t.Fatalf("admission table/signer = %s/%s, want tenant.events/%s", admission.TableID, admission.Signer, signer.Address())
	}

	next := signedStorageIntegrityQuery(t, signer)
	next.Session = qctx.Session
	next.Query.ID = "storage-integrity-build-test-2"
	if err := chain.OnQuery(context.Background(), next); err != nil {
		t.Fatalf("second storage write blocked after consumer consumed admission: %v", err)
	}
}

func TestBuildServer_StorageIntegrityIngressRequiresAdmissionConsumer(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}

	_, err = buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress admission consumer is required") {
		t.Fatalf("buildServer err = %v, want missing admission consumer rejection", err)
	}
}

func TestBuildServer_StorageIntegrityMutationDisabledConstructsNoRuntime(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	// Mutation defaults off; the server builds and the chain has no mutation
	// runtime (there is no mutation plugin to find; the build simply succeeds and
	// is byte-identical to a build with no mutation config).
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer with mutation off: %v", err)
	}
	defer bs.teardown()
}

func TestBuildServer_StorageIntegrityMutationEnabledRejected(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Mutation.Enabled = true
	_, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err == nil || !strings.Contains(err.Error(), "mutation is not runnable in v1") {
		t.Fatalf("buildServer err = %v, want mutation v1 rejection", err)
	}
}

func requireStorageIntegrityQueryPlugin(t *testing.T, chain *plugin.PluginChain) *storageintegrity.Plugin {
	t.Helper()
	var found *storageintegrity.Plugin
	for _, p := range chain.QueryPlugins {
		ingress, ok := p.(*storageintegrity.Plugin)
		if !ok {
			continue
		}
		if found != nil {
			t.Fatal("storage-integrity ingress wired multiple times in QueryPlugins")
		}
		found = ingress
	}
	if found == nil {
		t.Fatal("storage-integrity ingress missing from QueryPlugins")
	}
	return found
}

func requireStorageIntegrityStrictDataPlugin(t *testing.T, chain *plugin.PluginChain, want *storageintegrity.Plugin) {
	t.Helper()
	for _, p := range chain.StrictDataPlugins {
		if p == want {
			return
		}
	}
	t.Fatal("storage-integrity ingress missing from StrictDataPlugins")
}

func requireStorageIntegrityInputCompletePlugin(t *testing.T, chain *plugin.PluginChain, want *storageintegrity.Plugin) {
	t.Helper()
	for _, p := range chain.QueryInputCompletePlugins {
		if p == want {
			return
		}
	}
	t.Fatal("storage-integrity ingress missing from QueryInputCompletePlugins")
}

func requireStorageIntegrityInputCompleteStrictPlugin(t *testing.T, chain *plugin.PluginChain, want *storageintegrity.Plugin) {
	t.Helper()
	for _, p := range chain.QueryInputCompleteStrictPlugins {
		if p == want {
			return
		}
	}
	t.Fatal("storage-integrity ingress missing from QueryInputCompleteStrictPlugins")
}

func requireStorageIntegrityAbortPlugin(t *testing.T, chain *plugin.PluginChain, want *storageintegrity.Plugin) {
	t.Helper()
	for _, p := range chain.QueryAbortPlugins {
		if p == want {
			return
		}
	}
	t.Fatal("storage-integrity ingress missing from QueryAbortPlugins")
}

func requireStorageIntegrityClosePlugin(t *testing.T, chain *plugin.PluginChain, want *storageintegrity.Plugin) {
	t.Helper()
	for _, p := range chain.ClosePlugins {
		if p == want {
			return
		}
	}
	t.Fatal("storage-integrity ingress missing from ClosePlugins")
}

func signedStorageIntegrityQuery(t *testing.T, signer *auth.RelaySigner) *plugin.QueryContext {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	token, err := signer.SignToken(sql)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	state := chsession.NewSessionState()
	state.ClientRevision = 54453
	return &plugin.QueryContext{
		Session: &buildTestSession{
			id:    1001,
			state: state,
		},
		OriginalSQL: sql,
		Query: &chproto.Query{
			ID:   "storage-integrity-build-test",
			Body: sql,
			Settings: []chproto.Setting{{
				Key:    auth.AuthTokenSettingKey,
				Value:  "'" + token + "'",
				Custom: true,
			}},
		},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant",
			OriginalTable:    "events",
			LogicalDatabase:  "tenant",
		}},
	}
}

type buildTestSession struct {
	id    int64
	state *chsession.SessionState
}

func (s *buildTestSession) ID() int64                                          { return s.id }
func (s *buildTestSession) State() *chsession.SessionState                     { return s.state }
func (s *buildTestSession) Client() *chproto.Codec                             { return nil }
func (s *buildTestSession) Upstream() *chproto.Codec                           { return nil }
func (s *buildTestSession) RemoteAddr() net.Addr                               { return nil }
func (s *buildTestSession) Close() error                                       { return nil }
func (s *buildTestSession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *buildTestSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *buildTestSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *buildTestSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

type recordingAdmissionConsumer struct {
	mu        sync.Mutex
	admission []storageintegrity.Admission
}

func (c *recordingAdmissionConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, admission storageintegrity.Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.admission = append(c.admission, admission)
	return nil
}

func (c *recordingAdmissionConsumer) requireOne(t *testing.T) storageintegrity.Admission {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.admission) != 1 {
		t.Fatalf("consumed admissions = %d, want 1", len(c.admission))
	}
	return c.admission[0]
}
