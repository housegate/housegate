package housegate

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/forward"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/proxy"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sqlmeta"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
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
	if !qctx.SuppressUpstreamExecution {
		t.Fatal("storage-integrity ingress must suppress ordinary upstream payload rows when admission consumer is wired")
	}
	limit, enforce := chain.ClientDataReadLimit(qctx)
	if !enforce || limit != cfg.StorageIntegrity.Ingress.MaxPayloadBytes {
		t.Fatalf("ClientDataReadLimit = %d/%v, want %d/true", limit, enforce, cfg.StorageIntegrity.Ingress.MaxPayloadBytes)
	}
	if err := chain.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	// The consumer is driven at the strict end-of-input boundary. Because
	// OnQuery set SuppressUpstreamExecution, success lets Relay forward only
	// the terminating empty block, not the captured payload rows.
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
	next.Query.ID = buildTestStatementID(signer, 2)
	if err := chain.OnQuery(context.Background(), next); err != nil {
		t.Fatalf("second storage write blocked after consumer consumed admission: %v", err)
	}
}

func TestBuildServer_StorageIntegrityIngressWiresCSVPayloadMaterializer(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 50 * time.Millisecond
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 64
	consumer := &recordingAdmissionConsumer{}
	materializer := &recordingBuildPayloadMaterializer{
		out: sicore.PayloadMaterializationResult{
			Payload:  []byte("id,region\n1,eu\n"),
			Encoding: sicore.EncodingCSVWithNames,
		},
	}

	bs, err := buildServer(Options{
		Config:                              cfg,
		NetworkState:                        network.NewInMemoryNetworkState(),
		StorageIntegrityAdmissionConsumer:   consumer,
		StorageIntegrityPayloadMaterializer: materializer,
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	chain := requireExternalChain(t, bs)
	sql := "INSERT INTO tenant.events FORMAT CSVWithNames"
	qctx := signedStorageIntegrityQuerySQL(t, signer, sql)
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := chain.OnClientDataStrict(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := chain.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}

	if materializer.input.PayloadEncoding != sicore.EncodingCSVWithNames || materializer.input.TableID != "tenant.events" {
		t.Fatalf("materializer input encoding/table = %q/%q", materializer.input.PayloadEncoding, materializer.input.TableID)
	}
	admission := consumer.requireOne(t)
	if admission.Payload.Encoding != sicore.EncodingCSVWithNames || string(admission.Payload.Bytes) != string(materializer.out.Payload) {
		t.Fatalf("admission payload = %q/%q, want materialized CSV %q", admission.Payload.Encoding, admission.Payload.Bytes, materializer.out.Payload)
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

func TestBuildStorageIntegrityRuntimeRequiresPorts(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	_, _, err = buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, StorageIntegrityRuntimeOptions{})
	if err == nil {
		t.Fatal("runtime assembly succeeded without storage-integrity runtime ports")
	}
	for _, want := range []string{
		"storage_integrity.runtime.statement_submitter is required",
		"storage_integrity.runtime.source_preparer is required",
		"storage_integrity.runtime.status_querier is required",
		"storage_integrity.runtime.payload_writer is required",
		"storage_integrity.runtime.merge_guard or merge_conn is required",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("runtime assembly err = %v, missing %q", err, want)
		}
	}
}

func TestBuildStorageIntegrityRuntimeRequiresPreparedLookup(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	_, _, err = buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{},
			SourcePreparer:     &rootPreparerWithoutLookup{},
			StatusQuerier:      rootRecordingStatusQuerier{},
			PayloadWriter:      &rootRecordingPayloadWriter{},
			MergeGuard:         &recordingBuildMergeGuard{},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "source_preparer must implement prepared statement lookup") {
		t.Fatalf("runtime assembly err = %v, want missing prepared lookup rejection", err)
	}
}

func TestBuildStorageIntegrityRuntimeRequiresStatusQuerier(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	_, _, err = buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{},
			SourcePreparer:     &rootRecordingPreparer{},
			PayloadWriter:      &rootRecordingPayloadWriter{},
			MergeGuard:         &recordingBuildMergeGuard{},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.status_querier is required") {
		t.Fatalf("runtime assembly err = %v, want missing status querier rejection", err)
	}
}

func TestBuildServer_StorageIntegrityRuntimeAutoWiresArbiterStatusQuerier(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	bs, err := buildServer(Options{
		Config:                              cfg,
		NetworkState:                        network.NewInMemoryNetworkState(),
		StorageIntegrityPayloadMaterializer: &recordingBuildPayloadMaterializer{},
		StorageIntegrityRuntime: StorageIntegrityRuntimeOptions{
			ArbiterIngressClient: &rootArbiterIngressClient{},
			SourcePreparer:       &rootRecordingPreparer{},
			PayloadWriter:        &rootRecordingPayloadWriter{},
			MergeGuard:           &recordingBuildMergeGuard{},
		},
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()
}

func TestBuildServer_StorageIntegrityRuntimeRequiresCSVPayloadMaterializer(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	_, err = buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
		StorageIntegrityRuntime: StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{},
			SourcePreparer:     &rootRecordingPreparer{},
			StatusQuerier:      rootRecordingStatusQuerier{},
			PayloadWriter:      &rootRecordingPayloadWriter{},
			MergeGuard:         &recordingBuildMergeGuard{},
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "StorageIntegrityPayloadMaterializer is required") {
		t.Fatalf("buildServer err = %v, want missing CSV payload materializer rejection", err)
	}
}

func TestBuildServer_StorageIntegrityRuntimeRejectsManualConsumer(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	_, err = buildServer(Options{
		Config:                            cfg,
		NetworkState:                      network.NewInMemoryNetworkState(),
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
		StorageIntegrityRuntime: StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{},
			SourcePreparer:     &rootRecordingPreparer{},
			PayloadWriter:      &rootRecordingPayloadWriter{},
			MergeGuard:         &recordingBuildMergeGuard{},
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.enabled cannot be combined with StorageIntegrityAdmissionConsumer") {
		t.Fatalf("buildServer err = %v, want manual-consumer rejection", err)
	}
}

func TestBuildStorageIntegrityRuntimeBuildsConsumerAndRunsMergeGuard(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)
	cfg.StorageIntegrity.Runtime.ExpectedSource = "  snode-A  "

	guard := &recordingBuildMergeGuard{}
	writer := &rootRecordingPayloadWriter{
		result: sicore.PayloadPutResult{
			PayloadRef:         "payload://store/ref-1",
			State:              sicore.PayloadStateAvailable,
			LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	}
	submitter := &rootRecordingSubmitter{
		outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted},
	}
	preparer := &rootRecordingPreparer{
		source: "snode-A",
		claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
	}

	ingress, mergeGuard, err := buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: submitter,
			SourcePreparer:     preparer,
			StatusQuerier:      rootRecordingStatusQuerier{},
			PayloadWriter:      writer,
			MergeGuard:         guard,
		},
	)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	if ingress.matKind != sicore.MaterializerCSV {
		t.Fatalf("runtime materializer = %v, want CSV", ingress.matKind)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer ingress.Close()
	if err := startStorageIntegrityRuntime(ctx, ingress, mergeGuard); err != nil {
		t.Fatalf("startStorageIntegrityRuntime: %v", err)
	}
	if guard.calls != 1 {
		t.Fatalf("merge guard calls = %d, want 1", guard.calls)
	}
	payload := []byte("id,region\n1,eu\n")
	sql := "INSERT INTO events FORMAT CSVWithNames"
	if err := ingress.ConsumeStorageIntegrityAdmission(ctx, storageintegrity.Admission{
		StatementID: strings.ToLower(signer.Address()) + ":1:n1",
		Kind:        storageintegrity.KindInsert,
		TableID:     "net1.events",
		SQL:         sql,
		SQLHash:     replay.DigestString(sql),
		Signer:      strings.ToLower(signer.Address()),
		UserJWS:     "jws",
		Payload: storageintegrity.CapturedPayload{
			Bytes:    payload,
			Length:   uint64(len(payload)),
			Encoding: sicore.EncodingCSVWithNames,
			Revision: 54465,
			Complete: true,
		},
	}); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}

	if writer.calls != 1 {
		t.Fatalf("payload writer calls = %d, want 1", writer.calls)
	}
	if submitter.calls != 1 {
		t.Fatalf("submitter calls = %d, want 1", submitter.calls)
	}
	if preparer.prepareCalls != 1 {
		t.Fatalf("preparer prepare calls = %d, want 1", preparer.prepareCalls)
	}
	if submitter.env.PayloadRef != writer.result.PayloadRef || preparer.env.PayloadRef != writer.result.PayloadRef {
		t.Fatalf("payload_ref submit/prepare = %q/%q, want %q", submitter.env.PayloadRef, preparer.env.PayloadRef, writer.result.PayloadRef)
	}
	requireDirHasFiles(t, cfg.StorageIntegrity.Runtime.PayloadSpoolDir, "payload spool", 2)
	requireDirHasFiles(t, cfg.StorageIntegrity.Runtime.JournalDir, "intake journal", 1)
}

func TestBuildStorageIntegrityRuntimeBuildsMergeGuardFromConnAndConfig(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	mergeConn := &recordingBuildMergeConn{}
	ingress, guard, err := buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
			SourcePreparer:     &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
			StatusQuerier:      rootRecordingStatusQuerier{},
			PayloadWriter:      &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}},
			MergeConn:          mergeConn,
		},
	)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer ingress.Close()
	if err := startStorageIntegrityRuntime(ctx, ingress, guard); err != nil {
		t.Fatalf("startStorageIntegrityRuntime: %v", err)
	}
	wantExecs := []string{
		"SYSTEM STOP MERGES `hg_safe`.`events`",
		"SYSTEM STOP MERGES `hg_unsafe`.`events`",
	}
	if strings.Join(mergeConn.execs, "\n") != strings.Join(wantExecs, "\n") {
		t.Fatalf("STOP MERGES execs = %v, want %v", mergeConn.execs, wantExecs)
	}
	if !mergeConn.queryRan {
		t.Fatal("merge guard did not run verify query")
	}
	if mergeConn.queriedAt != len(wantExecs) {
		t.Fatalf("verify query ran after %d execs, want %d", mergeConn.queriedAt, len(wantExecs))
	}
}

func TestBuildStorageIntegrityRuntimeWrapsMergeSupervisor(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)
	rawGuard := &recordingBuildMergeGuard{}

	ingress, guard, err := buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
			SourcePreparer: &rootRecordingPreparer{
				source: "snode-A",
				claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			},
			StatusQuerier: rootRecordingStatusQuerier{},
			PayloadWriter: &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{
				PayloadRef:         "payload://store/ref-1",
				State:              sicore.PayloadStateAvailable,
				LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
			}},
			MergeGuard: rawGuard,
		},
	)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	supervisor, ok := guard.(*StorageIntegrityMergeSupervisor)
	if !ok {
		t.Fatalf("runtime merge guard type = %T, want *StorageIntegrityMergeSupervisor", guard)
	}
	if ingress.guard != supervisor {
		t.Fatal("ingress and preServe must share the same merge supervisor")
	}
}

func TestStartStorageIntegrityRuntimeFailsClosedOnMergeGuardError(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	guard := &recordingBuildMergeGuard{err: errors.New("native merge still active")}
	ingress, mergeGuard, err := buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
			SourcePreparer:     &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
			StatusQuerier:      rootRecordingStatusQuerier{},
			PayloadWriter:      &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}},
			MergeGuard:         guard,
		},
	)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	defer ingress.Close()

	err = startStorageIntegrityRuntime(context.Background(), ingress, mergeGuard)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.merge_guard") {
		t.Fatalf("startStorageIntegrityRuntime err = %v, want merge guard failure", err)
	}
}

type preServeOrderRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *preServeOrderRecorder) add(event string) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *preServeOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type orderedBuildMergeGuard struct {
	order *preServeOrderRecorder
}

func (g *orderedBuildMergeGuard) AssertStopMerges(context.Context) error {
	g.order.add("merge")
	return nil
}

type orderedBuildSubmitter struct {
	order *preServeOrderRecorder
}

func (s *orderedBuildSubmitter) SubmitStatement(context.Context, sicore.StatementEnvelope) (sicore.SubmitOutcome, error) {
	s.order.add("recover")
	return sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}, nil
}

func TestStartStorageIntegrityRuntimeRecoversAfterInitialMergeAssert(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)

	journal, err := sicore.NewFileIntakeJournal(cfg.StorageIntegrity.Runtime.JournalDir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	payload := []byte("id,region\n1,eu\n")
	sql := "INSERT INTO events FORMAT CSVWithNames"
	adm := sicore.AdmissionRecord{
		StatementID:     "0xabc:1:n1",
		Kind:            sicore.KindInsert,
		TableID:         "net1.events",
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		Signer:          "0xabc",
		UserJWS:         "jws",
		Payload:         payload,
		PayloadLength:   uint64(len(payload)),
		PayloadHash:     replay.DigestBytes(payload),
		PayloadEncoding: sicore.EncodingCSVWithNames,
		Revision:        54465,
	}
	env, err := sicore.EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := sicore.PreparedLocalResult{
		StatementID:     adm.StatementID,
		SourceNode:      "snode-A",
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding,
		Revision:        env.Revision,
		Lifecycle:       sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(context.Background(), sicore.IntakeJournalRecord{
		StatementID:     adm.StatementID,
		Source:          "snode-A",
		FrontierOrdinal: 1,
		Env:             env,
		Admission:       adm,
		Stage:           sicore.LifecycleUnsafeWritten,
		Prepared:        prepared,
		HasPrepared:     true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}

	order := &preServeOrderRecorder{}
	payloadWriter := &rootRecordingPayloadWriter{
		result: sicore.PayloadPutResult{
			PayloadRef:         env.PayloadRef,
			State:              sicore.PayloadStateAvailable,
			LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	}
	ingress, mergeGuard, err := buildStorageIntegrityRuntimeConsumer(
		cfg.StorageIntegrity.Runtime,
		StorageIntegrityRuntimeOptions{
			StatementSubmitter: &orderedBuildSubmitter{order: order},
			SourcePreparer: &rootRecordingPreparer{
				source: "snode-A",
				claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			},
			StatusQuerier: rootRecordingStatusQuerier{},
			PayloadWriter: payloadWriter,
			MergeGuard:    &orderedBuildMergeGuard{order: order},
		},
	)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer ingress.Close()
	if err := startStorageIntegrityRuntime(ctx, ingress, mergeGuard); err != nil {
		t.Fatalf("startStorageIntegrityRuntime: %v", err)
	}
	events := order.snapshot()
	if len(events) < 2 || events[0] != "merge" || events[1] != "recover" {
		t.Fatalf("preServe events = %v, want [merge recover]", events)
	}
	if payloadWriter.calls != 1 {
		t.Fatalf("recovery payload lease puts = %d, want 1", payloadWriter.calls)
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

func requireDirHasFiles(t *testing.T, dir, label string, minFiles int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s dir %s: %v", label, dir, err)
	}
	if len(entries) < minFiles {
		t.Fatalf("%s dir %s has %d files, want at least %d", label, dir, len(entries), minFiles)
	}
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
	return signedStorageIntegrityQuerySQL(t, signer, "INSERT INTO tenant.events FORMAT Native")
}

func signedStorageIntegrityQuerySQL(t *testing.T, signer *auth.RelaySigner, sql string) *plugin.QueryContext {
	t.Helper()
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
			ID:   buildTestStatementID(signer, 1),
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

func buildTestStatementID(signer *auth.RelaySigner, seq uint64) string {
	return strings.ToLower(signer.Address()) + ":" + strconv.FormatUint(seq, 10) + ":n" + strconv.FormatUint(seq, 10)
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

type recordingBuildPayloadMaterializer struct {
	input sicore.PayloadMaterializationInput
	out   sicore.PayloadMaterializationResult
}

type rootArbiterIngressClient struct{}

func (c *rootArbiterIngressClient) SubmitStatement(context.Context, *pb.StatementEnvelopeV2, ...grpc.CallOption) (*pb.SequencedAck, error) {
	return &pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: 1}, nil
}

func (c *rootArbiterIngressClient) GetStatementStatus(context.Context, *pb.GetStatementStatusRequest, ...grpc.CallOption) (*pb.StatementStatus, error) {
	return &pb.StatementStatus{Found: false}, nil
}

func (m *recordingBuildPayloadMaterializer) MaterializePayload(_ context.Context, input sicore.PayloadMaterializationInput) (sicore.PayloadMaterializationResult, error) {
	m.input = input
	return m.out, nil
}

func enableStorageIntegrityRuntimeTestConfig(t *testing.T, cfg *config.Config, signer *auth.RelaySigner) {
	t.Helper()
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 50 * time.Millisecond
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 64
	cfg.StorageIntegrity.Runtime.Enabled = true
	cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
	runtimeDir := t.TempDir()
	cfg.StorageIntegrity.Runtime.JournalDir = filepath.Join(runtimeDir, "journal")
	cfg.StorageIntegrity.Runtime.PayloadSpoolDir = filepath.Join(runtimeDir, "payload-spool")
	cfg.StorageIntegrity.Runtime.MergeGuard.Tables = []config.StorageIntegrityRuntimeMergeTableConfig{
		{Database: "hg_safe", Table: "events"},
		{Database: "hg_unsafe", Table: "events"},
	}
}

type recordingBuildMergeGuard struct {
	calls int
	err   error
}

func (g *recordingBuildMergeGuard) AssertStopMerges(context.Context) error {
	g.calls++
	return g.err
}

type recordingBuildMergeConn struct {
	execs     []string
	queryRan  bool
	queriedAt int
}

func (c *recordingBuildMergeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *recordingBuildMergeConn) Query(_ context.Context, _ string, _ ...any) (sicore.MergeRows, error) {
	c.queryRan = true
	c.queriedAt = len(c.execs)
	return recordingBuildMergeRows{}, nil
}

type recordingBuildMergeRows struct{}

func (recordingBuildMergeRows) Next() bool        { return false }
func (recordingBuildMergeRows) Scan(...any) error { return nil }
func (recordingBuildMergeRows) Err() error        { return nil }
func (recordingBuildMergeRows) Close() error      { return nil }
