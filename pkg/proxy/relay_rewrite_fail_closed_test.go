package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	rewriteplugin "github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/rewriter"
)

// These tests drive the real Relay through the real rewrite plugin and the
// real SentioNetworkFactory (gRPC engine) against an in-process rewriter
// service, with storage integrity disabled. They pin spec 2026-09-26 T8 end to
// end: every engine rejection reaches the client as an Exception and nothing
// is forwarded upstream; a rewriter outage fails closed unless
// fail_open_on_unavailable is set (Plan C Review Focus 5).

// scriptedRewriterServer answers every Rewrite with a fixed response.
type scriptedRewriterServer struct {
	pb.UnimplementedRewriterServiceServer
	resp *pb.RewriteSQLResponse
	err  error

	mu   sync.Mutex
	seen []string
}

func (s *scriptedRewriterServer) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	s.mu.Lock()
	s.seen = append(s.seen, req.GetSql())
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

func (s *scriptedRewriterServer) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

// startScriptedRewriter serves resp on a loopback gRPC listener and returns the
// server, the grpc.Server (so a test can stop it to simulate an outage) and
// the rewrite plugin wired over a real SentioNetworkFactory.
func startScriptedRewriter(t *testing.T, resp *pb.RewriteSQLResponse, failOpen bool) (*scriptedRewriterServer, *grpc.Server, *rewriteplugin.Plugin) {
	t.Helper()
	srv, gs, plug, _ := startScriptedRewriterWithFactory(t, resp, failOpen)
	return srv, gs, plug
}

func startScriptedRewriterWithFactory(t *testing.T, resp *pb.RewriteSQLResponse, failOpen bool) (*scriptedRewriterServer, *grpc.Server, *rewriteplugin.Plugin, *rewriter.SentioNetworkFactory) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &scriptedRewriterServer{resp: resp}
	gs := grpc.NewServer()
	pb.RegisterRewriterServiceServer(gs, srv)
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)

	reg := network.NewInMemoryNetworkState()
	reg.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}
	factory, err := rewriter.NewSentioNetworkFactory(rewriter.Options{
		Enabled:               true,
		Engine:                rewriter.EngineGRPC,
		ServiceAddr:           listener.Addr().String(),
		Timeout:               2 * time.Second,
		PhysicalDatabase:      "phys",
		FailOpenOnUnavailable: failOpen,
	}, reg)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	t.Cleanup(func() { _ = factory.Close() })
	return srv, gs, &rewriteplugin.Plugin{
		Factory:               factory,
		PhysicalDatabase:      "phys",
		FailOpenOnUnavailable: failOpen,
	}, factory
}

// probeSession is a minimal rewriter.Session for outage probes.
type probeSession struct{}

func (probeSession) Account() string              { return "" }
func (probeSession) LogicalDatabaseName() string  { return "" }
func (probeSession) PhysicalDatabaseName() string { return "" }
func (probeSession) SetLogicalDatabase(string)    {}

// waitForRewriterOutage blocks until the factory's client has noticed the
// stopped server, i.e. a call fails before the request is sent. A call in
// the instant before that is written to a dying transport, which counts as
// sent and is therefore a rejection; the outage these tests model is the
// steady state after the service went away.
func waitForRewriterOutage(t *testing.T, factory *rewriter.SentioNetworkFactory) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := factory.NewRewriter(probeSession{}).Rewrite(context.Background(), "SELECT 1", "")
		var unavailable *rewriter.UnavailableError
		var rej *rewriter.RejectedError
		if errors.As(err, &unavailable) || (errors.As(err, &rej) && rej.Message == "rewriter unavailable") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("rewriter client did not notice the stopped server")
}

// relayRunResult is what one client Query produced on both legs.
type relayRunResult struct {
	exception *chproto.Exception
	upstream  []*chproto.Query
}

// runQueryThroughRelay sends one Query from the client through a Relay whose
// hooks are chain, and reports the client's Exception (if any) and every Query
// the upstream received within a short window.
func runQueryThroughRelay(t *testing.T, ctx context.Context, chain *plugin.PluginChain, sql string) relayRunResult {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	t.Cleanup(func() {
		_ = clientProxy.Close()
		_ = upstreamProxy.Close()
		_ = proxyClient.Close()
		_ = proxyUpstream.Close()
	})

	upstreamGot := make(chan []*chproto.Query, 1)
	go func() {
		codec := chproto.NewCodec(upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		var got []*chproto.Query
		_ = upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
		for {
			pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
			if err != nil || pkt == nil {
				break
			}
			if q, ok := pkt.Decoded.(*chproto.Query); ok {
				got = append(got, q)
			}
		}
		upstreamGot <- got
	}()

	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(deferredTestRev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: chain}
	loopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	loopDone := make(chan error, 1)
	go func() { loopDone <- r.clientToUpstream(loopCtx) }()

	// net.Pipe is synchronous: the client reader must run before the client
	// writes, or the relay's Exception write and the client's next write
	// block each other.
	excCh := make(chan *chproto.Exception, 1)
	go func() {
		codec := chproto.NewCodec(clientProxy, chproto.DirToUpstream)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		_ = clientProxy.SetReadDeadline(time.Now().Add(time.Second))
		pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil || pkt == nil {
			excCh <- nil
			return
		}
		exc, _ := pkt.Decoded.(*chproto.Exception)
		excCh <- exc
	}()

	writeAllConn(t, clientProxy, encodeInsertQuery(t, "q1", sql))
	// The ClickHouse client always follows a Query with the external-tables
	// terminator.
	writeAllConn(t, clientProxy, encodeEmptyClientData(t))

	var res relayRunResult
	res.exception = <-excCh
	res.upstream = <-upstreamGot
	_ = clientProxy.Close()
	_ = proxyUpstream.Close()
	select {
	case <-loopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("clientToUpstream did not return after the client closed")
	}
	return res
}

func TestRelay_RewriterRejectionWithoutStorageIntegrityReachesClientAndForwardsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		resp *pb.RewriteSQLResponse
	}{
		{
			// A vertical tab is whitespace to ClickHouse but not to the
			// engine's lexer; the engine answers SyntaxError. The audit
			// measured this shape being forwarded verbatim before T8.
			name: "syntax error on a vertical tab",
			sql:  "SELECT * FROM\vphys.`db2.secret`",
			resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_SyntaxError, Message: "unexpected character at position 13"},
		},
		{
			name: "unsupported statement",
			sql:  "DETACH TABLE phys.`db2.x`",
			resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "statement is not supported"},
		},
		{
			name: "invalid rewrite request",
			sql:  "RENAME TABLE phys.`db2.x` TO db1.stolen",
			resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_InvalidRewriteRequest, Message: "database phys is not addressable"},
		},
	} {
		for _, failOpen := range []bool{false, true} {
			name := tc.name
			if failOpen {
				name += "/fail_open_on_unavailable"
			}
			t.Run(name, func(t *testing.T) {
				srv, _, plug := startScriptedRewriter(t, tc.resp, failOpen)
				chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{plug}}
				res := runQueryThroughRelay(t, context.Background(), chain, tc.sql)

				if got := srv.calls(); len(got) != 1 || got[0] != tc.sql {
					t.Fatalf("rewriter saw %q, want exactly the client SQL", got)
				}
				if res.exception == nil {
					t.Fatal("client received no Exception")
				}
				if !strings.Contains(res.exception.Message, tc.resp.GetMessage()) {
					t.Fatalf("exception message = %q, want the engine message %q", res.exception.Message, tc.resp.GetMessage())
				}
				if len(res.upstream) != 0 {
					t.Fatalf("upstream received %d Query packet(s), first %q; want nothing forwarded", len(res.upstream), res.upstream[0].Body)
				}
			})
		}
	}
}

// TestRelay_RewriterOutageFollowsFailOpenOnUnavailable is Plan C Review Focus
// 5 at the relay: with storage integrity disabled and the rewriter down, the
// switch on forwards the original SQL and logs a warning; the switch off
// answers the client with an Exception and forwards nothing.
func TestRelay_RewriterOutageFollowsFailOpenOnUnavailable(t *testing.T) {
	const sql = "SELECT a FROM db1.t"
	for _, failOpen := range []bool{false, true} {
		t.Run(map[bool]string{false: "switch off", true: "switch on"}[failOpen], func(t *testing.T) {
			srv, gs, plug, factory := startScriptedRewriterWithFactory(t, &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "never"}, failOpen)
			gs.Stop() // the outage
			waitForRewriterOutage(t, factory)
			chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{plug}}
			logs := &relayCaptureHandler{}
			ctx := log.WithContext(context.Background(), log.New(logs))
			res := runQueryThroughRelay(t, ctx, chain, sql)

			if got := srv.calls(); len(got) != 0 {
				t.Fatalf("rewriter answered %q during an outage", got)
			}
			if failOpen {
				if res.exception != nil {
					t.Fatalf("client received Exception %q, want the query forwarded", res.exception.Message)
				}
				if len(res.upstream) != 1 || res.upstream[0].Body != sql {
					t.Fatalf("upstream received %d Query packet(s), want the original SQL once", len(res.upstream))
				}
				if !logs.hasWarn("fail_open_on_unavailable") {
					t.Fatalf("want a warning naming fail_open_on_unavailable, got %v", logs.messages())
				}
				return
			}
			if res.exception == nil || !strings.Contains(res.exception.Message, "rewriter unavailable") {
				t.Fatalf("exception = %+v, want a rewriter-unavailable Exception", res.exception)
			}
			if strings.Contains(res.exception.Message, "127.0.0.1") || strings.Contains(res.exception.Message, "rpc error") {
				t.Fatalf("exception %q leaks transport detail to the client", res.exception.Message)
			}
			if len(res.upstream) != 0 {
				t.Fatalf("upstream received %d Query packet(s), want nothing forwarded", len(res.upstream))
			}
		})
	}
}

// relayCaptureHandler records slog messages for assertions.
type relayCaptureHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *relayCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *relayCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *relayCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *relayCaptureHandler) WithGroup(string) slog.Handler      { return h }

func (h *relayCaptureHandler) hasWarn(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

func (h *relayCaptureHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.recs))
	for i, r := range h.recs {
		out[i] = r.Level.String() + " " + r.Message
	}
	return out
}

// TestRelay_EngineFailureAfterReceiptIsRefusedEvenUnderTheSwitch pins review
// M1 at the relay: an engine that fails on a statement it received (gRPC
// UNKNOWN from an unexpected C++ exception) must not turn into a
// fail_open_on_unavailable pass-through.
func TestRelay_EngineFailureAfterReceiptIsRefusedEvenUnderTheSwitch(t *testing.T) {
	const sql = "SELECT * FROM phys.`db2.secret`"
	srv, _, plug := startScriptedRewriter(t, nil, true)
	srv.err = status.Error(codes.Unknown, "unexpected exception in handler")
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{plug}}
	res := runQueryThroughRelay(t, context.Background(), chain, sql)
	if got := srv.calls(); len(got) != 1 {
		t.Fatalf("rewriter saw %q, want the statement once", got)
	}
	if res.exception == nil {
		t.Fatal("client received no Exception")
	}
	if len(res.upstream) != 0 {
		t.Fatalf("upstream received %d Query packet(s); an engine failure after receipt must not fail open", len(res.upstream))
	}
}

// TestRelay_InvalidUTF8IsRefusedEvenUnderTheSwitch pins re-review R1 at the
// relay: invalid UTF-8 used to fail proto marshalling before send, which was
// classified as unavailable and forwarded under fail_open_on_unavailable.
func TestRelay_InvalidUTF8IsRefusedEvenUnderTheSwitch(t *testing.T) {
	const sql = "SELECT '\xff\xfe' FROM phys.`db2.secret`"
	srv, _, plug := startScriptedRewriter(t, &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "never"}, true)
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{plug}}
	res := runQueryThroughRelay(t, context.Background(), chain, sql)
	if got := srv.calls(); len(got) != 0 {
		t.Fatalf("rewriter saw %d statement(s), want none", len(got))
	}
	if res.exception == nil || !strings.Contains(res.exception.Message, "not valid UTF-8") {
		t.Fatalf("exception = %+v, want a refusal naming invalid UTF-8", res.exception)
	}
	if len(res.upstream) != 0 {
		t.Fatalf("upstream received %d Query packet(s); invalid UTF-8 must not fail open", len(res.upstream))
	}
}
