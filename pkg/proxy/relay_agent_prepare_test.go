package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

type blockingResumeHooks struct {
	plugin.NoopHooks
	entered chan struct{}
	release chan struct{}
}

type relayPrepareHooks struct {
	plugin.NoopHooks
	prepare   func(context.Context) (plugin.PreparedAgentQuery, error)
	intent    func(context.Context, plugin.PreparedAgentQuery) error
	authorize func(context.Context, plugin.PreparedAgentQuery) error
	unknown   func(context.Context, plugin.PreparedAgentQuery) error
	aborts    *atomic.Int32
	completes *atomic.Int32
}

func (h relayPrepareHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	if h.aborts != nil {
		h.aborts.Add(1)
	}
}

func (h relayPrepareHooks) OnQueryComplete(context.Context, chsession.Session) {
	if h.completes != nil {
		h.completes.Add(1)
	}
}

func (h relayPrepareHooks) SupportsQueryContinuation() bool { return true }

func (h relayPrepareHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	qctx.AgentPrepare = &plugin.AgentPreparePlan{
		Prepare: h.prepare, PersistForwardIntent: h.intent, AuthorizeForward: h.authorize, PersistForwardUnknown: h.unknown,
		MaxControlBytes: 1024,
	}
	return nil
}

func (h blockingResumeHooks) ResumeQuery(context.Context, *plugin.QueryContext) error {
	close(h.entered)
	<-h.release // deliberately ignores the cancellation context
	return nil
}

func TestRelayAgentPrepare_CancelWinsOverLateWorker(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	defer clientPeer.Close()
	defer clientProxy.Close()
	sess := chsession.New(1, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "prepare-cancel"}}
	started := make(chan struct{})
	allowReturn := make(chan struct{})
	var reconciled atomic.Int32
	qctx.AgentPrepare = &plugin.AgentPreparePlan{
		Prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
			close(started)
			<-allowReturn
			return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "late"}, Claimed: true}, nil
		},
		ReconcileCancel: func(context.Context) error { reconciled.Add(1); return nil },
		MaxControlBytes: 1024,
	}
	if !r.beginActiveQuery(qctx.Query.ID) {
		t.Fatal("begin active query")
	}
	generation := r.nextAgentPrepareGeneration()
	result := make(chan error, 1)
	go func() {
		_, err := r.waitAgentPrepare(context.Background(), qctx, generation)
		result <- err
	}()
	<-started
	var raw proto.Buffer
	raw.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(raw.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errAgentPrepareCanceled) {
			t.Fatalf("wait result=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not observe Cancel while worker was blocked")
	}
	close(allowReturn)
	deadline := time.Now().Add(time.Second)
	for reconciled.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if reconciled.Load() == 0 {
		t.Fatal("cancel reconciliation was not scheduled")
	}
	if r.agentPrepareLive(qctx.Query.ID, generation) {
		t.Fatal("canceled generation remained live after late worker release")
	}
}

func TestRelayAgentPrepare_AuthorizationFailureForbidsLaunch(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	defer clientPeer.Close()
	defer clientProxy.Close()
	sess := chsession.New(1, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	if !r.beginActiveQuery("authorize") {
		t.Fatal("begin active query")
	}
	generation := r.nextAgentPrepareGeneration()
	var attempted atomic.Int32
	result := &agentPrepareResult{
		queryID:    "authorize",
		generation: generation,
		prepared:   plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "authorize"}},
		plan: &plugin.AgentPreparePlan{MaxControlBytes: 1024, AuthorizeForward: func(context.Context, plugin.PreparedAgentQuery) error {
			attempted.Add(1)
			return errors.New("fsync outcome unknown")
		}},
	}
	if err := r.authorizeAgentForward(context.Background(), result); err == nil {
		t.Fatal("authorization failure allowed launch")
	}
	if attempted.Load() != 1 {
		t.Fatalf("authorization calls=%d, want 1", attempted.Load())
	}
	// This test intentionally has no upstream codec/write.  A caller only gets
	// past authorizeAgentForward on a proven durable authorization.
}

func TestRelayAgentPrepare_CancelDuringContinuationOrAuthorizationStage(t *testing.T) {
	for _, stage := range []string{"continuation", "authorization"} {
		t.Run(stage, func(t *testing.T) {
			clientPeer, clientProxy := net.Pipe()
			defer clientPeer.Close()
			defer clientProxy.Close()
			sess := chsession.New(1, clientProxy)
			r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
			if !r.beginActiveQuery(stage) {
				t.Fatal("begin active query")
			}
			generation := r.nextAgentPrepareGeneration()
			entered := make(chan struct{})
			release := make(chan struct{})
			result := &agentPrepareResult{
				queryID: stage, generation: generation,
				prepared: plugin.PreparedAgentQuery{Query: &chproto.Query{ID: stage}},
				plan:     &plugin.AgentPreparePlan{MaxControlBytes: 1024},
			}
			done := make(chan error, 1)
			go func() {
				done <- r.runAgentPrepareStage(context.Background(), result, func(context.Context) error {
					close(entered)
					<-release
					return nil
				})
			}()
			<-entered
			var raw proto.Buffer
			raw.PutUVarInt(uint64(chproto.ClientCancelCode))
			if _, err := clientPeer.Write(raw.Buf); err != nil {
				t.Fatalf("write Cancel: %v", err)
			}
			// Cancellation closes/quarantines the session and joins the stage
			// before lifecycle cleanup, so an intentionally non-cooperative
			// callback keeps this call pending until released.
			select {
			case err := <-done:
				t.Fatalf("stage returned before blocked callback released: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, errAgentPrepareCanceled) {
					t.Fatalf("stage result=%v, want cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Cancel was not observed while stage blocked")
			}
			if r.agentPrepareLive(stage, generation) {
				t.Fatal("canceled stage remained eligible to forward")
			}
		})
	}
}

func TestRelayAgentPrepare_EOFInvalidatesGeneration(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	sess := chsession.New(1, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: "prepare-eof"}}
	started := make(chan struct{})
	release := make(chan struct{})
	qctx.AgentPrepare = &plugin.AgentPreparePlan{Prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
		close(started)
		<-release
		return plugin.PreparedAgentQuery{}, context.Canceled
	}, MaxControlBytes: 1024}
	if !r.beginActiveQuery(qctx.Query.ID) {
		t.Fatal("begin active query")
	}
	generation := r.nextAgentPrepareGeneration()
	done := make(chan error, 1)
	go func() {
		_, err := r.waitAgentPrepare(context.Background(), qctx, generation)
		done <- err
	}()
	<-started
	if err := clientPeer.Close(); err != nil {
		t.Fatalf("close client peer: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("EOF result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not observe EOF")
	}
	if r.agentPrepareLive(qctx.Query.ID, generation) {
		t.Fatal("EOF generation remained eligible to forward")
	}
	close(release)
	_ = clientProxy.Close()
}

func TestRelayAgentPrepare_NonCooperativeContinuationCancelHasZeroForward(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	defer clientPeer.Close()
	defer clientProxy.Close()
	sess := chsession.New(1, clientProxy)
	entered, release := make(chan struct{}), make(chan struct{})
	r := NewRelay(sess, blockingResumeHooks{entered: entered, release: release}, nil, nil)
	if !r.beginActiveQuery("continuation-cancel") {
		t.Fatal("begin active query")
	}
	generation := r.nextAgentPrepareGeneration()
	var intents, authorizations atomic.Int32
	result := &agentPrepareResult{
		queryID: "continuation-cancel", generation: generation,
		prepared: plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "continuation-cancel"}},
		plan: &plugin.AgentPreparePlan{
			MaxControlBytes:      1024,
			PersistForwardIntent: func(context.Context, plugin.PreparedAgentQuery) error { intents.Add(1); return nil },
			AuthorizeForward:     func(context.Context, plugin.PreparedAgentQuery) error { authorizations.Add(1); return nil },
		},
	}
	qctx := &plugin.QueryContext{Session: sess, Query: &chproto.Query{ID: result.queryID}, Values: map[string]any{}}
	done := make(chan error, 1)
	go func() { done <- r.applyAgentPrepare(context.Background(), qctx, result) }()
	<-entered
	var raw proto.Buffer
	raw.PutUVarInt(uint64(chproto.ClientCancelCode))
	if _, err := clientPeer.Write(raw.Buf); err != nil {
		t.Fatalf("write Cancel: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("apply returned before non-cooperative continuation released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, errAgentPrepareCanceled) {
			t.Fatalf("apply result=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("continuation cancellation was not observed")
	}
	if intents.Load() != 0 || authorizations.Load() != 0 {
		t.Fatalf("cancelled continuation persisted forward intent=%d authorization=%d", intents.Load(), authorizations.Load())
	}
}

func TestRelayAgentPrepare_RelayCancelNeverWritesUpstreamQuery(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	upstreamPeer, upstreamProxy := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()
	const rev = chproto.MaxSupportedRevision
	sess := chsession.New(1, clientProxy)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(upstreamProxy, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	hooks := relayPrepareHooks{
		prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
			close(started)
			<-release
			return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "SELECT 1"}}, nil
		},
		intent:    func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		authorize: func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		unknown:   func(context.Context, plugin.PreparedAgentQuery) error { return nil },
	}
	r := NewRelay(sess, hooks, nil, nil)
	client := chproto.NewCodec(clientPeer, chproto.DirToUpstream)
	client.SetRevision(rev)
	run := make(chan error, 1)
	go func() { run <- r.clientToUpstream(context.Background()) }()
	writeQuery := make(chan error, 1)
	go func() { writeQuery <- client.WriteQuery(&chproto.Query{ID: "cancel-no-write", Body: "SELECT 1"}) }()
	<-started
	if err := <-writeQuery; err != nil {
		t.Fatalf("write query: %v", err)
	}
	if err := client.WriteRawPacket([]byte{byte(chproto.ClientCancelCode)}); err != nil {
		t.Fatalf("write cancel: %v", err)
	}
	close(release)
	select {
	case <-run:
	case <-time.After(time.Second):
		t.Fatal("relay did not finish after canceled preparation")
	}
	_ = upstreamPeer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := upstreamPeer.Read(buf); n != 0 || err == nil {
		t.Fatalf("upstream received Query bytes n=%d err=%v", n, err)
	}
}

func TestRelayAgentPrepare_RelayAuthorizationFailureNeverWritesUpstreamQuery(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	upstreamPeer, upstreamProxy := net.Pipe()
	defer clientPeer.Close()
	defer upstreamPeer.Close()
	const rev = chproto.MaxSupportedRevision
	sess := chsession.New(1, clientProxy)
	sess.Client().SetRevision(rev)
	up := chproto.NewCodec(upstreamProxy, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	hooks := relayPrepareHooks{
		prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
			return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "SELECT 1"}}, nil
		},
		intent: func(context.Context, plugin.PreparedAgentQuery) error { return nil },
		authorize: func(context.Context, plugin.PreparedAgentQuery) error {
			return errors.New("authorization fsync unknown")
		},
		unknown: func(context.Context, plugin.PreparedAgentQuery) error { return nil },
	}
	r := NewRelay(sess, hooks, nil, nil)
	client := chproto.NewCodec(clientPeer, chproto.DirToUpstream)
	client.SetRevision(rev)
	run := make(chan error, 1)
	go func() { run <- r.clientToUpstream(context.Background()) }()
	if err := client.WriteQuery(&chproto.Query{ID: "authorize-no-write", Body: "SELECT 1"}); err != nil {
		t.Fatalf("write query: %v", err)
	}
	if _, err := client.ReadPacket(uint64(chproto.ServerExceptionCode)); err != nil {
		t.Fatalf("read authorization exception: %v", err)
	}
	_ = clientPeer.Close()
	select {
	case <-run:
	case <-time.After(time.Second):
		t.Fatal("relay did not finish after authorization failure")
	}
	_ = upstreamPeer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := upstreamPeer.Read(buf); n != 0 || err == nil {
		t.Fatalf("upstream received Query bytes n=%d err=%v", n, err)
	}
}

func TestRelayAgentPrepare_GateWonCancelPersistsForwardUnknown(t *testing.T) {
	clientPeer, clientProxy := net.Pipe()
	defer clientPeer.Close()
	defer clientProxy.Close()
	sess := chsession.New(1, clientProxy)
	r := NewRelay(sess, plugin.NoopHooks{}, nil, nil)
	if !r.beginActiveQuery("gate-won") {
		t.Fatal("begin active query")
	}
	generation := r.nextAgentPrepareGeneration()
	entered, release := make(chan struct{}), make(chan struct{})
	var authorized, unknown atomic.Int32
	result := &agentPrepareResult{
		queryID: "gate-won", generation: generation,
		prepared: plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "gate-won"}},
		plan: &plugin.AgentPreparePlan{
			MaxControlBytes: 1024,
			AuthorizeForward: func(context.Context, plugin.PreparedAgentQuery) error {
				authorized.Add(1)
				close(entered)
				<-release
				return nil
			},
			PersistForwardUnknown: func(context.Context, plugin.PreparedAgentQuery) error {
				unknown.Add(1)
				return nil
			},
		},
	}
	done := make(chan error, 1)
	go func() { done <- r.authorizeAgentForward(context.Background(), result) }()
	<-entered
	if err := clientPeer.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, errAgentPrepareForwardUnknown) {
			t.Fatalf("authorize result=%v, want forward unknown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("authorized stage did not settle")
	}
	if authorized.Load() != 1 || unknown.Load() != 1 {
		t.Fatalf("authorized=%d unknown=%d, want 1/1", authorized.Load(), unknown.Load())
	}
}

func TestRelayAgentPrepare_RelayGateWonCancelReconcilesOnlyAfterAuthorizationSuccess(t *testing.T) {
	for _, tc := range []struct {
		name         string
		authorizeErr error
		wantUnknown  int32
	}{
		{name: "success", wantUnknown: 1},
		{name: "failure", authorizeErr: errors.New("durable authorization unknown"), wantUnknown: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientPeer, clientProxy := net.Pipe()
			upstreamPeer, upstreamProxy := net.Pipe()
			defer clientPeer.Close()
			defer upstreamPeer.Close()
			const rev = chproto.MaxSupportedRevision
			sess := chsession.New(1, clientProxy)
			sess.Client().SetRevision(rev)
			up := chproto.NewCodec(upstreamProxy, chproto.DirToUpstream)
			up.SetRevision(rev)
			if err := sess.BindUpstream(context.Background(), up); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var authorized, unknown, aborts, completes atomic.Int32
			hooks := relayPrepareHooks{
				prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
					return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "SELECT 1"}}, nil
				},
				intent: func(context.Context, plugin.PreparedAgentQuery) error { return nil },
				authorize: func(context.Context, plugin.PreparedAgentQuery) error {
					authorized.Add(1)
					close(entered)
					<-release
					return tc.authorizeErr
				},
				unknown: func(context.Context, plugin.PreparedAgentQuery) error { unknown.Add(1); return nil },
				aborts:  &aborts, completes: &completes,
			}
			r := NewRelay(sess, hooks, nil, nil)
			client := chproto.NewCodec(clientPeer, chproto.DirToUpstream)
			client.SetRevision(rev)
			run := make(chan error, 1)
			go func() { run <- r.clientToUpstream(context.Background()) }()
			if err := client.WriteQuery(&chproto.Query{ID: "gate-won", Body: "SELECT 1"}); err != nil {
				t.Fatal(err)
			}
			<-entered
			_ = clientPeer.Close() // EOF after the forward gate has won.
			close(release)
			var err error
			select {
			case err = <-run:
			case <-time.After(time.Second):
				t.Fatal("relay did not settle")
			}
			if authorized.Load() != 1 || unknown.Load() != tc.wantUnknown {
				t.Fatalf("authorized=%d unknown=%d", authorized.Load(), unknown.Load())
			}
			if tc.authorizeErr == nil {
				if !errors.Is(err, errAgentPrepareForwardUnknown) || aborts.Load() != 0 || completes.Load() != 0 {
					t.Fatalf("success err=%v aborts=%d completes=%d", err, aborts.Load(), completes.Load())
				}
			} else if !errors.Is(err, tc.authorizeErr) {
				t.Fatalf("failure err=%v want %v", err, tc.authorizeErr)
			}
			_ = upstreamPeer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			b := make([]byte, 1)
			if n, e := upstreamPeer.Read(b); n != 0 || e == nil {
				t.Fatalf("upstream bytes n=%d err=%v", n, e)
			}
		})
	}
}
