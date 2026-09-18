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
		plan: &plugin.AgentPreparePlan{AuthorizeForward: func(context.Context, plugin.PreparedAgentQuery) error {
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
				plan:     &plugin.AgentPreparePlan{},
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
			select {
			case err := <-done:
				if !errors.Is(err, errAgentPrepareCanceled) {
					t.Fatalf("stage result=%v, want cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Cancel was not observed while stage blocked")
			}
			close(release)
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
	}}
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
		if !errors.Is(err, errAgentPrepareCanceled) {
			t.Fatalf("apply result=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("continuation cancellation was not observed")
	}
	if intents.Load() != 0 || authorizations.Load() != 0 {
		t.Fatalf("cancelled continuation persisted forward intent=%d authorization=%d", intents.Load(), authorizations.Load())
	}
	close(release)
}
