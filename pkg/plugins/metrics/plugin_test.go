package metrics

import (
	"context"
	"testing"
	"time"

	"housegate/housegate/pkg/chproto"
)

type fakeObserver struct {
	connOpens  int
	connCloses int
	queriesFwd int
	errors     []string
	handshakes []float64
}

func (f *fakeObserver) ConnectionOpened()            { f.connOpens++ }
func (f *fakeObserver) ConnectionClosed()            { f.connCloses++ }
func (f *fakeObserver) QueryForwarded()              { f.queriesFwd++ }
func (f *fakeObserver) Error(phase string, _ error)  { f.errors = append(f.errors, phase) }
func (f *fakeObserver) HandshakeCompleted(d float64) { f.handshakes = append(f.handshakes, d) }

func TestPlugin_ConnLifecycle(t *testing.T) {
	obs := &fakeObserver{}
	p := New(obs)

	if err := p.OnConnect(context.Background(), nil); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	if obs.connOpens != 1 {
		t.Errorf("connOpens=%d, want 1", obs.connOpens)
	}
	p.OnDisconnect(nil)
	if obs.connCloses != 1 {
		t.Errorf("connCloses=%d, want 1", obs.connCloses)
	}
}

func TestPlugin_OnQuery_EmitsForwarded(t *testing.T) {
	obs := &fakeObserver{}
	p := New(obs)

	if err := p.OnQuery(context.Background(), nil); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if obs.queriesFwd != 1 {
		t.Errorf("queriesFwd=%d, want 1", obs.queriesFwd)
	}
}

func TestPlugin_OnException_RecordsError(t *testing.T) {
	obs := &fakeObserver{}
	p := New(obs)

	exc := &chproto.Exception{Code: 42, Name: "DB::Exception", Message: "boom"}
	if err := p.OnException(context.Background(), nil, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if len(obs.errors) != 1 || obs.errors[0] != "upstream_exception" {
		t.Errorf("errors=%v, want [upstream_exception]", obs.errors)
	}

	// Nil exception: no Error call.
	obs2 := &fakeObserver{}
	p2 := New(obs2)
	_ = p2.OnException(context.Background(), nil, nil)
	if len(obs2.errors) != 0 {
		t.Errorf("nil exc: errors=%v, want none", obs2.errors)
	}
}

func TestPlugin_OnHandshakeComplete_RecordsSeconds(t *testing.T) {
	obs := &fakeObserver{}
	p := New(obs)

	p.OnHandshakeComplete(context.Background(), nil, 250*time.Millisecond)
	if len(obs.handshakes) != 1 {
		t.Fatalf("handshakes=%v, want 1 entry", obs.handshakes)
	}
	if got := obs.handshakes[0]; got < 0.24 || got > 0.26 {
		t.Errorf("handshake duration=%v seconds, want ~0.25", got)
	}
}

func TestNew_NilObserverReturnsNil(t *testing.T) {
	if p := New(nil); p != nil {
		t.Errorf("New(nil) = %v, want nil", p)
	}
}
