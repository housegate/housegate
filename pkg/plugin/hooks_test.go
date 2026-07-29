package plugin

import (
	"context"
	"testing"
	"time"
)

func TestNoopHooks_AllMethodsSafe(t *testing.T) {
	h := NoopHooks{}
	if err := h.OnConnect(context.Background(), nil); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	if err := h.OnHello(context.Background(), nil, nil); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	h.OnHandshakeComplete(context.Background(), nil, 10*time.Millisecond)
	if err := h.OnQuery(context.Background(), nil); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if h.RejectUndecodableQuery(nil) {
		t.Fatal("RejectUndecodableQuery = true, want false")
	}
	if err := h.OnException(context.Background(), nil, nil); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	h.OnQueryInputComplete(context.Background(), nil)
	h.OnQueryAbort(context.Background(), nil)
	h.OnQueryComplete(context.Background(), nil)
	h.OnClose(nil)
	h.OnDisconnect(nil)
}
