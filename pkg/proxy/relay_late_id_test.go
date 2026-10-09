package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

const lateStatementID = "0x00000000000000000000000000000000000000a1:1:late"

type lateIDHooks struct {
	deferredInsertHooks
	mu        sync.Mutex
	successID string
}

func (h *lateIDHooks) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	qctx.Query.ID = lateStatementID
	return nil
}

func (h *lateIDHooks) OnQuerySuccess(_ context.Context, _ chsession.Session, queryID string) {
	h.mu.Lock()
	h.successID = queryID
	h.mu.Unlock()
}

// Spec 2026-10-09 §3.1 / §9.1: the id the strict hook writes is the id Relay
// records as active and the upstream receives — no retagging exists.
func TestRelay_DeferredInsertForwardsTheIDTheStrictHookWrote(t *testing.T) {
	hooks := &lateIDHooks{deferredInsertHooks: deferredInsertHooks{inputDone: make(chan struct{}, 1)}}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			upDone <- err
			return
		}
		if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.ID != lateStatementID {
			upDone <- errors.New("upstream did not receive the strict hook's statement id")
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // external-tables marker
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		for i := 0; i < 2; i++ { // payload + terminator
			if _, err := codec.ReadPacket(); err != nil {
				upDone <- err
				return
			}
		}
		select {
		case <-hooks.inputDone:
		case <-time.After(time.Second):
			upDone <- errors.New("no input completion")
			return
		}
		_, err = h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "client-id", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("sample = %x", got)
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal = %d", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.successID != lateStatementID {
		t.Fatalf("OnQuerySuccess id = %q, want the strict hook's id", hooks.successID)
	}
	for _, err := range h.close(t) {
		if err != nil && !errors.Is(err, io.EOF) {
			t.Logf("relay loop returned: %v", err)
		}
	}
}
