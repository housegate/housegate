package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

type switchingHooks struct {
	plugin.NoopHooks
	newUp *chproto.Codec
}

func (h *switchingHooks) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	return qctx.Session.SwitchUpstream(ctx, h.newUp, &chproto.ClientHello{Name: "client", ProtocolVersion: deferredTestRev})
}

// Spec 2026-10-09 §6.4 step 6: the switch runs inside OnQuery; Relay forwards
// the query on the new upstream and upstreamToClient resumes on it once its
// blocked read on the closed old connection fails.
func TestRelay_SwitchUpstreamInOnQueryServesTheQueryOnTheNewUpstream(t *testing.T) {
	newServer, newClient := net.Pipe()
	t.Cleanup(func() { _ = newServer.Close(); _ = newClient.Close() })
	hooks := &switchingHooks{newUp: chproto.NewCodec(newClient, chproto.DirToUpstream)}
	h := newDeferredHarness(t, hooks)
	// Relay's handshake records the client leg's revision; the harness skips it.
	h.relay.sess.State().ClientRevision = deferredTestRev
	served := make(chan string, 1)
	go func() {
		defer newServer.Close()
		codec := chproto.NewCodec(newServer, chproto.DirFromClient)
		if _, err := codec.ReadPacket(uint64(chproto.ClientHelloCode)); err != nil {
			served <- "hello: " + err.Error()
			return
		}
		var buf proto.Buffer
		(&proto.ServerHello{Name: "target", Major: 24, Minor: 1, Revision: deferredTestRev}).EncodeAware(&buf, deferredTestRev)
		_, _ = newServer.Write(buf.Buf)
		codec.SetRevision(deferredTestRev)
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			served <- "query: " + err.Error()
			return
		}
		served <- pkt.Decoded.(*chproto.Query).Body
		_, _ = codec.ReadPacket() // the client's empty Data
		_, _ = newServer.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "SELECT 1"))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	select {
	case body := <-served:
		if body != "SELECT 1" {
			t.Fatalf("new upstream got %q", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the query never reached the new upstream")
	}
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want the new upstream's EndOfStream", got[0])
	}
	if h.relay.sess.Upstream() != hooks.newUp {
		t.Fatal("the session does not keep the new upstream")
	}
	// The old upstream connection was closed by the switch: its peer reads
	// EOF, not a deadline.
	_ = h.upstreamProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.upstreamProxy.Read(make([]byte, 1))
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("the old upstream connection is still open")
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("old upstream read = %v, want io.EOF", err)
	}
}
