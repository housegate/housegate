package proxy

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// stagedRejectHooks mimics the storage-integrity ingress plugin: it stages the
// first INSERT and rejects it at the end-of-input boundary with a retryable,
// session-preserving 252.
type stagedRejectHooks struct {
	plugin.NoopHooks
	mu        sync.Mutex
	queries   []string
	rejectOne bool
	aborts    int
	completes int
	successes int
}

func (h *stagedRejectHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queries = append(h.queries, qctx.Query.Body)
	qctx.SuppressUpstreamExecution = len(h.queries) == 1
	return nil
}

func (h *stagedRejectHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.rejectOne {
		return nil
	}
	h.rejectOne = false
	return &chproto.ClientError{
		Code:        chproto.CodeTooManyParts,
		Message:     "storage_integrity: back-pressure: hg_unsafe.db__t partition p_p0 has 2400 active parts (soft limit 2400); retry later",
		KeepSession: true,
	}
}

func (h *stagedRejectHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.aborts++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.completes++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	h.mu.Lock()
	h.successes++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) counts() (aborts, completes, successes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.aborts, h.completes, h.successes
}

// Spec L D6 acceptance: the client receives Exception 252 and the same
// connection remains usable for a subsequent query.
func TestRelay_StagedRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	hooks := &stagedRejectHooks{rejectOne: true}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upDone := make(chan error, 1)
	go func() { upDone <- serveStagedRejectUpstream(t, h.upstreamProxy) }()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	writeAllConn(t, h.clientProxy, empty) // external-tables marker
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	writeAllConn(t, h.clientProxy, empty) // terminator

	exc := readServerException(t, h.clientProxy)
	if exc.Code != proto.Error(chproto.CodeTooManyParts) {
		t.Fatalf("exception code = %d, want 252", exc.Code)
	}
	if !bytes.Contains([]byte(exc.Message), []byte("back-pressure")) {
		t.Fatalf("exception message = %q", exc.Message)
	}
	waitForRejectCounts(t, hooks)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("a relay loop exited after the rejection: %v", err)
	default:
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
}

func waitForRejectCounts(t *testing.T, hooks *stagedRejectHooks) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if aborts, completes, successes := hooks.counts(); aborts == 1 && completes == 1 && successes == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	aborts, completes, successes := hooks.counts()
	t.Fatalf("lifecycle abort/complete/success = %d/%d/%d, want 1/1/0", aborts, completes, successes)
}

func serveStagedRejectUpstream(t *testing.T, conn net.Conn) error {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	for _, want := range []string{"INSERT INTO db.t FORMAT Native", "SELECT 1"} {
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			return err
		}
		q, ok := pkt.Decoded.(*chproto.Query)
		if !ok || q.Body != want {
			t.Errorf("upstream query = %#v, want %q", pkt.Decoded, want)
		}
		marker, err := codec.ReadPacket()
		if err != nil {
			return err
		}
		if empty, err := chproto.ClientDataPacketIsEmpty(marker.Raw, proto.CompressionDisabled); err != nil || !empty {
			t.Errorf("upstream marker empty/err = %v/%v, want true/nil", empty, err)
		}
		if want == "INSERT INTO db.t FORMAT Native" {
			if _, err := conn.Write(encodeServerSampleDataPacket(t, deferredTestRev)); err != nil {
				return err
			}
			term, err := codec.ReadPacket()
			if err != nil {
				return err
			}
			if empty, err := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); err != nil || !empty {
				t.Errorf("upstream terminator empty/err = %v/%v, staged payload must be withheld", empty, err)
			}
		}
		if _, err := conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return err
		}
	}
	return nil
}

func readServerException(t *testing.T, conn net.Conn) *chproto.Exception {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirToUpstream)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("read server exception: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok {
		t.Fatalf("server packet %d is not an Exception", pkt.Type)
	}
	return exc
}
