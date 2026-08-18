package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

const deferredTestRev = 54453

// deferredInsertHooks marks every Query as a deferred INSERT and records the
// lifecycle hooks Relay fires.
type deferredInsertHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	maxPayload     uint64
	alsoSuppress   bool
	strictDataErr  error
	strictDoneErr  error
	strictDataRaw  [][]byte
	strictComplete int
	inputCompletes int
	queryCompletes int
	queryAborts    int
}

func (h *deferredInsertHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	max := h.maxPayload
	if max == 0 {
		max = 1 << 20
	}
	qctx.DeferredInsert = &plugin.DeferredInsertPlan{
		SampleColumns:   []chproto.SampleColumn{{Name: "v", Type: "UInt64"}},
		MaxPayloadBytes: max,
	}
	qctx.SuppressUpstreamExecution = h.alsoSuppress
	return nil
}

func (h *deferredInsertHooks) OnClientDataStrict(_ context.Context, _ *plugin.QueryContext, raw []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.strictDataErr != nil {
		return h.strictDataErr
	}
	h.strictDataRaw = append(h.strictDataRaw, append([]byte(nil), raw...))
	return nil
}

func (h *deferredInsertHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.strictComplete++
	return h.strictDoneErr
}

func (h *deferredInsertHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.inputCompletes++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.queryCompletes++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.queryAborts++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) counts() (strictData, strictComplete, inputCompletes, queryCompletes, aborts int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.strictDataRaw), h.strictComplete, h.inputCompletes, h.queryCompletes, h.queryAborts
}

// deferredHarness wires a Relay between two net.Pipe pairs with codecs at
// revision deferredTestRev, exactly like the staged-input tests above.
type deferredHarness struct {
	clientProxy, proxyClient     net.Conn
	upstreamProxy, proxyUpstream net.Conn
	relay                        *Relay
	loopErr                      chan error
	cancel                       context.CancelFunc
}

func newDeferredHarness(t *testing.T, hooks plugin.Hooks) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(deferredTestRev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: hooks}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: clientProxy, proxyClient: proxyClient, upstreamProxy: upstreamProxy, proxyUpstream: proxyUpstream, relay: r, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- r.clientToUpstream(ctx) }()
	go func() { h.loopErr <- r.upstreamToClient(ctx) }()
	t.Cleanup(func() {
		cancel()
		clientProxy.Close()
		upstreamProxy.Close()
	})
	return h
}

// close tears the pipes down and returns the two loop results.
func (h *deferredHarness) close(t *testing.T) []error {
	t.Helper()
	h.clientProxy.Close()
	h.upstreamProxy.Close()
	var errs []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			errs = append(errs, err)
		case <-time.After(3 * time.Second):
			t.Fatal("relay loop did not return after close")
		}
	}
	return errs
}

func encodeInsertQuery(t *testing.T, id, sql string) []byte {
	t.Helper()
	var qb proto.Buffer
	(&proto.Query{
		ID: id, Body: sql,
		Info: proto.ClientInfo{
			ProtocolVersion: deferredTestRev, Major: 24, Minor: 1,
			Interface: proto.InterfaceTCP,
			Query:     proto.ClientQueryInitial,
		},
	}).EncodeAware(&qb, deferredTestRev)
	return append([]byte(nil), qb.Buf...)
}

func encodeEmptyClientData(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	c := chproto.NewCodec(&readWriter{r: &bytes.Buffer{}, w: &buf}, chproto.DirFromClient)
	c.SetCompression(proto.CompressionDisabled)
	if err := c.WriteEmptyDataBlock(); err != nil {
		t.Fatalf("WriteEmptyDataBlock: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

type readWriter struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (rw *readWriter) Read(p []byte) (int, error)  { return rw.r.Read(p) }
func (rw *readWriter) Write(p []byte) (int, error) { return rw.w.Write(p) }

func readExact(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return buf
}

func writeAllConn(t *testing.T, c net.Conn, b []byte) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write(b); err != nil {
		t.Fatalf("write %d bytes: %v", len(b), err)
	}
}

// upstreamAcceptsDeferredInsert plays a ClickHouse that receives Query,
// external-tables terminator, answers the sample block, receives payload +
// terminator, and answers EndOfStream. clientDone must be set by the client
// before the terminator was sent so the test proves the Query was deferred.
func upstreamAcceptsDeferredInsert(t *testing.T, conn net.Conn, clientDone *atomic.Bool, wantPayload []byte, done chan<- error) {
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		done <- err
		return
	}
	if !clientDone.Load() {
		t.Errorf("upstream received the Query before the client finished sending its payload")
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "INSERT INTO t FORMAT Native" {
		t.Errorf("upstream query = %#v", pkt.Decoded)
	}
	first, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if empty, _ := chproto.ClientDataPacketIsEmpty(first.Raw, proto.CompressionDisabled); !empty {
		t.Errorf("upstream expected the external-tables terminator first, got %x", first.Raw)
	}
	if _, err := conn.Write(encodeServerSampleDataPacket(t, deferredTestRev)); err != nil {
		done <- err
		return
	}
	data, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if !bytes.Equal(data.Raw, wantPayload) {
		t.Errorf("upstream payload = %x, want %x", data.Raw, wantPayload)
	}
	term, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if empty, _ := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); !empty {
		t.Errorf("upstream expected terminator, got %x", term.Raw)
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	done <- err
}

func TestRelay_DeferredInsert_HappyPathAnswersSampleLocallyAndForwardsAfterTerminator(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	// net.Pipe writes block until read, so the test client reads the locally
	// answered sample block BEFORE it sends the external-tables marker (a real
	// TCP client sends the marker immediately; the relay accepts either order).
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, empty) // end of external tables
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientDone.Store(true)
	writeAllConn(t, h.clientProxy, empty) // terminator
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got server packet %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	strictData, strictComplete, inputCompletes, queryCompletes, aborts := hooks.counts()
	if strictData != 1 || !bytes.Equal(hooks.strictDataRaw[0], nonEmpty) {
		t.Fatalf("strict data captured %d packets, want exactly the payload", strictData)
	}
	if strictComplete != 1 || inputCompletes != 1 || queryCompletes != 1 || aborts != 0 {
		t.Fatalf("hooks strictComplete/input/complete/abort = %d/%d/%d/%d, want 1/1/1/0", strictComplete, inputCompletes, queryCompletes, aborts)
	}
	for _, err := range h.close(t) {
		if err != nil && !errors.Is(err, io.EOF) {
			t.Logf("relay loop returned: %v", err)
		}
	}
}
