package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
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

// Query + external-tables marker + payload + terminator arrive in ONE TCP
// segment (net.Pipe delivers one Write as one Read into the codec's bufio).
func TestRelay_DeferredInsert_CoalescedQueryAndPayloadInOneSegment(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	clientDone.Store(true) // everything is on the wire before the relay reads
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	segment := append(append(append(encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"), empty...), nonEmpty...), empty...)
	writeAllConn(t, h.clientProxy, segment)
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if strictData, strictComplete, _, _, aborts := hooks.counts(); strictData != 1 || strictComplete != 1 || aborts != 0 {
		t.Fatalf("counts strictData/strictComplete/aborts = %d/%d/%d", strictData, strictComplete, aborts)
	}
	h.close(t)
}

// The terminator (and the payload packet) is split across two Writes.
func TestRelay_DeferredInsert_TerminatorSplitAcrossSegments(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty[:3])
	time.Sleep(20 * time.Millisecond)
	writeAllConn(t, h.clientProxy, nonEmpty[3:])
	writeAllConn(t, h.clientProxy, empty[:1])
	clientDone.Store(true)
	time.Sleep(20 * time.Millisecond)
	writeAllConn(t, h.clientProxy, empty[1:])
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if strictData, _, inputCompletes, _, aborts := hooks.counts(); strictData != 1 || inputCompletes != 1 || aborts != 0 {
		t.Fatalf("counts strictData/input/aborts = %d/%d/%d", strictData, inputCompletes, aborts)
	}
	h.close(t)
}

// Upstream answers the deferred Query with an Exception instead of a sample
// block: the Exception reaches the client, no payload is forwarded, the
// query lifecycle is aborted+completed, and the session stays usable.
func TestRelay_DeferredInsert_UpstreamExceptionAtSampleStep(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	upstreamGotPayload := make(chan struct{}, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // external-tables marker
			upDone <- err
			return
		}
		if err := codec.WriteException(&chproto.Exception{Code: 60, Name: "DB::Exception", Message: "Table default.t does not exist"}); err != nil {
			upDone <- err
			return
		}
		upDone <- nil
		if _, err := codec.ReadPacket(); err == nil {
			upstreamGotPayload <- struct{}{}
		}
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || exc.Code != 60 {
		t.Fatalf("client got %#v, want the upstream Exception (code 60)", pkt.Decoded)
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	select {
	case <-upstreamGotPayload:
		t.Fatal("payload must not be forwarded after the upstream rejected the Query")
	case <-time.After(200 * time.Millisecond):
	}
	if _, _, inputCompletes, queryCompletes, aborts := hooks.counts(); inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts input/complete/abort = %d/%d/%d, want 0/1/1", inputCompletes, queryCompletes, aborts)
	}
	// Session still alive: both relay loops must still be running.
	select {
	case err := <-h.loopErr:
		t.Fatalf("relay loop exited after a recoverable upstream Exception: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	h.close(t)
}

// An Exception after the upstream sample has been consumed terminates the
// deferred writer before any later buffered packet is written. The abort hook
// runs before completion/terminal bytes, and the session closes because the
// upstream did not consume the remainder of the INSERT stream.
func TestRelay_DeferredInsert_UpstreamExceptionAfterSampleStopsWriter(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	type upstreamResult struct {
		extraPacket bool
		extraType   uint64
		err         error
	}
	upDone := make(chan upstreamResult, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // external-tables marker
			upDone <- upstreamResult{err: err}
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // first payload packet
			upDone <- upstreamResult{err: err}
			return
		}
		if err := codec.WriteException(&chproto.Exception{Code: 27, Name: "DB::Exception", Message: "cannot parse input"}); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		// Give the relay loop a scheduling turn to observe the terminal packet;
		// any subsequent write after that observation violates the writer gate.
		time.Sleep(20 * time.Millisecond)
		_ = h.upstreamProxy.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		extra, err := codec.ReadPacket()
		result := upstreamResult{extraPacket: err == nil}
		if extra != nil {
			result.extraType = extra.Type
		}
		upDone <- result
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || exc.Code != 27 {
		t.Fatalf("client got %#v, want upstream parse Exception", pkt.Decoded)
	}
	result := <-upDone
	if result.err != nil {
		t.Fatalf("upstream flow: %v", result.err)
	}
	if result.extraPacket {
		t.Fatalf("relay wrote packet type %d after the upstream terminal Exception", result.extraType)
	}
	if _, _, inputCompletes, queryCompletes, aborts := hooks.counts(); inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts input/complete/abort = %d/%d/%d, want 0/1/1", inputCompletes, queryCompletes, aborts)
	}
	h.close(t)
}

func TestRelay_DeferredInsert_RequiresExternalTablesMarkerBeforePayload(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, nonEmpty) // marker deliberately omitted
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !strings.Contains(exc.Message, "external-tables marker") {
		t.Fatalf("client got %#v, want missing-marker Exception", pkt.Decoded)
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes without the required marker, want 0", n)
	}
}

func TestRelay_DeferredInsert_PayloadExactlyAtLimitAllowsTerminator(t *testing.T) {
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	hooks := &deferredInsertHooks{maxPayload: uint64(len(nonEmpty))}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientDone.Store(true)
	terminatorWrite := make(chan error, 1)
	go func() {
		_, err := h.clientProxy.Write(empty)
		terminatorWrite <- err
	}()
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream at exact payload limit", got[0])
	}
	if err := <-terminatorWrite; err != nil {
		t.Fatalf("write terminator: %v", err)
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	h.close(t)
}

// Payload larger than the plan's MaxPayloadBytes: rejected before any byte
// reaches upstream, connection closes fail-closed.
func TestRelay_DeferredInsert_OversizedPayloadIsRejectedBeforeForwarding(t *testing.T) {
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	hooks := &deferredInsertHooks{maxPayload: uint64(len(nonEmpty) - 1)}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	// ReadPacketWithDataLimit intentionally stops before consuming the full
	// oversized packet. Write concurrently so the test can read Housegate's
	// fail-closed Exception while the client upload is still backpressured.
	payloadWrite := make(chan error, 1)
	go func() {
		_, err := h.clientProxy.Write(nonEmpty)
		payloadWrite <- err
	}()
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("exceeds limit")) {
		t.Fatalf("client got %#v, want limit Exception", pkt.Decoded)
	}
	errs := h.close(t)
	<-payloadWrite // may complete when the one-byte overflow is fully buffered
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want 0", n)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
	sawLimit := false
	for _, err := range errs {
		if err != nil && errors.Is(err, chproto.ErrPacketTooLarge) {
			sawLimit = true
		}
	}
	if !sawLimit {
		t.Fatalf("clientToUpstream must return the limit error, got %v", errs)
	}
}

// Cancel mid-payload: local EndOfStream, buffer dropped, nothing upstream,
// session usable for the next query.
func TestRelay_DeferredInsert_CancelMidPayloadDropsBufferAndAnswersEndOfStream(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d after cancel, want EndOfStream", got[0])
	}
	if strictData, _, inputCompletes, queryCompletes, aborts := hooks.counts(); strictData != 1 || inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts strictData/input/complete/abort = %d/%d/%d/%d, want 1/0/1/1", strictData, inputCompletes, queryCompletes, aborts)
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("relay loop exited after a client cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes after cancel, want 0", n)
	}
}

// A plan that also sets SuppressUpstreamExecution is rejected outright.
func TestRelay_DeferredInsert_MutuallyExclusiveWithSuppressUpstreamExecution(t *testing.T) {
	hooks := &deferredInsertHooks{alsoSuppress: true}
	h := newDeferredHarness(t, hooks)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("mutually exclusive")) {
		t.Fatalf("client got %#v, want mutual-exclusion Exception", pkt.Decoded)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
	h.close(t)
}

// A strict data hook error aborts before forwarding and closes the session.
func TestRelay_DeferredInsert_StrictDataErrorAbortsBeforeForwarding(t *testing.T) {
	hooks := &deferredInsertHooks{strictDataErr: errors.New("compressed block rejected")}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("compressed block rejected")) {
		t.Fatalf("client got %#v, want the strict hook error", pkt.Decoded)
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want 0", n)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
}
