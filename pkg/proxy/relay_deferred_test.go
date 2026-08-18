package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
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
	mutateCompress bool
	strictDataErr  error
	strictDoneErr  error
	strictDataRaw  [][]byte
	strictComplete int
	inputCompletes int
	queryCompletes int
	queryAborts    int
	querySuccesses int
	lifecycle      []string
	inputDone      chan struct{}
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
	if h.mutateCompress {
		qctx.Query.Compression = proto.CompressionEnabled
	}
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
	if h.inputDone != nil {
		select {
		case h.inputDone <- struct{}{}:
		default:
		}
	}
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.queryCompletes++
	h.lifecycle = append(h.lifecycle, "complete")
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.queryAborts++
	h.lifecycle = append(h.lifecycle, "abort")
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	h.mu.Lock()
	h.querySuccesses++
	h.lifecycle = append(h.lifecycle, "success")
	h.mu.Unlock()
}

func (h *deferredInsertHooks) counts() (strictData, strictComplete, inputCompletes, queryCompletes, aborts int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.strictDataRaw), h.strictComplete, h.inputCompletes, h.queryCompletes, h.queryAborts
}

func (h *deferredInsertHooks) terminalCounts() (successes int, lifecycle []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.querySuccesses, append([]string(nil), h.lifecycle...)
}

type firstDeferredInsertHooks struct {
	*deferredInsertHooks
	queries atomic.Int32
}

func (h *firstDeferredInsertHooks) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if h.queries.Add(1) == 1 {
		return h.deferredInsertHooks.OnQuery(ctx, qctx)
	}
	return nil
}

type blockingSuccessDeferredInsertHooks struct {
	*deferredInsertHooks
	successEntered chan struct{}
	releaseSuccess chan struct{}
}

func (h *blockingSuccessDeferredInsertHooks) OnQuerySuccess(ctx context.Context, sess chsession.Session, queryID string) {
	h.deferredInsertHooks.OnQuerySuccess(ctx, sess, queryID)
	close(h.successEntered)
	<-h.releaseSuccess
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

var errDeferredClientWrite = errors.New("injected deferred client write failure")

type failDeferredClientWriteConn struct {
	net.Conn
	fail atomic.Bool
}

type blockDeferredClientWriteConn struct {
	net.Conn
	arm     atomic.Bool
	wrote   chan struct{}
	release chan struct{}
}

func (c *blockDeferredClientWriteConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && c.arm.CompareAndSwap(true, false) {
		close(c.wrote)
		<-c.release
	}
	return n, err
}

func (c *failDeferredClientWriteConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, errDeferredClientWrite
	}
	return c.Conn.Write(p)
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

func newDeferredWriteFailHarness(t *testing.T, hooks plugin.Hooks) (*deferredHarness, *failDeferredClientWriteConn) {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	clientConn := &failDeferredClientWriteConn{Conn: proxyClient}
	sess := chsession.New(1, clientConn)
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
		_ = clientProxy.Close()
		_ = upstreamProxy.Close()
	})
	return h, clientConn
}

func newDeferredWriteBlockHarness(t *testing.T, hooks plugin.Hooks) (*deferredHarness, *blockDeferredClientWriteConn) {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	clientConn := &blockDeferredClientWriteConn{
		Conn:    proxyClient,
		wrote:   make(chan struct{}),
		release: make(chan struct{}),
	}
	sess := chsession.New(1, clientConn)
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
		_ = clientProxy.Close()
		_ = upstreamProxy.Close()
		select {
		case <-clientConn.release:
		default:
			close(clientConn.release)
		}
	})
	return h, clientConn
}

func tcpConnPair(t *testing.T) (peer, relay net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accept := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accept <- conn
	}()
	relay, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	select {
	case peer = <-accept:
	case err := <-acceptErr:
		_ = relay.Close()
		_ = ln.Close()
		t.Fatal(err)
	}
	_ = ln.Close()
	return peer, relay
}

func newDeferredTCPHarness(t *testing.T, hooks plugin.Hooks) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := tcpConnPair(t)
	upstreamProxy, proxyUpstream := tcpConnPair(t)
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
		_ = clientProxy.Close()
		_ = upstreamProxy.Close()
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
func upstreamAcceptsDeferredInsert(t *testing.T, conn net.Conn, clientDone *atomic.Bool, wantPayload []byte, inputDone <-chan struct{}, done chan<- error) {
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
	select {
	case <-inputDone:
	case <-time.After(time.Second):
		done <- errors.New("relay did not publish terminator completion")
		return
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	done <- err
}

func TestRelay_DeferredInsert_HappyPathAnswersSampleLocallyAndForwardsAfterTerminator(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)

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
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	clientDone.Store(true) // everything is on the wire before the relay reads
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)

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

func TestRelay_DeferredInsert_RestoresClientCompressionAfterPluginMutation(t *testing.T) {
	hooks := &deferredInsertHooks{mutateCompress: true, inputDone: make(chan struct{}, 1)}
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
		q, ok := pkt.Decoded.(*chproto.Query)
		if !ok || q.Compression != proto.CompressionDisabled {
			upDone <- fmt.Errorf("upstream Query compression = %v (%T), want original disabled", q.Compression, pkt.Decoded)
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		select {
		case <-hooks.inputDone:
		case <-time.After(time.Second):
			upDone <- errors.New("relay did not publish terminator completion")
			return
		}
		_, err = h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	readExact(t, h.clientProxy, 1)
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	h.close(t)
}

// The terminator (and the payload packet) is split across two Writes.
func TestRelay_DeferredInsert_TerminatorSplitAcrossSegments(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)

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
		deadline := time.Now().Add(time.Second)
		for {
			gate := h.relay.currentDeferredInput()
			if gate != nil && gate.markerWritten() {
				break
			}
			if time.Now().After(deadline) {
				upDone <- errors.New("relay did not publish marker completion")
				return
			}
			time.Sleep(time.Millisecond)
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

func TestRelay_DeferredInsert_SampleExceptionBeforeMarkerWriteDoesNotDeadlock(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		// Never read the external-tables marker. The relay's marker Write is
		// backpressured when this Exception races in immediately after Query.
		upDone <- codec.WriteException(&chproto.Exception{Code: 60, Name: "DB::Exception", Message: "rejected before marker"})
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("sample Exception deadlocked behind marker write: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || exc.Code != 60 {
		t.Fatalf("client got %#v, want sample Exception", pkt.Decoded)
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-h.loopErr:
		if err == nil {
			t.Fatal("incomplete marker write closed relay with nil error")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sample Exception left relay loops deadlocked")
	}
	if _, _, inputCompletes, completes, aborts := hooks.counts(); inputCompletes != 0 || completes != 1 || aborts != 1 {
		t.Fatalf("input/complete/abort = %d/%d/%d, want 0/1/1", inputCompletes, completes, aborts)
	}
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

func TestRelay_DeferredInsert_PrematureEOSStopsBackpressuredWriter(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		// Send sample+EOS without reading the payload. net.Pipe backpressures the
		// relay's first payload write, so EOS must actively stop that writer.
		_, err := h.upstreamProxy.Write(append(append([]byte(nil), sample...), byte(chproto.ServerEndOfStreamCode)))
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)

	var errs []error
	for len(errs) < 2 {
		select {
		case err := <-h.loopErr:
			errs = append(errs, err)
			if len(errs) == 1 {
				_ = h.clientProxy.Close()
			}
		case <-time.After(time.Second):
			t.Fatal("premature EOS deadlocked the deferred writer")
		}
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if _, _, inputCompletes, queryCompletes, aborts := hooks.counts(); inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("hooks input/complete/abort = %d/%d/%d, want 0/1/1", inputCompletes, queryCompletes, aborts)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_EOSObservedBeforeTerminatorWriteIsPermanentlyFatal(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	eosDecoded := make(chan struct{})
	checkTerminator := make(chan struct{})
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // payload
			upDone <- err
			return
		}
		first := make([]byte, 1)
		if _, err := io.ReadFull(h.upstreamProxy, first); err != nil {
			upDone <- err
			return
		}
		// The EOS Write returning proves upstreamToClient decoded the EOS while
		// the terminator writer was still blocked on its remaining bytes.
		if _, err := h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			upDone <- err
			return
		}
		close(eosDecoded)
		<-checkTerminator
		_ = h.upstreamProxy.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		rest := make([]byte, len(empty)-1)
		_, err := io.ReadFull(h.upstreamProxy, rest)
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	go func() { _, _ = h.clientProxy.Write(empty) }()
	select {
	case <-eosDecoded:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("relay did not read premature EOS")
	}
	select {
	case err := <-h.loopErr:
		if err == nil {
			t.Fatal("premature EOS closed relay with nil error")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("premature EOS was allowed to become success after terminator write completed")
	}
	close(checkTerminator)
	_ = h.clientProxy.Close()
	if err := <-upDone; err == nil {
		t.Fatal("upstream unexpectedly read the rest of a terminator after premature EOS")
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_PostTerminatorEOSAllowsSuccessWithKernelBufferedWriter(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredTCPHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		select {
		case <-hooks.inputDone:
		case <-time.After(time.Second):
			upDone <- errors.New("relay did not finish writing terminator")
			return
		}
		// The server intentionally has not read payload bytes; successful local
		// writes sit in the TCP send buffer. EOS is nevertheless post-terminator
		// from Relay's observable ordering and is the only EOS success case.
		_, err := h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 1 || !slices.Equal(lifecycle, []string{"success", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 1/[success complete]", successes, lifecycle)
	}
	h.close(t)
}

func TestRelay_DeferredInsert_NextQueryArrivingAfterTerminalBytesIsPrefetched(t *testing.T) {
	baseHooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	hooks := &firstDeferredInsertHooks{deferredInsertHooks: baseHooks}
	h, clientConn := newDeferredWriteBlockHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	upDone := make(chan error, 1)

	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // marker
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // payload
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // terminator
			upDone <- err
			return
		}
		select {
		case <-baseHooks.inputDone:
		case <-time.After(time.Second):
			upDone <- errors.New("relay did not publish terminator completion")
			return
		}
		clientConn.arm.Store(true)
		if _, err := h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			upDone <- err
			return
		}
		next, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			upDone <- err
			return
		}
		q, ok := next.Decoded.(*chproto.Query)
		if !ok || q.ID != "qid-next" {
			upDone <- fmt.Errorf("next upstream packet = %#v", next.Decoded)
			return
		}
		_, err = h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want first EndOfStream", got[0])
	}
	select {
	case <-clientConn.wrote:
	case <-time.After(time.Second):
		t.Fatal("client-facing terminal write did not reach its completion barrier")
	}
	nextWrite := make(chan error, 1)
	go func() {
		_, err := h.clientProxy.Write(encodeInsertQuery(t, "qid-next", "SELECT 1"))
		nextWrite <- err
	}()
	select {
	case err := <-nextWrite:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("next Query was not consumed while terminal delivery was settling")
	}
	close(clientConn.release)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want second EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	if successes, lifecycle := baseHooks.terminalCounts(); successes != 2 || !slices.Equal(lifecycle, []string{"success", "complete", "success", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 2/[success complete success complete]", successes, lifecycle)
	}
	h.close(t)
}

func TestRelay_DeferredInsert_PostTerminatorClientCancelOrEOFStopsHungUpstream(t *testing.T) {
	for _, tc := range []struct {
		name string
		eof  bool
	}{
		{name: "cancel"},
		{name: "eof", eof: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := &deferredInsertHooks{}
			h := newDeferredHarness(t, hooks)
			nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
			sample := encodeServerSampleDataPacket(t, deferredTestRev)
			empty := encodeEmptyClientData(t)
			terminatorRead := make(chan struct{})
			upDone := make(chan error, 1)

			go func() {
				codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
				codec.SetRevision(deferredTestRev)
				codec.SetCompression(proto.CompressionDisabled)
				if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
					upDone <- err
					return
				}
				if _, err := codec.ReadPacket(); err != nil { // marker
					upDone <- err
					return
				}
				if _, err := h.upstreamProxy.Write(sample); err != nil {
					upDone <- err
					return
				}
				if _, err := codec.ReadPacket(); err != nil { // payload
					upDone <- err
					return
				}
				if _, err := codec.ReadPacket(); err != nil { // terminator
					upDone <- err
					return
				}
				close(terminatorRead)
				_, err := codec.ReadPacket() // blocks until Relay disposes upstream
				upDone <- err
			}()

			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
			readExact(t, h.clientProxy, len(sample))
			writeAllConn(t, h.clientProxy, empty)
			writeAllConn(t, h.clientProxy, nonEmpty)
			writeAllConn(t, h.clientProxy, empty)
			select {
			case <-terminatorRead:
			case <-time.After(time.Second):
				t.Fatal("upstream did not receive terminator")
			}
			if tc.eof {
				_ = h.clientProxy.Close()
			} else {
				writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
			}

			for i := 0; i < 2; i++ {
				select {
				case err := <-h.loopErr:
					if err == nil {
						t.Fatal("client termination closed relay with nil error")
					}
				case <-time.After(500 * time.Millisecond):
					t.Fatal("client termination left deferred INSERT goroutines parked")
				}
			}
			if err := <-upDone; err == nil {
				t.Fatal("hung upstream was not closed")
			}
			if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
				t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
			}
		})
	}
}

func TestRelay_DeferredInsert_PostSampleForwardFailureAbortsExactlyOnce(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h, clientConn := newDeferredWriteFailHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	terminatorRead := make(chan struct{})
	sendProgress := make(chan struct{})
	upDone := make(chan error, 1)

	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // marker
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // payload
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // terminator
			upDone <- err
			return
		}
		close(terminatorRead)
		<-sendProgress
		var progress proto.Buffer
		progress.PutUVarInt(uint64(chproto.ServerProgressCode))
		(proto.Progress{Rows: 1}).EncodeAware(&progress, deferredTestRev)
		_, err := h.upstreamProxy.Write(progress.Buf)
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	select {
	case <-terminatorRead:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive terminator")
	}
	clientConn.fail.Store(true)
	close(sendProgress)

	var loopErrs []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			loopErrs = append(loopErrs, err)
			if err == nil {
				t.Fatal("forward failure closed relay with nil error")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("post-sample forward failure left relay goroutines parked after %v", loopErrs)
		}
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream progress write: %v", err)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_PreSampleForwardFailureUnblocksWriter(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h, clientConn := newDeferredWriteFailHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	markerRead := make(chan struct{})
	sendProgress := make(chan struct{})
	upDone := make(chan error, 1)

	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // marker
			upDone <- err
			return
		}
		close(markerRead)
		<-sendProgress
		var progress proto.Buffer
		progress.PutUVarInt(uint64(chproto.ServerProgressCode))
		(proto.Progress{Rows: 1}).EncodeAware(&progress, deferredTestRev)
		_, err := h.upstreamProxy.Write(progress.Buf)
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(encodeServerSampleDataPacket(t, deferredTestRev)))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, empty)
	select {
	case <-markerRead:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive marker")
	}
	clientConn.fail.Store(true)
	close(sendProgress)

	var loopErrs []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			loopErrs = append(loopErrs, err)
			if err == nil {
				t.Fatal("pre-sample forward failure closed relay with nil error")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("pre-sample forward failure left relay goroutines parked after %v", loopErrs)
		}
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream progress write: %v", err)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_PreSampleClientEOFStopsHungUpstream(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	markerRead := make(chan struct{})
	upDone := make(chan error, 1)

	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // marker
			upDone <- err
			return
		}
		close(markerRead)
		_, err := codec.ReadPacket() // blocks until Relay disposes upstream
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(encodeServerSampleDataPacket(t, deferredTestRev)))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	select {
	case <-markerRead:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive marker")
	}
	_ = h.clientProxy.Close()

	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			if err == nil {
				t.Fatal("pre-sample client EOF closed relay with nil error")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("pre-sample client EOF left relay goroutines parked")
		}
	}
	if err := <-upDone; err == nil {
		t.Fatal("hung upstream was not closed")
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_EOSOwnsLifecycleBeforeSuccessHooks(t *testing.T) {
	baseHooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	hooks := &blockingSuccessDeferredInsertHooks{
		deferredInsertHooks: baseHooks,
		successEntered:      make(chan struct{}),
		releaseSuccess:      make(chan struct{}),
	}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	upDone := make(chan error, 1)
	var clientDone atomic.Bool
	clientDone.Store(true)

	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(encodeServerSampleDataPacket(t, deferredTestRev)))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	select {
	case <-hooks.successEntered:
	case <-time.After(time.Second):
		t.Fatal("upstream EOS did not enter success hook")
	}
	// The EOS decision already owns the lifecycle. A client disconnect in the
	// success-hook window must not race in a contradictory abort.
	_ = h.clientProxy.Close()
	close(hooks.releaseSuccess)

	for i := 0; i < 2; i++ {
		select {
		case <-h.loopErr:
		case <-time.After(time.Second):
			t.Fatal("terminal ownership race left relay goroutines parked")
		}
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream EOS write: %v", err)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 1 || !slices.Equal(lifecycle, []string{"success", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 1/[success complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_LateExceptionClosesKernelBufferedSession(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredTCPHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	type upstreamResult struct {
		sawNextQuery bool
		err          error
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
		if _, err := codec.ReadPacket(); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		select {
		case <-hooks.inputDone:
		case <-time.After(time.Second):
			upDone <- upstreamResult{err: errors.New("relay did not finish queueing payload")}
			return
		}
		if err := codec.WriteException(&chproto.Exception{Code: 27, Name: "DB::Exception", Message: "late payload parse failure"}); err != nil {
			upDone <- upstreamResult{err: err}
			return
		}
		_ = h.upstreamProxy.SetReadDeadline(time.Now().Add(time.Second))
		for {
			pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
			if err != nil {
				upDone <- upstreamResult{}
				return
			}
			if pkt.Type == uint64(chproto.ClientQueryCode) {
				upDone <- upstreamResult{sawNextQuery: true}
				return
			}
		}
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	if pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode)); err != nil {
		t.Fatalf("client read Exception: %v", err)
	} else if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || exc.Code != 27 {
		t.Fatalf("client got %#v, want late Exception", pkt.Decoded)
	}
	_ = h.clientProxy.SetReadDeadline(time.Time{})
	_, _ = h.clientProxy.Write(encodeInsertQuery(t, "qid-next", "INSERT INTO t FORMAT Native"))
	select {
	case err := <-h.loopErr:
		if err == nil {
			t.Fatal("late Exception closed relay with nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("late post-sample Exception left the deferred session reusable")
	}
	result := <-upDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.sawNextQuery {
		t.Fatal("next Query crossed an upstream polluted by queued INSERT data")
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
	if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
		t.Fatalf("successes/lifecycle = %d/%v, want 0/[abort complete]", successes, lifecycle)
	}
}

func TestRelay_DeferredInsert_UpstreamReadErrorOrdersAbortBeforeSingleComplete100x(t *testing.T) {
	for i := 0; i < 100; i++ {
		hooks := &deferredInsertHooks{}
		h := newDeferredHarness(t, hooks)
		nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
		sample := encodeServerSampleDataPacket(t, deferredTestRev)
		empty := encodeEmptyClientData(t)
		upDone := make(chan error, 1)
		go func() {
			codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
			codec.SetRevision(deferredTestRev)
			codec.SetCompression(proto.CompressionDisabled)
			if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
				upDone <- err
				return
			}
			if _, err := codec.ReadPacket(); err != nil {
				upDone <- err
				return
			}
			if _, err := h.upstreamProxy.Write(sample); err != nil {
				upDone <- err
				return
			}
			upDone <- h.upstreamProxy.Close()
		}()

		writeAllConn(t, h.clientProxy, encodeInsertQuery(t, fmt.Sprintf("qid-%d", i), "INSERT INTO t FORMAT Native"))
		readExact(t, h.clientProxy, len(sample))
		writeAllConn(t, h.clientProxy, empty)
		writeAllConn(t, h.clientProxy, nonEmpty)
		writeAllConn(t, h.clientProxy, empty)
		if err := <-upDone; err != nil {
			t.Fatalf("iteration %d upstream: %v", i, err)
		}
		for n := 0; n < 2; n++ {
			select {
			case <-h.loopErr:
				if n == 0 {
					_ = h.clientProxy.Close()
				}
			case <-time.After(time.Second):
				_ = h.clientProxy.Close()
				t.Fatalf("iteration %d relay loop did not exit", i)
			}
		}
		if successes, lifecycle := hooks.terminalCounts(); successes != 0 || !slices.Equal(lifecycle, []string{"abort", "complete"}) {
			t.Fatalf("iteration %d successes/lifecycle = %d/%v, want 0/[abort complete]", i, successes, lifecycle)
		}
		_ = h.clientProxy.Close()
	}
}

func TestRelay_DeferredInsert_AcceptsPayloadWithoutExternalTablesMarker(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, nonEmpty) // external-tables marker deliberately omitted
	clientDone.Store(true)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	h.close(t)
	if strictData, strictComplete, inputCompletes, queryCompletes, aborts := hooks.counts(); strictData != 1 || strictComplete != 1 || inputCompletes != 1 || queryCompletes != 1 || aborts != 0 {
		t.Fatalf("hooks data/strict/input/complete/abort = %d/%d/%d/%d/%d, want 1/1/1/1/0", strictData, strictComplete, inputCompletes, queryCompletes, aborts)
	}
}

func TestRelay_DeferredInsert_SlowPayloadAfterMarkerUsesProtocolStateNotClock(t *testing.T) {
	hooks := &deferredInsertHooks{inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid-slow", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	time.Sleep(350 * time.Millisecond) // exceeds the removed 250ms heuristic
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientDone.Store(true)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	h.close(t)
}

func TestRelay_DeferredInsert_ZeroRowsWithMarkerRejectsDeterministically(t *testing.T) {
	hooks := &deferredInsertHooks{strictDoneErr: errors.New("SI payload must contain non-empty Data bytes")}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid-zero", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatal("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty) // external-tables marker
	writeAllConn(t, h.clientProxy, empty) // zero-row terminator
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || !strings.Contains(exc.Message, "non-empty Data") {
		t.Fatalf("client got %#v, want zero-payload rejection", pkt.Decoded)
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes for rejected zero-row INSERT", n)
	}
}

func TestRelay_DeferredInsert_PayloadExactlyAtLimitAllowsTerminator(t *testing.T) {
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	hooks := &deferredInsertHooks{maxPayload: uint64(len(nonEmpty)), inputDone: make(chan struct{}, 1)}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, hooks.inputDone, upDone)
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
