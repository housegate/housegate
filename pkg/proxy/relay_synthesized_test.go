package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
)

const synthesizedBody = "INSERT INTO db.t (v) FORMAT Native"

// synthesizedInsertHooks installs a SynthesizedInsert plan on every Query and
// counts the lifecycle hooks Relay fires.
type synthesizedInsertHooks struct {
	plugin.NoopHooks
	mu              sync.Mutex
	events          map[string]int
	limit           uint64
	enforce         bool
	alsoDefer       bool
	conflict        string
	strictErr       error
	blocks          [][]proto.InputColumn
	sample          []chproto.SampleColumn
	skipPlanFor     string // a Query body that must stay on the ordinary path
	payloadAtStrict []byte
	strictRan       atomic.Bool
	inputDone       chan struct{}
}

func newSynthesizedHooks() *synthesizedInsertHooks {
	return &synthesizedInsertHooks{events: map[string]int{}, inputDone: make(chan struct{}, 1)}
}

func (h *synthesizedInsertHooks) bump(name string) { h.mu.Lock(); h.events[name]++; h.mu.Unlock() }

func (h *synthesizedInsertHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if h.skipPlanFor != "" && qctx.Query.Body == h.skipPlanFor {
		return nil
	}
	cols := []chproto.SampleColumn{{Name: "v", Type: "UInt64"}}
	qctx.SynthesizedInsert = &plugin.SynthesizedInsertPlan{
		Blocks:        [][]proto.InputColumn{synthesizedBlock()},
		SampleColumns: cols,
		Rows:          3,
	}
	if h.blocks != nil {
		qctx.SynthesizedInsert.Blocks = h.blocks
	}
	if h.sample != nil {
		qctx.SynthesizedInsert.SampleColumns = h.sample
	}
	switch h.conflict {
	case "Deferred":
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{}
	case "Suppress":
		qctx.SuppressUpstreamExecution = true
	case "AgentPrepare":
		qctx.AgentPrepare = &plugin.AgentPreparePlan{}
	case "QueryOnly":
		qctx.QueryOnly = &plugin.QueryOnlyPlan{}
	case "AbortWithSuccess":
		qctx.AbortWithSuccess = true
	}
	if h.alsoDefer {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: 1 << 20}
	}
	return nil
}

func (h *synthesizedInsertHooks) ClientDataReadLimit(qctx *plugin.QueryContext) (uint64, bool) {
	if qctx == nil || qctx.SynthesizedInsert == nil {
		return 0, false
	}
	return h.limit, h.enforce
}

func (h *synthesizedInsertHooks) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	h.events["strict"]++
	if qctx.SynthesizedInsert != nil {
		h.payloadAtStrict = qctx.SynthesizedInsert.Payload()
	}
	h.mu.Unlock()
	h.strictRan.Store(true)
	if qctx.SynthesizedInsert != nil {
		return h.strictErr
	}
	return nil
}

func (h *synthesizedInsertHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	h.bump("input")
	select {
	case h.inputDone <- struct{}{}:
	default:
	}
}

func (h *synthesizedInsertHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.bump("complete")
}

func (h *synthesizedInsertHooks) OnQueryAbort(context.Context, *plugin.QueryContext) { h.bump("abort") }

func (h *synthesizedInsertHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	h.bump("success")
}

func (h *synthesizedInsertHooks) counts() (strict, inputs, completes, aborts, successes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.events["strict"], h.events["input"], h.events["complete"], h.events["abort"], h.events["success"]
}

func synthesizedBlock() []proto.InputColumn {
	values := proto.ColUInt64{1, 2, 3}
	return []proto.InputColumn{{Name: "v", Data: &values}}
}

func synthesizedPacket(t *testing.T, rev int) []byte {
	t.Helper()
	raw, err := nativepayload.EncodeClientDataPacket(rev, synthesizedBlock())
	if err != nil {
		t.Fatalf("EncodeClientDataPacket: %v", err)
	}
	return raw
}

func encodeServerSampleNamed(t *testing.T, rev int, name string) []byte {
	t.Helper()
	values := proto.ColUInt64{}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 0, Columns: 1}).EncodeBlock(&buf, rev, proto.Input{{Name: name, Data: &values}}); err != nil {
		t.Fatalf("encode sample: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func encodeServerExceptionPacket(rev int, code int32, message string) []byte {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeException))
	(&chproto.Exception{Code: proto.Error(code), Name: "DB::Exception", Message: message}).EncodeAware(&buf, rev)
	return append([]byte(nil), buf.Buf...)
}

// encodeTableColumnsPacket is what ClickHouse sends ahead of the sample when
// input_format_defaults_for_omitted_fields is on: table name + columns
// description, both plain strings below revision 54481.
func encodeTableColumnsPacket(name, description string) []byte {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeTableColumns))
	buf.PutString(name)
	buf.PutString(description)
	return append([]byte(nil), buf.Buf...)
}

func newSynthesizedHarness(t *testing.T, hooks plugin.Hooks, rev int, chunked, tcp bool, clientChunked ...bool) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	if tcp {
		clientProxy, proxyClient = tcpConnPair(t)
		upstreamProxy, proxyUpstream = tcpConnPair(t)
	}
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(rev)
	if len(clientChunked) > 0 && clientChunked[0] {
		sess.Client().EnableChunked(true, true)
	}
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(rev)
	if chunked {
		up.EnableChunked(true, true)
	}
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: hooks}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: clientProxy, proxyClient: proxyClient, upstreamProxy: upstreamProxy, proxyUpstream: proxyUpstream, relay: r, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- r.clientToUpstream(ctx) }()
	go func() { h.loopErr <- r.upstreamToClient(ctx) }()
	t.Cleanup(func() { cancel(); clientProxy.Close(); upstreamProxy.Close() })
	return h
}

// synthUpstream plays a ClickHouse: it reads Query + exactly one empty marker,
// asserts the strict hook already ran, then runs stage.
func synthUpstream(t *testing.T, conn net.Conn, rev int, chunked bool, strictRan *atomic.Bool, stage func(up *chproto.Codec) error, done chan<- error) {
	up := chproto.NewCodec(conn, chproto.DirFromClient)
	up.SetRevision(rev)
	up.SetCompression(proto.CompressionDisabled)
	if chunked {
		up.EnableChunked(true, true)
	}
	done <- func() error {
		pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			return fmt.Errorf("read query: %w", err)
		}
		if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != synthesizedBody {
			return fmt.Errorf("upstream query = %#v, want body %q", pkt.Decoded, synthesizedBody)
		}
		if !strictRan.Load() {
			return errors.New("Query reached upstream before OnQueryInputCompleteStrict")
		}
		marker, err := up.ReadPacket()
		if err != nil {
			return fmt.Errorf("read marker: %w", err)
		}
		if empty, err := chproto.ClientDataPacketIsEmpty(marker.Raw, proto.CompressionDisabled); err != nil || !empty {
			return fmt.Errorf("first packet after Query is not the empty marker (empty=%v err=%v)", empty, err)
		}
		return stage(up)
	}()
}

// readPayloadAndTerminator asserts the lane wrote the plan's packets and then
// exactly one empty terminator.
func readPayloadAndTerminator(up *chproto.Codec, want []byte) error {
	data, err := up.ReadPacket()
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	if !bytes.Equal(data.Raw, want) {
		return fmt.Errorf("upstream payload = %x, want %x", data.Raw, want)
	}
	term, err := up.ReadPacket()
	if err != nil {
		return fmt.Errorf("read terminator: %w", err)
	}
	if empty, err := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); err != nil || !empty {
		return fmt.Errorf("terminator is not an empty block (empty=%v err=%v)", empty, err)
	}
	return nil
}

func TestRelay_SynthesizedInsert_ForwardsPlanPayloadAfterSampleGate(t *testing.T) {
	for _, tc := range []struct {
		name                string
		rev                 int
		chunked, tcp, split bool
	}{
		{"non-chunked upstream leg", deferredTestRev, false, false, false},
		{"chunked upstream leg", 54470, true, true, false},
		{"tcp: query and marker coalesced in one segment", deferredTestRev, false, true, false},
		{"tcp: marker fragmented across segments", deferredTestRev, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, tc.rev, tc.chunked, tc.tcp)
			payload := synthesizedPacket(t, tc.rev)
			upDone := make(chan error, 1)
			t.Cleanup(func() {
				if t.Failed() {
					select {
					case err := <-upDone:
						t.Logf("upstream failure: %v", err)
					default:
					}
					select {
					case err := <-h.loopErr:
						t.Logf("relay failure: %v", err)
					default:
					}
				}
			})
			go synthUpstream(t, h.upstreamProxy, tc.rev, tc.chunked, &hooks.strictRan, func(up *chproto.Codec) error {
				if err := up.WriteRawPacket(encodeTableColumnsPacket("t", "v UInt64")); err != nil {
					return err
				}
				if err := up.WriteRawPacket(encodeServerSampleNamed(t, tc.rev, "v")); err != nil {
					return err
				}
				if err := readPayloadAndTerminator(up, payload); err != nil {
					return err
				}
				select {
				case <-hooks.inputDone:
				case <-time.After(time.Second):
					return errors.New("OnQueryInputComplete did not fire within 1s")
				}
				return up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)})
			}, upDone)

			query, marker := encodeSynthesizedQuery(tc.rev, "qid", synthesizedBody), encodeEmptyClientData(t)
			switch {
			case tc.split:
				writeAllConn(t, h.clientProxy, query)
				for i := range marker {
					writeAllConn(t, h.clientProxy, marker[i:i+1])
				}
			case tc.tcp:
				writeAllConn(t, h.clientProxy, append(append([]byte(nil), query...), marker...))
			default:
				writeAllConn(t, h.clientProxy, query)
				writeAllConn(t, h.clientProxy, marker)
			}
			if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
				t.Fatalf("client got server packet %d, want EndOfStream", got[0])
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
			strict, inputs, completes, aborts, successes := hooks.counts()
			if strict != 1 || inputs != 1 || completes != 1 || aborts != 0 || successes != 1 {
				t.Fatalf("hooks strict/input/complete/abort/success = %d/%d/%d/%d/%d, want 1/1/1/0/1",
					strict, inputs, completes, aborts, successes)
			}
			if !bytes.Equal(hooks.payloadAtStrict, payload) {
				t.Fatalf("payload hashed at the strict hook = %x, want %x", hooks.payloadAtStrict, payload)
			}
			for _, err := range h.close(t) {
				if err != nil && !errors.Is(err, io.EOF) {
					t.Logf("relay loop returned: %v", err)
				}
			}
		})
	}
}

func encodeSynthesizedQuery(revision int, id, body string) []byte {
	var b proto.Buffer
	(&proto.Query{ID: id, Body: body, Info: proto.ClientInfo{ProtocolVersion: revision, Major: 26, Minor: 3, Interface: proto.InterfaceTCP, Query: proto.ClientQueryInitial}}).EncodeAware(&b, revision)
	return b.Buf
}

func readSomeConn(t *testing.T, c net.Conn) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return buf[:n]
}

func assertNoUpstreamBytes(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := c.Read(buf); err == nil || n > 0 {
		t.Fatalf("upstream received %d bytes (%x), want nothing", n, buf[:n])
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Logf("upstream read ended with %v", err)
	}
}

func TestRelay_SynthesizedInsert_ClientFailuresBeforeUpstream(t *testing.T) {
	for _, tc := range []struct {
		name    string
		send    func(t *testing.T, h *deferredHarness)
		wantEOS bool
		wantExc bool
	}{
		{"marker never arrives, client disconnects", func(t *testing.T, h *deferredHarness) { h.clientProxy.Close() }, false, false},
		{"marker is a payload block", func(t *testing.T, h *deferredHarness) {
			writeAllConn(t, h.clientProxy, synthesizedPacket(t, deferredTestRev))
		}, false, true},
		{"cancel before the upstream query", func(t *testing.T, h *deferredHarness) {
			writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			tc.send(t, h)
			switch {
			case tc.wantEOS:
				if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
					t.Fatalf("client got packet %d, want EndOfStream", got[0])
				}
			case tc.wantExc:
				if got := readSomeConn(t, h.clientProxy); got[0] != byte(chproto.ServerExceptionCode) {
					t.Fatalf("client got packet %d, want Exception", got[0])
				}
			}
			assertNoUpstreamBytes(t, h.upstreamProxy)
			strict, inputs, _, aborts, successes := hooks.counts()
			if strict != 0 || inputs != 0 || aborts != 1 || successes != 0 {
				t.Fatalf("hooks strict/input/abort/success = %d/%d/%d/%d, want 0/0/1/0", strict, inputs, aborts, successes)
			}
			h.close(t)
		})
	}
}

func TestRelay_SynthesizedInsert_UpstreamSampleStepFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answer   func(t *testing.T) []byte
		wantExc  bool
		wantText string
	}{
		{"upstream exception at the sample step", func(t *testing.T) []byte {
			return encodeServerExceptionPacket(deferredTestRev, 60, "Table db.t does not exist")
		}, true, "does not exist"},
		{"premature end of stream before the payload", func(t *testing.T) []byte {
			return []byte{byte(chproto.ServerEndOfStreamCode)}
		}, false, ""},
		{"sample names a different column", func(t *testing.T) []byte {
			return encodeServerSampleNamed(t, deferredTestRev, "other")
		}, true, "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			answer := tc.answer(t)
			upDone := make(chan error, 1)
			go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan,
				func(up *chproto.Codec) error { return up.WriteRawPacket(answer) }, upDone)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
			if tc.wantExc {
				got := readSomeConn(t, h.clientProxy)
				if got[0] != byte(chproto.ServerExceptionCode) || !bytes.Contains(got, []byte(tc.wantText)) {
					t.Fatalf("client got %x, want an Exception mentioning %q", got, tc.wantText)
				}
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
			if _, _, _, _, successes := hooks.counts(); successes != 0 {
				t.Fatalf("OnQuerySuccess fired %d times, want 0", successes)
			}
			h.close(t)
		})
	}
}

func TestRelay_SynthesizedInsert_TruncatedValuesShapeStaysOnOrdinaryPath(t *testing.T) {
	const truncated = "INSERT INTO t VALUES "
	hooks := newSynthesizedHooks()
	hooks.skipPlanFor = truncated
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	rows, empty := synthesizedPacket(t, deferredTestRev), encodeEmptyClientData(t)
	upDone := make(chan error, 1)
	go func() {
		up := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		up.SetRevision(deferredTestRev)
		up.SetCompression(proto.CompressionDisabled)
		upDone <- func() error {
			pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
			if err != nil {
				return fmt.Errorf("read query: %w", err)
			}
			if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != truncated {
				return fmt.Errorf("upstream query = %#v, want %q", pkt.Decoded, truncated)
			}
			for _, want := range [][]byte{rows, empty} {
				got, err := up.ReadPacket()
				if err != nil || !bytes.Equal(got.Raw, want) {
					return fmt.Errorf("upstream packet = %v (err %v), want %x", got, err, want)
				}
			}
			return nil
		}()
	}()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", truncated))
	writeAllConn(t, h.clientProxy, rows)
	writeAllConn(t, h.clientProxy, empty)
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if len(hooks.payloadAtStrict) != 0 {
		t.Fatal("the synthesized lane ran for the truncated shape")
	}
	writeAllConn(t, h.clientProxy, rows)
	var sawGuard bool
	for _, err := range h.close(t) {
		if err != nil && strings.Contains(err.Error(), "client Data packet has no active query") {
			sawGuard = true
		}
	}
	if !sawGuard {
		t.Fatal("a stray client Data packet no longer hits the no-active-query guard")
	}
}

// serveSynthesizedNext proves reuse with a complete ordinary query, including
// its marker, on the same upstream codec and connection.
func serveSynthesizedNext(up *chproto.Codec) error {
	pkt, err := up.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		return err
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "SELECT 1" {
		return fmt.Errorf("next query = %#v", pkt.Decoded)
	}
	pkt, err = up.ReadPacket()
	if err != nil {
		return err
	}
	if pkt.Type != uint64(chproto.ClientDataCode) {
		return fmt.Errorf("next marker type %d", pkt.Type)
	}
	return up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)})
}

func synthesizedNext(t *testing.T, h *deferredHarness) {
	t.Helper()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "next", "SELECT 1"))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("next terminal = %x", got)
	}
}

func TestRelay_SynthesizedInsert_CancelDrainsAndReuses(t *testing.T) {
	for _, afterPayload := range []bool{false, true} {
		for _, exception := range []bool{false, true} {
			t.Run(fmt.Sprintf("payload=%v/exception=%v", afterPayload, exception), func(t *testing.T) {
				hooks := newSynthesizedHooks()
				hooks.skipPlanFor = "SELECT 1"
				h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
				ready := make(chan struct{})
				upDone := make(chan error, 1)
				go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
					if afterPayload {
						if err := up.WriteRawPacket(encodeServerSampleNamed(t, deferredTestRev, "v")); err != nil {
							return err
						}
						if err := readPayloadAndTerminator(up, synthesizedPacket(t, deferredTestRev)); err != nil {
							return err
						}
						<-hooks.inputDone
					}
					close(ready)
					pkt, err := up.ReadPacket()
					if err != nil {
						return err
					}
					if !bytes.Equal(pkt.Raw, []byte{byte(chproto.ClientCancelCode)}) {
						return fmt.Errorf("cancel = %x", pkt.Raw)
					}
					terminal := []byte{byte(chproto.ServerEndOfStreamCode)}
					if exception {
						terminal = encodeServerExceptionPacket(deferredTestRev, 394, "query cancelled")
					}
					if err := up.WriteRawPacket(terminal); err != nil {
						return err
					}
					return serveSynthesizedNext(up)
				}, upDone)
				writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "first", synthesizedBody))
				writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
				<-ready
				writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
				if exception {
					readServerException(t, h.clientProxy)
				} else if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
					t.Fatalf("terminal %x", got)
				}
				_, _, completes, aborts, successes := hooks.counts()
				if completes != 1 || aborts != 0 || successes != 0 {
					t.Fatalf("cancel lifecycle complete/abort/success=%d/%d/%d", completes, aborts, successes)
				}
				synthesizedNext(t, h)
				if err := <-upDone; err != nil {
					t.Fatal(err)
				}
				_, _, completes, aborts, successes = hooks.counts()
				if completes != 2 || aborts != 0 || successes != 1 {
					t.Fatalf("reuse lifecycle %d/%d/%d", completes, aborts, successes)
				}
				h.close(t)
			})
		}
	}
}

func TestRelay_SynthesizedInsert_AllOwnershipConflicts(t *testing.T) {
	for _, conflict := range []string{"Deferred", "Suppress", "AgentPrepare", "QueryOnly", "AbortWithSuccess"} {
		t.Run(conflict, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			hooks.conflict = conflict
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			exc := readServerException(t, h.clientProxy)
			if !strings.Contains(exc.Message, "conflicts with another ownership plan") {
				t.Fatal(exc.Message)
			}
			assertNoUpstreamBytes(t, h.upstreamProxy)
			strict, _, complete, abort, success := hooks.counts()
			if strict != 0 || complete != 1 || abort != 1 || success != 0 {
				t.Fatalf("lifecycle %d/%d/%d/%d", strict, complete, abort, success)
			}
			h.close(t)
		})
	}
}

func TestRelay_SynthesizedInsert_LocalRejectionReuse(t *testing.T) {
	for _, kind := range []string{"cancel", "overflow", "strict keep session", "strict close"} {
		t.Run(kind, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			hooks.skipPlanFor = "SELECT 1"
			switch kind {
			case "overflow":
				hooks.limit = 1
				hooks.enforce = true
			case "strict keep session":
				hooks.strictErr = &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: "retry", KeepSession: true}
			case "strict close":
				hooks.strictErr = errors.New("admission rejected")
			}
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			if kind == "cancel" {
				writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
				if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
					t.Fatalf("cancel terminal %x", got)
				}
			} else {
				writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
				readServerException(t, h.clientProxy)
			}
			assertNoUpstreamBytes(t, h.upstreamProxy)
			_ = h.upstreamProxy.SetReadDeadline(time.Time{})
			if kind == "strict close" {
				select {
				case err := <-h.loopErr:
					if err == nil {
						t.Fatal("closing rejection returned nil")
					}
				case <-time.After(time.Second):
					t.Fatal("closing rejection did not end reader")
				}
				return
			}
			done := make(chan error, 1)
			go func() {
				up := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
				up.SetRevision(deferredTestRev)
				done <- serveSynthesizedNext(up)
			}()
			synthesizedNext(t, h)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			_, _, complete, abort, success := hooks.counts()
			if complete != 2 || abort != 1 || success != 1 {
				t.Fatalf("reuse lifecycle %d/%d/%d", complete, abort, success)
			}
			h.close(t)
		})
	}
}

// synthesizedMarkerHooks makes upstream-first shutdown deterministic: the
// local marker owner cannot publish lifecycle hooks until the test releases it.
type synthesizedMarkerHooks struct {
	*synthesizedInsertHooks
	admitted     chan struct{}
	abortEntered chan struct{}
	releaseAbort chan struct{}
}

func (h *synthesizedMarkerHooks) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	err := h.synthesizedInsertHooks.OnQuery(ctx, qctx)
	close(h.admitted)
	return err
}

func (h *synthesizedMarkerHooks) OnQueryAbort(ctx context.Context, qctx *plugin.QueryContext) {
	if h.releaseAbort != nil {
		close(h.abortEntered)
		<-h.releaseAbort
	}
	h.synthesizedInsertHooks.OnQueryAbort(ctx, qctx)
}

func TestRelay_SynthesizedInsert_MissingOrNamedMarker(t *testing.T) {
	for _, kind := range []string{"missing live", "fragment live", "named empty"} {
		t.Run(kind, func(t *testing.T) {
			hooks := &synthesizedMarkerHooks{
				synthesizedInsertHooks: newSynthesizedHooks(), admitted: make(chan struct{}),
			}
			var release sync.Once
			releaseAbort := func() {
				if hooks.releaseAbort != nil {
					release.Do(func() { close(hooks.releaseAbort) })
				}
			}
			t.Cleanup(releaseAbort)
			if kind != "named empty" {
				hooks.abortEntered = make(chan struct{})
				hooks.releaseAbort = make(chan struct{})
			}
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			select {
			case <-hooks.admitted:
			case <-time.After(time.Second):
				t.Fatal("query was not admitted before marker cancellation")
			}
			if kind == "named empty" {
				marker := encodeEmptyClientData(t)
				// Replace the empty name's zero-length string, retaining BlockInfo/body.
				raw := append([]byte{byte(chproto.ClientDataCode), 3}, []byte("ext")...)
				raw = append(raw, marker[2:]...)
				writeAllConn(t, h.clientProxy, raw)
				exc := readServerException(t, h.clientProxy)
				if !strings.Contains(exc.Message, "external table block") {
					t.Fatal(exc.Message)
				}
			} else {
				if kind == "fragment live" {
					// net.Pipe acknowledges this write only after the lane's sole
					// reader consumes the packet type; the rest of the frame is absent.
					writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientDataCode)})
				}
				h.cancel()
				select {
				case <-hooks.abortEntered:
				case <-time.After(time.Second):
					t.Fatal("marker reader did not enter its abort hook")
				}
			}
			select {
			case err := <-h.loopErr:
				if err == nil {
					t.Fatal("marker failure returned nil")
				}
			case <-time.After(time.Second):
				t.Fatal("live marker wait did not end")
			}
			// For canceled marker reads the barrier forces the upstream loop
			// to return first, before the local owner can publish abort/complete.
			// For a named block the client loop returns first; emulate production
			// runPostHandshake closing the transport to release the other reader.
			if kind == "named empty" {
				_ = h.proxyUpstream.Close()
			} else {
				releaseAbort()
			}
			select {
			case err := <-h.loopErr:
				if err == nil {
					t.Fatal("second marker relay loop returned nil")
				}
			case <-time.After(time.Second):
				t.Fatal("marker shutdown did not join both relay loops")
			}
			assertNoUpstreamBytes(t, h.upstreamProxy)
			strict, _, complete, abort, success := hooks.counts()
			if strict != 0 || complete != 1 || abort != 1 || success != 0 {
				t.Fatalf("marker lifecycle %d/%d/%d/%d", strict, complete, abort, success)
			}
		})
	}
}

func encodeSynthesizedSample(t *testing.T, rev int, cols []chproto.SampleColumn) []byte {
	t.Helper()
	var buf bytes.Buffer
	codec := chproto.NewCodec(&readWriter{r: &bytes.Buffer{}, w: &buf}, chproto.DirToUpstream)
	codec.SetRevision(rev)
	if err := codec.WriteSampleBlock(cols); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRelay_SynthesizedInsert_SampleSchemaDriftCloses(t *testing.T) {
	want := []chproto.SampleColumn{{Name: "v", Type: "UInt64"}, {Name: "w", Type: "String"}}
	for _, tc := range []struct {
		name string
		cols []chproto.SampleColumn
	}{
		{"name", []chproto.SampleColumn{{Name: "other", Type: "UInt64"}, want[1]}},
		{"type", []chproto.SampleColumn{{Name: "v", Type: "Int64"}, want[1]}},
		{"order", []chproto.SampleColumn{want[1], want[0]}},
		{"missing", want[:1]},
		{"extra", append(append([]chproto.SampleColumn{}, want...), chproto.SampleColumn{Name: "extra", Type: "UInt8"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hooks := newSynthesizedHooks()
			hooks.sample = want
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			done := make(chan error, 1)
			go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
				return up.WriteRawPacket(encodeSynthesizedSample(t, deferredTestRev, tc.cols))
			}, done)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
			exc := readServerException(t, h.clientProxy)
			if !strings.Contains(exc.Message, "upstream sample mismatch") {
				t.Fatal(exc.Message)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			var closing bool
			select {
			case err := <-h.loopErr:
				closing = err != nil
			case <-time.After(time.Second):
			}
			if !closing {
				t.Fatal("sample drift did not close")
			}
			_, input, complete, abort, success := hooks.counts()
			if input != 0 || complete != 1 || abort != 1 || success != 0 {
				t.Fatalf("lifecycle %d/%d/%d/%d", input, complete, abort, success)
			}
		})
	}
}

func TestRelay_SynthesizedInsert_MultipleBlocksAndIndependentChunking(t *testing.T) {
	for _, clientChunked := range []bool{false, true} {
		for _, upChunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("client=%v/upstream=%v", clientChunked, upChunked), func(t *testing.T) {
				const rev = 54470
				hooks := newSynthesizedHooks()
				second := proto.ColUInt64{44, 55}
				hooks.blocks = [][]proto.InputColumn{synthesizedBlock(), {{Name: "v", Data: &second}}}
				hooks.skipPlanFor = "SELECT 1"
				packets := make([][]byte, 0, 2)
				for _, block := range hooks.blocks {
					raw, err := nativepayload.EncodeClientDataPacket(rev, block)
					if err != nil {
						t.Fatal(err)
					}
					packets = append(packets, raw)
				}
				h := newSynthesizedHarness(t, hooks, rev, upChunked, true, clientChunked)
				done := make(chan error, 1)
				go synthUpstream(t, h.upstreamProxy, rev, upChunked, &hooks.strictRan, func(up *chproto.Codec) error {
					if err := up.WriteRawPacket(encodeServerSampleNamed(t, rev, "v")); err != nil {
						return err
					}
					for i, want := range packets {
						pkt, err := up.ReadPacket()
						if err != nil {
							return err
						}
						if !bytes.Equal(pkt.Raw, want) {
							return fmt.Errorf("block %d mismatch", i)
						}
					}
					pkt, err := up.ReadPacket()
					if err != nil {
						return err
					}
					if empty, err := chproto.ClientDataPacketIsEmpty(pkt.Raw, proto.CompressionDisabled); err != nil || !empty {
						return fmt.Errorf("terminator %x (%v)", pkt.Raw, err)
					}
					<-hooks.inputDone
					if err := up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
						return err
					}
					return serveSynthesizedNext(up)
				}, done)
				client := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
				client.SetRevision(rev)
				if clientChunked {
					client.EnableChunked(true, true)
				}
				if err := client.WriteRawPacket(encodeSynthesizedQuery(rev, "qid", synthesizedBody)); err != nil {
					t.Fatal(err)
				}
				if err := client.WriteEmptyDataBlock(); err != nil {
					t.Fatal(err)
				}
				pkt, err := client.ReadPacket()
				if err != nil {
					t.Fatal(err)
				}
				if pkt.Type != uint64(chproto.ServerEndOfStreamCode) {
					t.Fatalf("terminal %d", pkt.Type)
				}
				if err := client.WriteRawPacket(encodeSynthesizedQuery(rev, "next", "SELECT 1")); err != nil {
					t.Fatal(err)
				}
				if err := client.WriteEmptyDataBlock(); err != nil {
					t.Fatal(err)
				}
				next, err := client.ReadPacket()
				if err != nil || next.Type != uint64(chproto.ServerEndOfStreamCode) {
					t.Fatalf("next query terminal %v (%v)", next, err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(hooks.payloadAtStrict, bytes.Join(packets, nil)) {
					t.Fatal("strict payload differs from ordered wire packets")
				}
				h.close(t)
			})
		}
	}
}

func TestRelay_SynthesizedInsert_BackpressureDisposition(t *testing.T) {
	for _, housegate := range []bool{false, true} {
		t.Run(fmt.Sprintf("housegate=%v", housegate), func(t *testing.T) {
			hooks := newSynthesizedHooks()
			hooks.skipPlanFor = "SELECT 1"
			h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
			done := make(chan error, 1)
			go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
				if err := up.WriteRawPacket(encodeServerSampleNamed(t, deferredTestRev, "v")); err != nil {
					return err
				}
				if err := readPayloadAndTerminator(up, synthesizedPacket(t, deferredTestRev)); err != nil {
					return err
				}
				<-hooks.inputDone
				msg := "too many parts"
				if housegate {
					msg = "storage_integrity: back-pressure: unsafe parts over limit"
				}
				if err := up.WriteRawPacket(encodeServerExceptionPacket(deferredTestRev, 252, msg)); err != nil {
					return err
				}
				if housegate {
					return serveSynthesizedNext(up)
				}
				return nil
			}, done)
			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
			writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
			exc := readServerException(t, h.clientProxy)
			if exc.Code != 252 {
				t.Fatalf("code %d", exc.Code)
			}
			if housegate {
				synthesizedNext(t, h)
			} else {
				select {
				case err := <-h.loopErr:
					if err == nil {
						t.Fatal("generic252 returned nil")
					}
				case <-time.After(time.Second):
					t.Fatal("generic252 did not close")
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			_, _, complete, abort, success := hooks.counts()
			wantComplete, wantSuccess := 1, 0
			if housegate {
				wantComplete, wantSuccess = 2, 1
			}
			if complete != wantComplete || abort != 1 || success != wantSuccess {
				t.Fatalf("lifecycle %d/%d/%d", complete, abort, success)
			}
		})
	}
}

func TestRelay_SynthesizedInsert_SecondQueryRejected(t *testing.T) {
	hooks := newSynthesizedHooks()
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	ready := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error { return nil }, ready)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid2", synthesizedBody))
	select {
	case err := <-h.loopErr:
		if err == nil {
			t.Fatal("second query returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("second query not rejected")
	}
	_, _, complete, abort, success := hooks.counts()
	if complete != 1 || abort != 1 || success != 0 {
		t.Fatalf("lifecycle %d/%d/%d", complete, abort, success)
	}
}

// A completed Cancel write may not yet have returned to Relay when the peer
// sends its terminal. Hold that return to exercise the result-arbitration seam.
type synthesizedCancelWriteBarrier struct {
	net.Conn
	wrote   chan struct{}
	release chan struct{}
	fail    bool
}

func (c *synthesizedCancelWriteBarrier) Write(raw []byte) (int, error) {
	n, err := c.Conn.Write(raw)
	if bytes.Equal(raw, []byte{byte(chproto.ClientCancelCode)}) {
		close(c.wrote)
		<-c.release
		if c.fail {
			return n, errors.New("injected Cancel write failure")
		}
	}
	return n, err
}

func newSynthesizedBarrierHarness(t *testing.T, hooks plugin.Hooks, wrap func(net.Conn) net.Conn, obs PacketObserver) *deferredHarness {
	t.Helper()
	client, proxyClient := net.Pipe()
	upstream, proxyUpstream := net.Pipe()
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(deferredTestRev)
	up := chproto.NewCodec(wrap(proxyUpstream), chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	relay := NewRelay(sess, hooks, obs, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: client, proxyClient: proxyClient, upstreamProxy: upstream, proxyUpstream: proxyUpstream, relay: relay, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- relay.clientToUpstream(ctx) }()
	go func() { h.loopErr <- relay.upstreamToClient(ctx) }()
	t.Cleanup(func() { cancel(); client.Close(); upstream.Close() })
	return h
}

func TestRelay_SynthesizedInsert_CancelWriteTerminalRace(t *testing.T) {
	for _, fail := range []bool{false, true} {
		for _, exception := range []bool{false, true} {
			t.Run(fmt.Sprintf("writeFailure=%v/exception=%v", fail, exception), func(t *testing.T) {
				hooks := newSynthesizedHooks()
				hooks.skipPlanFor = "SELECT 1"
				var barrier *synthesizedCancelWriteBarrier
				h := newSynthesizedBarrierHarness(t, hooks, func(c net.Conn) net.Conn {
					barrier = &synthesizedCancelWriteBarrier{Conn: c, wrote: make(chan struct{}), release: make(chan struct{}), fail: fail}
					return barrier
				}, nil)
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(barrier.release) }) })
				ready := make(chan struct{})
				terminalRead := make(chan struct{})
				done := make(chan error, 1)
				go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
					close(ready)
					pkt, err := up.ReadPacket()
					if err != nil {
						return err
					}
					if pkt.Type != uint64(chproto.ClientCancelCode) {
						return fmt.Errorf("cancel type %d", pkt.Type)
					}
					terminal := []byte{byte(chproto.ServerEndOfStreamCode)}
					if exception {
						terminal = encodeServerExceptionPacket(deferredTestRev, 394, "cancelled")
					}
					if err := up.WriteRawPacket(terminal); err != nil {
						return err
					}
					close(terminalRead)
					if !fail {
						return serveSynthesizedNext(up)
					}
					return nil
				}, done)
				writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
				writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
				<-ready
				writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
				<-barrier.wrote
				<-terminalRead
				// The complete upstream terminal has been read while Cancel's Write result
				// is unavailable. Neither success nor completion may run yet.
				_, _, complete, abort, success := hooks.counts()
				if complete != 0 || abort != 0 || success != 0 {
					t.Fatalf("premature lifecycle %d/%d/%d", complete, abort, success)
				}
				release.Do(func() { close(barrier.release) })
				if fail {
					for i := 0; i < 2; i++ {
						select {
						case err := <-h.loopErr:
							if err == nil {
								t.Fatal("Cancel write failure returned nil")
							}
						case <-time.After(time.Second):
							t.Fatal("Cancel write failure stuck")
						}
					}
					_, _, complete, abort, success = hooks.counts()
					if complete != 1 || abort != 1 || success != 0 {
						t.Fatalf("failed cancel lifecycle %d/%d/%d", complete, abort, success)
					}
				} else {
					if exception {
						readServerException(t, h.clientProxy)
					} else if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
						t.Fatalf("terminal %x", got)
					}
					synthesizedNext(t, h)
					_, _, complete, abort, success = hooks.counts()
					if complete != 2 || abort != 0 || success != 1 {
						t.Fatalf("successful cancel lifecycle %d/%d/%d", complete, abort, success)
					}
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRelay_SynthesizedInsert_PrematureEOSBeatsLateCancel(t *testing.T) {
	hooks := newSynthesizedHooks()
	observer := &blockingDeferredTerminalObserver{entered: make(chan struct{}), release: make(chan struct{})}
	observer.armed.Store(true)
	h := newSynthesizedBarrierHarness(t, hooks, func(c net.Conn) net.Conn { return c }, observer)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(observer.release) }) })
	done := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error { return up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}) }, done)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	<-observer.entered
	writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
	release.Do(func() { close(observer.release) })
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			if err == nil {
				t.Fatal("premature terminal returned nil")
			}
		case <-time.After(time.Second):
			t.Fatal("late cancel race stuck")
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertNoUpstreamBytes(t, h.upstreamProxy)
	_, _, complete, abort, success := hooks.counts()
	if complete != 1 || abort != 1 || success != 0 {
		t.Fatalf("premature lifecycle %d/%d/%d", complete, abort, success)
	}
}

func TestRelay_SynthesizedInsert_PrematureEOSStopsPayloadWriter(t *testing.T) {
	hooks := newSynthesizedHooks()
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	done := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error {
		raw := append(encodeServerSampleNamed(t, deferredTestRev, "v"), byte(chproto.ServerEndOfStreamCode))
		_, err := h.upstreamProxy.Write(raw)
		return err
	}, done)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			if err == nil {
				t.Fatal("premature EOS returned nil")
			}
		case <-time.After(time.Second):
			t.Fatal("premature EOS left writer blocked")
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, input, complete, abort, success := hooks.counts()
	if input != 0 || complete != 1 || abort != 1 || success != 0 {
		t.Fatalf("lifecycle %d/%d/%d/%d", input, complete, abort, success)
	}
}

func TestRelay_SynthesizedInsert_EncodesAtUpstreamRevision(t *testing.T) {
	hooks := newSynthesizedHooks()
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	h.relay.sess.Upstream().SetRevision(54470)
	done := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, 54470, false, &hooks.strictRan, func(up *chproto.Codec) error {
		if err := up.WriteRawPacket(encodeServerSampleNamed(t, 54470, "v")); err != nil {
			return err
		}
		if err := readPayloadAndTerminator(up, synthesizedPacket(t, 54470)); err != nil {
			return err
		}
		<-hooks.inputDone
		return up.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)})
	}, done)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal %x", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hooks.payloadAtStrict, synthesizedPacket(t, 54470)) {
		t.Fatal("signed bytes used client revision")
	}
}

func TestParseSynthesizedSampleColumns_BlockInfoCompatibility(t *testing.T) {
	for _, rev := range []int{51902, 51903, 54453, 54454, 54470} {
		t.Run(fmt.Sprint(rev), func(t *testing.T) {
			want := []chproto.SampleColumn{{Name: "v", Type: "UInt64"}}
			raw := encodeSynthesizedSample(t, rev, want)
			if rev >= 51903 {
				var extra proto.Buffer
				extra.PutUVarInt(3)
				extra.PutUVarInt(2)
				extra.PutInt32(7)
				extra.PutInt32(9)
				raw = append(append(append([]byte{}, raw[:9]...), extra.Buf...), raw[9:]...)
			}
			original := append([]byte{}, raw...)
			got, err := parseSampleColumns(raw, rev)
			if err != nil {
				t.Fatal(err)
			}
			if err := matchSampleColumns(want, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("sample normalization mutated captured wire bytes")
			}
		})
	}
}

func TestRelay_SynthesizedInsert_ContextCancelsBlockedCancelWrite(t *testing.T) {
	hooks := newSynthesizedHooks()
	h := newSynthesizedHarness(t, hooks, deferredTestRev, false, false)
	ready := make(chan error, 1)
	go synthUpstream(t, h.upstreamProxy, deferredTestRev, false, &hooks.strictRan, func(up *chproto.Codec) error { return nil }, ready)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", synthesizedBody))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	// The live upstream deliberately stops reading before Cancel. The lane
	// must still react to its context while its sole writer is backpressured.
	writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
	h.cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-h.loopErr:
		case <-time.After(time.Second):
			t.Fatal("context did not release blocked Cancel write")
		}
	}
	_, _, complete, abort, success := hooks.counts()
	if complete != 1 || abort != 1 || success != 0 {
		t.Fatalf("lifecycle %d/%d/%d", complete, abort, success)
	}
}

type synthesizedCloseBarrier struct {
	net.Conn
	entered, release chan struct{}
}

func (c *synthesizedCloseBarrier) Close() error {
	err := c.Conn.Close()
	close(c.entered)
	<-c.release
	return err
}

func TestRelay_SynthesizedInsert_JoinsStartedCancellationCallback(t *testing.T) {
	client, proxyClient := net.Pipe()
	upstream, proxyUpstream := net.Pipe()
	defer client.Close()
	defer upstream.Close()
	barrier := &synthesizedCloseBarrier{Conn: proxyUpstream, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(barrier.release) })
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(deferredTestRev)
	up := chproto.NewCodec(barrier, chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	hooks := newSynthesizedHooks()
	relay := NewRelay(sess, hooks, nil, nil)
	qctx := &plugin.QueryContext{Query: &chproto.Query{ID: "qid", Body: synthesizedBody}, Session: sess}
	if err := hooks.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- relay.runSynthesizedInsert(ctx, qctx, proto.CompressionDisabled) }()
	cancel()
	<-barrier.entered
	// Close has already released the blocked marker read, but the callback
	// itself has not returned. The lane must join before it can hand back control.
	select {
	case <-done:
		t.Fatal("lane returned while its cancellation callback was still closing codecs")
	case <-time.After(25 * time.Millisecond):
	}
	release.Do(func() { close(barrier.release) })
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled marker returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("lane did not return after callback completed")
	}
}

// TestQueryMayStreamClientDataTreatsInlineFormatDataAsNoPayload pins the relay
// half of the FORMAT-with-inline-data fix: rows after the format name ride in
// the query text, so the relay must not expect a ClientData payload for them.
func TestQueryMayStreamClientDataTreatsInlineFormatDataAsNoPayload(t *testing.T) {
	for sql, want := range map[string]bool{
		"INSERT INTO t FORMAT Native":             true,
		"INSERT INTO t FORMAT Values":             true,
		"INSERT INTO t FORMAT Values ;":           true,
		"INSERT INTO t":                           true,
		"INSERT INTO t VALUES (1)":                false,
		"INSERT INTO t FORMAT Values (1)":         false,
		"INSERT INTO t (x) FORMAT Values(40 + 2)": false,
		"INSERT INTO t FORMAT CSV 1,2":            false,
		"INSERT INTO t SELECT 1":                  false,
	} {
		qctx := &plugin.QueryContext{Query: &chproto.Query{Body: sql}}
		if got := queryMayStreamClientData(qctx); got != want {
			t.Errorf("queryMayStreamClientData(%q) = %v, want %v", sql, got, want)
		}
	}
}
