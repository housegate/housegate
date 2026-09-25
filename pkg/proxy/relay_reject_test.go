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
	// rejectErr overrides the default 252 back-pressure refusal.
	rejectErr error
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
	if h.rejectErr != nil {
		return h.rejectErr
	}
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

// The upstream reader must consume q1's terminal replacement while it still
// owns q1. If it clears activeQuery first, the client reader can begin and
// reject q2 in between, overwriting the single pending-rejection slot and
// exposing q1's withheld INSERT as a false-success EndOfStream.
func TestRelay_TakeActiveQueryStateConsumesPendingRejectionAtomically(t *testing.T) {
	r := &Relay{}
	q1 := &chproto.Exception{Code: proto.Error(chproto.CodeTooManyParts), Message: "q1 back-pressure"}
	if !r.beginActiveQuery("q1") {
		t.Fatal("begin q1 = false")
	}
	if !r.rejectActiveQueryTerminal("q1", q1) {
		t.Fatal("reject q1 = false")
	}

	queryID, canceled, active, rejection := r.takeActiveQueryState()
	if queryID != "q1" || canceled || !active || rejection != q1 {
		t.Fatalf("q1 terminal state = %q/%v/%v/%p, want q1/false/true/%p", queryID, canceled, active, rejection, q1)
	}

	q2 := &chproto.Exception{Code: proto.Error(chproto.CodeTooManyParts), Message: "q2 back-pressure"}
	if !r.beginActiveQuery("q2") {
		t.Fatal("begin q2 = false after taking q1")
	}
	if !r.rejectActiveQueryTerminal("q2", q2) {
		t.Fatal("reject q2 = false")
	}
	queryID, canceled, active, rejection = r.takeActiveQueryState()
	if queryID != "q2" || canceled || !active || rejection != q2 {
		t.Fatalf("q2 terminal state = %q/%v/%v/%p, want q2/false/true/%p", queryID, canceled, active, rejection, q2)
	}
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

// A storage-integrity admission refused because its table's merge latch is
// not asserted yet (a newly Active table) is the retryable 733 with
// KeepSession: the staged lane keeps the session exactly as for 252.
func TestRelay_StagedTableActivatingRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	testStagedSessionPreservingRejection(t, chproto.CodeTableIsBeingRestarted, chproto.TableActivatingMessage("db1.t"))
}

// Spec §9.6/§7.4: a statement whose table retired before the arbiter
// sequenced it is refused non-retryably, but the session survives.
func TestRelay_StagedNoLongerAcceptsWritesRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	testStagedSessionPreservingRejection(t, chproto.CodeQueryIsProhibited, chproto.TableNoLongerAcceptsWritesMessage("db1.t"))
}

func testStagedSessionPreservingRejection(t *testing.T, code int32, message string) {
	t.Helper()
	hooks := &stagedRejectHooks{rejectOne: true, rejectErr: &chproto.ClientError{
		Code:        code,
		Message:     message,
		KeepSession: true,
	}}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upDone := make(chan error, 1)
	go func() { upDone <- serveStagedRejectUpstream(t, h.upstreamProxy) }()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	writeAllConn(t, h.clientProxy, empty)

	exc := readServerException(t, h.clientProxy)
	if exc.Code != proto.Error(code) || exc.Message != message {
		t.Fatalf("exception = %d %q, want %d %q", exc.Code, exc.Message, code, message)
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

// deferredRejectHooks is the agent-side shape: Relay answers the sample block
// locally, buffers the payload, and the strict hook refuses the first query.
type deferredRejectHooks struct {
	stagedRejectHooks
}

func (h *deferredRejectHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queries = append(h.queries, qctx.Query.Body)
	if len(h.queries) == 1 {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{
			SampleColumns:   []chproto.SampleColumn{{Name: "v", Type: "UInt64"}},
			MaxPayloadBytes: 1 << 20,
		}
	}
	return nil
}

func TestRelay_DeferredRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	hooks := &deferredRejectHooks{stagedRejectHooks: stagedRejectHooks{rejectOne: true}}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upDone := make(chan error, 1)
	go func() { upDone <- serveSecondQueryOnlyUpstream(t, h.upstreamProxy) }()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	writeAllConn(t, h.clientProxy, empty)

	exc := readServerException(t, h.clientProxy)
	if exc.Code != proto.Error(chproto.CodeTooManyParts) {
		t.Fatalf("exception code = %d, want 252", exc.Code)
	}
	waitForRejectCounts(t, &hooks.stagedRejectHooks)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("a relay loop exited after the deferred rejection: %v", err)
	default:
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
}

// serveSecondQueryOnlyUpstream proves the rejected deferred INSERT never
// reached upstream: the first packet it sees is the SELECT.
func serveSecondQueryOnlyUpstream(t *testing.T, conn net.Conn) error {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		return err
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "SELECT 1" {
		t.Errorf("upstream first query = %#v, want SELECT 1", pkt.Decoded)
	}
	marker, err := codec.ReadPacket()
	if err != nil {
		return err
	}
	if empty, inspectErr := chproto.ClientDataPacketIsEmpty(marker.Raw, proto.CompressionDisabled); inspectErr != nil || !empty {
		t.Errorf("SELECT input marker empty/err = %v/%v, want true/nil", empty, inspectErr)
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	return err
}

// An agent relay has already consumed and signed its whole deferred payload
// when a server-mode Housegate returns the session-preserving back-pressure
// Exception. That terminal must not force the agent to reconnect either.
func TestRelay_DeferredUpstreamBackpressure_KeepsSessionAndServesNextQuery(t *testing.T) {
	testDeferredUpstreamSessionPreservingRejection(t, &chproto.Exception{
		Code:    proto.Error(chproto.CodeTooManyParts),
		Name:    "DB::Exception",
		Message: "storage_integrity: back-pressure: retry later",
	})
}

// The server-mode Housegate refuses an admission whose table's merge latch is
// not asserted yet with the session-preserving 733 activation refusal, after
// it consumed the complete staged input; the agent must keep its session too.
func TestRelay_DeferredUpstreamTableActivating_KeepsSessionAndServesNextQuery(t *testing.T) {
	testDeferredUpstreamSessionPreservingRejection(t, &chproto.Exception{
		Code:    proto.Error(chproto.CodeTableIsBeingRestarted),
		Name:    "DB::Exception",
		Message: chproto.TableActivatingMessage("db1.t"),
	})
}

// The server-mode Housegate answers a statement whose table retired before
// sequencing with the session-preserving §9.6 refusal after it consumed the
// complete staged input; the agent must keep its session too.
func TestRelay_DeferredUpstreamNoLongerAcceptsWrites_KeepsSessionAndServesNextQuery(t *testing.T) {
	testDeferredUpstreamSessionPreservingRejection(t, &chproto.Exception{
		Code:    proto.Error(chproto.CodeQueryIsProhibited),
		Name:    "DB::Exception",
		Message: chproto.TableNoLongerAcceptsWritesMessage("db1.t"),
	})
}

func testDeferredUpstreamSessionPreservingRejection(t *testing.T, rejection *chproto.Exception) {
	t.Helper()
	baseHooks := &deferredInsertHooks{}
	hooks := &firstDeferredInsertHooks{deferredInsertHooks: baseHooks}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	payload := encodeNonEmptyClientDataPacket(t, deferredTestRev)

	upDone := make(chan error, 1)
	go func() { upDone <- serveDeferredRejectionThenSelect(t, h.upstreamProxy, rejection) }()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, payload)
	writeAllConn(t, h.clientProxy, empty)
	if exc := readServerException(t, h.clientProxy); exc.Code != rejection.Code || exc.Message != rejection.Message {
		t.Fatalf("exception = %d %q, want %d %q", exc.Code, exc.Message, rejection.Code, rejection.Message)
	}

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("relay loop exited after session-preserving upstream rejection: %v", err)
	default:
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if successes, lifecycle := baseHooks.terminalCounts(); successes != 1 || len(lifecycle) != 4 || lifecycle[0] != "abort" || lifecycle[1] != "complete" || lifecycle[2] != "success" || lifecycle[3] != "complete" {
		t.Fatalf("successes/lifecycle = %d/%v, want 1/[abort complete success complete]", successes, lifecycle)
	}
}

func serveDeferredRejectionThenSelect(t *testing.T, conn net.Conn, rejection *chproto.Exception) error {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
		return err
	}
	if _, err := codec.ReadPacket(); err != nil { // external-tables marker
		return err
	}
	if _, err := conn.Write(encodeServerSampleDataPacket(t, deferredTestRev)); err != nil {
		return err
	}
	if _, err := codec.ReadPacket(); err != nil { // payload
		return err
	}
	if _, err := codec.ReadPacket(); err != nil { // terminator
		return err
	}
	if err := codec.WriteException(rejection); err != nil {
		return err
	}
	pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		return err
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "SELECT 1" {
		t.Errorf("upstream second query = %#v, want SELECT 1", pkt.Decoded)
	}
	if _, err := codec.ReadPacket(); err != nil {
		return err
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	return err
}

// TestSessionPreservingIngressException pins the narrow wire contract: only
// Housegate's own storage-integrity back-pressure and table-activation
// refusals preserve the session; every other late payload exception, including
// the other 733 lifecycle refusals and a native TOO_MANY_PARTS, stays fatal.
func TestSessionPreservingIngressException(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		msg  string
		want bool
	}{
		{"back-pressure", chproto.CodeTooManyParts, "storage_integrity: back-pressure: retry later", true},
		{"table activating", chproto.CodeTableIsBeingRestarted, chproto.TableActivatingMessage("net1.events"), true},
		{"native too many parts", chproto.CodeTooManyParts, "Too many parts (300)", false},
		{"activation text under 252", chproto.CodeTooManyParts, chproto.TableActivatingMessage("net1.events"), false},
		{"pending activation", chproto.CodeTableIsBeingRestarted, "storage_integrity: table net1.events is pending activation (retryable)", false},
		{"native table restarting", chproto.CodeTableIsBeingRestarted, "Table db.t is being restarted", false},
		{"no table id", chproto.CodeTableIsBeingRestarted, chproto.TableActivatingMessage(""), false},
		{"activation id with whitespace", chproto.CodeTableIsBeingRestarted, chproto.TableActivatingMessage("net1 events"), false},
		{"no longer accepts writes", chproto.CodeQueryIsProhibited, chproto.TableNoLongerAcceptsWritesMessage("net1.events"), true},
		{"no longer accepts writes under 733", chproto.CodeTableIsBeingRestarted, chproto.TableNoLongerAcceptsWritesMessage("net1.events"), false},
		{"activation under 392", chproto.CodeQueryIsProhibited, chproto.TableActivatingMessage("net1.events"), false},
		{"refused", chproto.CodeQueryIsProhibited, "storage_integrity: table net1.events was refused: SCHEMA_INVALID: bad column", false},
		{"table state unavailable", chproto.CodeQueryIsProhibited, "storage_integrity: table state is unavailable for this query", false},
		{"governed create", chproto.CodeQueryIsProhibited, "storage_integrity: table net1.events is governed by storage integrity and cannot be created with data; create the table first, then INSERT", false},
		{"no longer accepts writes id with whitespace", chproto.CodeQueryIsProhibited, chproto.TableNoLongerAcceptsWritesMessage("net1 events"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exc := &chproto.Exception{Code: proto.Error(tc.code), Name: "DB::Exception", Message: tc.msg}
			if got := isSessionPreservingIngressException(exc); got != tc.want {
				t.Fatalf("isSessionPreservingIngressException(%d, %q) = %v, want %v", tc.code, tc.msg, got, tc.want)
			}
		})
	}
	if isSessionPreservingIngressException(nil) || isSessionPreservingIngressException("not an exception") {
		t.Fatal("a non-Exception packet must not preserve the session")
	}
}
