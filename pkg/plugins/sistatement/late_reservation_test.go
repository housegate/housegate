package sistatement

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type seqMetrics struct {
	mu       sync.Mutex
	recycled int
	burned   map[string]int
}

func (m *seqMetrics) InlineValuesSynthesized()      {}
func (m *seqMetrics) InlineValuesEvaluationFailed() {}
func (m *seqMetrics) InlineValuesClosureRefused()   {}
func (m *seqMetrics) SeqRecycled()                  { m.mu.Lock(); m.recycled++; m.mu.Unlock() }
func (m *seqMetrics) SeqBurned(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.burned == nil {
		m.burned = map[string]int{}
	}
	m.burned[reason]++
}

func lateFixture(t *testing.T) (*Plugin, *SeqCounter, *seqMetrics) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seq.Close() })
	metrics := &seqMetrics{}
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Observer: metrics})
	if err != nil {
		t.Fatal(err)
	}
	return p, seq, metrics
}

// signDeferred drives one claimed INSERT through the strict hook and returns
// its statement id.
func signDeferred(t *testing.T, p *Plugin, qctx *plugin.QueryContext) string {
	t.Helper()
	if err := p.OnClientDataStrict(context.Background(), qctx, encodeRows(t)); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	return qctx.Query.ID
}

func seqOf(t *testing.T, statementID string) uint64 {
	t.Helper()
	_, seq, _, err := sicore.ParseFlatStatementID(statementID)
	if err != nil {
		t.Fatalf("statement id %q: %v", statementID, err)
	}
	return seq
}

const lateSQL = "INSERT INTO shop.orders FORMAT Native"

func TestLateReservation_OnQueryReservesNothing(t *testing.T) {
	p, seq, _ := lateFixture(t)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.DeferredInsert == nil || q.Query.ID != "client-uuid-1" || seq.Last() != 0 {
		t.Fatalf("after OnQuery deferred=%v id=%q last=%d; want claimed, client id kept, nothing reserved", q.DeferredInsert, q.Query.ID, seq.Last())
	}
	if id := signDeferred(t, p, q); seqOf(t, id) != 1 || seq.Last() != 1 {
		t.Fatalf("strict hook id = %q last=%d, want seq 1", id, seq.Last())
	}
}

func TestLateReservation_CancelAndPayloadLimitReserveNothing(t *testing.T) {
	p, seq, _ := lateFixture(t)
	sess := newSession(1, "")
	canceled := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), canceled); err != nil {
		t.Fatal(err)
	}
	p.OnQueryAbort(context.Background(), canceled)
	p.OnQueryComplete(context.Background(), sess)

	p.maxPayload = 4
	limited := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), limited); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), limited, encodeRows(t)); err == nil {
		t.Fatal("over-limit payload must be refused")
	}
	p.OnQueryAbort(context.Background(), limited)
	p.OnQueryComplete(context.Background(), sess)
	if seq.Last() != 0 {
		t.Fatalf("cancel / payload-limit consumed client_seq (last=%d)", seq.Last())
	}
}

func TestLateReservation_UnspentExceptionRecyclesTheSeq(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")
	q := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	first := signDeferred(t, p, q)
	if err := p.OnException(context.Background(), sess, &chproto.Exception{Code: 252, Message: "storage_integrity: back-pressure: retry later" + chproto.SeqUnspentSuffix}); err != nil {
		t.Fatal(err)
	}
	p.OnQueryComplete(context.Background(), sess)

	next := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	second := signDeferred(t, p, next)
	if seqOf(t, first) != 1 || seqOf(t, second) != 1 || seq.Last() != 1 || second == first {
		t.Fatalf("first=%q second=%q last=%d; want seq 1 reused with a new nonce", first, second, seq.Last())
	}
	if metrics.recycled != 1 || len(metrics.burned) != 0 {
		t.Fatalf("recycled=%d burned=%v", metrics.recycled, metrics.burned)
	}
}

// A marked refusal delivered where the deferred lane expects the upstream
// sample block reaches the plugin in Relay's order for that step: abort (the
// deferred input stops), then the Exception, then completion
// (TestRelay_DeferredInsert_MarkedSampleStepExceptionReachesOnException).
// The abort must not drop the reservation, so the seq is recycled (final
// review M3 (c)).
func TestLateReservation_SampleStepHookOrderRecycles(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")
	q := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	first := signDeferred(t, p, q)
	p.OnQueryAbort(context.Background(), q)
	if err := p.OnException(context.Background(), sess, &chproto.Exception{Code: 497, Message: "storage_integrity: 0xa is not a writer of database shop" + chproto.SeqUnspentSuffix}); err != nil {
		t.Fatal(err)
	}
	p.OnQueryComplete(context.Background(), sess)

	next := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	second := signDeferred(t, p, next)
	if seqOf(t, first) != 1 || seqOf(t, second) != 1 || seq.Last() != 1 {
		t.Fatalf("first=%q second=%q last=%d; want seq 1 recycled", first, second, seq.Last())
	}
	if metrics.recycled != 1 || len(metrics.burned) != 0 {
		t.Fatalf("recycled=%d burned=%v; want one recycle and no burn", metrics.recycled, metrics.burned)
	}
}

func TestLateReservation_OtherOutcomesBurn(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")

	plain := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), plain)
	signDeferred(t, p, plain)
	_ = p.OnException(context.Background(), sess, &chproto.Exception{Code: 403, Message: "storage_integrity ingress: orchestrate failed"})
	p.OnQueryComplete(context.Background(), sess)

	ok := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), ok)
	id := signDeferred(t, p, ok)
	p.OnQuerySuccess(context.Background(), sess, id)
	p.OnQueryComplete(context.Background(), sess)

	if seq.Last() != 2 || metrics.burned["unknown_outcome"] != 1 || metrics.recycled != 0 {
		t.Fatalf("last=%d burned=%v recycled=%d; want the unmarked failure burned and the success not counted", seq.Last(), metrics.burned, metrics.recycled)
	}
}

type failingStatementSigner struct{ *auth.RelaySigner }

func (failingStatementSigner) SignStatementV2(auth.JWSStatementPayloadV2) (string, error) {
	return "", errors.New("hsm unavailable")
}

func TestLateReservation_SigningFailureReleasesDirectly(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	relay, _ := auth.NewRelaySigner(testKey)
	p.signer = failingStatementSigner{RelaySigner: relay}
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err != nil {
		t.Fatal(err)
	}
	err := p.OnQueryInputCompleteStrict(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "hsm unavailable") {
		t.Fatalf("err = %v", err)
	}
	if got, _ := seq.Reserve(); got != 1 || metrics.recycled != 1 {
		t.Fatalf("next Reserve = %d recycled=%d; want the released seq 1", got, metrics.recycled)
	}
}

// The agent sees the server's composed refusal text; the marker is matched as
// a suffix of the trimmed message, never by prefix or equality, and a marker
// that is not the suffix does not recycle.
func TestLateReservation_MarkerIsMatchedAsSuffixOfComposedText(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")

	inner := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), inner)
	signDeferred(t, p, inner)
	_ = p.OnException(context.Background(), sess, &chproto.Exception{Code: 403, Message: "upstream said" + chproto.SeqUnspentSuffix + " and then more"})
	p.OnQueryComplete(context.Background(), sess)
	if metrics.recycled != 0 || metrics.burned["unknown_outcome"] != 1 {
		t.Fatalf("a non-suffix marker recycled: recycled=%d burned=%v", metrics.recycled, metrics.burned)
	}

	composed := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), composed)
	id := signDeferred(t, p, composed)
	msg := "storage_integrity admission rejected for " + id + ": storage_integrity: statement " + id + " rejected by the arbiter: ADMISSION_CODE_SCHEMA_NOT_ALLOWED" + chproto.SeqUnspentSuffix + "\n"
	_ = p.OnException(context.Background(), sess, &chproto.Exception{Code: 392, Message: msg})
	// A repeated Exception for the same statement must not release twice.
	_ = p.OnException(context.Background(), sess, &chproto.Exception{Code: 392, Message: msg})
	p.OnQueryComplete(context.Background(), sess)
	if metrics.recycled != 1 || seqOf(t, id) != 2 {
		t.Fatalf("composed marker: recycled=%d id=%q", metrics.recycled, id)
	}
	if got, _ := seq.Reserve(); got != 2 {
		t.Fatalf("next Reserve = %d, want the recycled seq 2", got)
	}
}

func TestLateReservation_CloseWithOutstandingSeqBurns(t *testing.T) {
	p, _, metrics := lateFixture(t)
	sess := newSession(1, "")
	q := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), q)
	signDeferred(t, p, q)
	p.OnClose(sess)
	p.OnClose(sess)
	if metrics.burned["unknown_outcome"] != 1 || metrics.recycled != 0 {
		t.Fatalf("burned=%v recycled=%d; want one unknown_outcome burn", metrics.burned, metrics.recycled)
	}
}
