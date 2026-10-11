package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

func TestStatementTokenSettingMatchesAuth(t *testing.T) {
	if statementTokenSetting != auth.StatementTokenSettingKey {
		t.Fatalf("relay statement-token key %q drifted from auth %q", statementTokenSetting, auth.StatementTokenSettingKey)
	}
}

func TestExceptionForPluginError_RendersUnspentMarkerOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"client error": {fmt.Errorf("strict: %w", chproto.MarkSeqUnspent(&chproto.ClientError{Code: 497, Message: "storage_integrity: 0xa is not a writer of database d"})), "storage_integrity: 0xa is not a writer of database d [client_seq unspent]"},
		"field":        {&chproto.ClientError{Code: 252, Message: "storage_integrity: back-pressure: retry later", SeqUnspent: true}, "storage_integrity: back-pressure: retry later [client_seq unspent]"},
		"plain":        {chproto.MarkSeqUnspent(errors.New("storage_integrity rejects compressed payloads")), "storage_integrity rejects compressed payloads [client_seq unspent]"},
		"already":      {chproto.MarkSeqUnspent(errors.New("x [client_seq unspent]")), "x [client_seq unspent]"},
		"unmarked":     {errors.New("jws invalid"), "jws invalid"},
	} {
		if got := exceptionForPluginError(tc.err).Message; got != tc.want {
			t.Errorf("%s: message = %q, want %q", name, got, tc.want)
		}
	}
}

// Only the typed flag may produce the marker: an unflagged refusal whose text
// happens to end with it (for example user-controlled text quoted at the end
// of a message) is rendered without it, so the agent never recycles a seq a
// post-submission refusal may have spent (final review M9).
func TestExceptionForPluginError_StripsAnUnflaggedMarker(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"plain":        {errors.New(`storage_integrity: rejected "x [client_seq unspent]"` + chproto.SeqUnspentSuffix), `storage_integrity: rejected "x [client_seq unspent]"`},
		"client error": {&chproto.ClientError{Code: 392, Message: "storage_integrity: refused" + chproto.SeqUnspentSuffix}, "storage_integrity: refused"},
		"repeated":     {errors.New("x" + chproto.SeqUnspentSuffix + chproto.SeqUnspentSuffix + " "), "x"},
		"flagged":      {chproto.MarkSeqUnspent(errors.New("x" + chproto.SeqUnspentSuffix)), "x" + chproto.SeqUnspentSuffix},
	} {
		got := exceptionForPluginError(tc.err).Message
		if got != tc.want {
			t.Errorf("%s: message = %q, want %q", name, got, tc.want)
		}
		if !chproto.IsSeqUnspent(tc.err) && chproto.HasSeqUnspentSuffix(got) {
			t.Errorf("%s: an unflagged refusal rendered the marker: %q", name, got)
		}
	}
}

func TestSessionPreservingIngressException_AcceptsMarker(t *testing.T) {
	for _, exc := range []*chproto.Exception{
		{Code: proto.Error(chproto.CodeTooManyParts), Message: "storage_integrity: back-pressure: retry later" + chproto.SeqUnspentSuffix},
		{Code: proto.Error(chproto.CodeTableIsBeingRestarted), Message: chproto.TableActivatingMessage("db1.t") + chproto.SeqUnspentSuffix},
		{Code: proto.Error(chproto.CodeQueryIsProhibited), Message: chproto.TableNoLongerAcceptsWritesMessage("db1.t") + chproto.SeqUnspentSuffix},
		{Code: proto.Error(chproto.CodeTableIsBeingRestarted), Message: chproto.SourceUnavailableMessage("db1.t") + chproto.SeqUnspentSuffix},
	} {
		if !isSessionPreservingIngressException(exc) {
			t.Errorf("marked %d %q must stay session-preserving", exc.Code, exc.Message)
		}
	}
}

type onQueryRejectHooks struct {
	plugin.NoopHooks
	err error
}

func (h *onQueryRejectHooks) OnQuery(context.Context, *plugin.QueryContext) error { return h.err }

func encodeQueryWithSettings(t *testing.T, id, sql string, settings []proto.Setting) []byte {
	t.Helper()
	var qb proto.Buffer
	(&proto.Query{
		ID: id, Body: sql, Settings: settings,
		Info: proto.ClientInfo{
			ProtocolVersion: deferredTestRev, Major: 24, Minor: 1,
			Interface: proto.InterfaceTCP,
			Query:     proto.ClientQueryInitial,
		},
	}).EncodeAware(&qb, deferredTestRev)
	return append([]byte(nil), qb.Buf...)
}

func signedSettings() []proto.Setting {
	return []proto.Setting{{Key: auth.StatementTokenSettingKey, Value: "'tok'", Custom: true}}
}

// Spec 2026-10-09 §6.6 (1): nothing in the OnQuery chain can reach the
// arbiter, so a token-bearing refusal there is marked; a Query without the
// token is not.
func TestRelay_OnQueryRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	for name, tc := range map[string]struct {
		settings []proto.Setting
		want     string
	}{
		"signed":   {signedSettings(), "storage_integrity: refused [client_seq unspent]"},
		"unsigned": {nil, "storage_integrity: refused"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newDeferredHarness(t, &onQueryRejectHooks{err: errors.New("storage_integrity: refused")})
			writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", tc.settings))
			exc := readServerException(t, h.clientProxy)
			if exc.Message != tc.want {
				t.Fatalf("message = %q, want %q", exc.Message, tc.want)
			}
			if strings.Count(exc.Message, "[client_seq unspent]") > 1 {
				t.Fatalf("marker rendered twice: %q", exc.Message)
			}
		})
	}
}

type strictDataRejectHooks struct {
	plugin.NoopHooks
}

func (strictDataRejectHooks) OnClientDataStrict(context.Context, *plugin.QueryContext, []byte) error {
	return errors.New("storage_integrity payload exceeds max_payload_bytes")
}

func TestRelay_StrictDataRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	h := newDeferredHarness(t, strictDataRejectHooks{})
	upstreamDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		_, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		upstreamDone <- err
	}()
	writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", signedSettings()))
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	exc := readServerException(t, h.clientProxy)
	if !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("message = %q, want the unspent marker", exc.Message)
	}
	<-upstreamDone
}

// dataLimitHooks forwards every Query and caps its ClientData at one byte, so
// the first non-empty Data packet trips the strict data-limit gate.
type dataLimitHooks struct {
	plugin.NoopHooks
}

func (dataLimitHooks) ClientDataReadLimit(qctx *plugin.QueryContext) (uint64, bool) {
	return 1, qctx != nil
}

func TestRelay_DataLimitRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	h := newDeferredHarness(t, dataLimitHooks{})
	upstreamDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		_, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		upstreamDone <- err
	}()
	writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", signedSettings()))
	if err := <-upstreamDone; err != nil {
		t.Fatalf("upstream read query: %v", err)
	}
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	exc := readServerException(t, h.clientProxy)
	if !strings.Contains(exc.Message, "exceeds remaining payload limit") || !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("message = %q, want the data-limit refusal with the unspent marker", exc.Message)
	}
}

// planHooks installs a conflicting ownership plan from OnQuery, so Relay's own
// pre-forward plan check refuses the Query before anything reaches upstream.
type planHooks struct {
	plugin.NoopHooks
	install func(*plugin.QueryContext)
}

func (h *planHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.install(qctx)
	return nil
}

func TestRelay_PlanConflictRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	for name, install := range map[string]func(*plugin.QueryContext){
		"synthesized insert": func(qctx *plugin.QueryContext) {
			qctx.SynthesizedInsert = &plugin.SynthesizedInsertPlan{}
			qctx.SuppressUpstreamExecution = true
		},
		"deferred insert": func(qctx *plugin.QueryContext) {
			qctx.DeferredInsert = &plugin.DeferredInsertPlan{MaxPayloadBytes: 1}
			qctx.SuppressUpstreamExecution = true
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newDeferredHarness(t, &planHooks{install: install})
			writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", signedSettings()))
			exc := readServerException(t, h.clientProxy)
			if !strings.Contains(exc.Message, "q1") || !chproto.HasSeqUnspentSuffix(exc.Message) {
				t.Fatalf("message = %q, want the plan conflict with the unspent marker", exc.Message)
			}
		})
	}
}

// A Query that arrives while a rejected Query's input is still being drained
// is refused before it is forwarded, so its coordinate is unspent too.
func TestRelay_QueryBeforeRejectedInputCompletedCarriesMarker(t *testing.T) {
	h := newDeferredHarness(t, &onQueryRejectHooks{err: errors.New("storage_integrity: refused")})
	writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", nil))
	if exc := readServerException(t, h.clientProxy); chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("unsigned refusal carried the marker: %q", exc.Message)
	}
	writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q2", "INSERT INTO db.t FORMAT Native", signedSettings()))
	exc := readServerException(t, h.clientProxy)
	if !strings.Contains(exc.Message, "before completing rejected input") || !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("message = %q, want the protocol refusal with the unspent marker", exc.Message)
	}
}

// Spec 2026-10-09 §6.6: Relay never marks an OnQueryInputCompleteStrict
// refusal itself, because the ingress may already have submitted the
// statement there. A signed statement refused at that hook carries the marker
// only when the hook's error is flagged, so a post-submission refusal (here the
// back-pressure conversion after Orchestrate) reaches the agent unmarked.
func TestRelay_StrictInputCompleteRefusalOfSignedQueryRendersOnlyTheHooksFlag(t *testing.T) {
	const message = "storage_integrity: back-pressure: retry later"
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"unmarked": {&chproto.ClientError{Code: chproto.CodeTooManyParts, Message: message, KeepSession: true}, message},
		"marked":   {chproto.MarkSeqUnspent(&chproto.ClientError{Code: chproto.CodeTooManyParts, Message: message, KeepSession: true}), message + chproto.SeqUnspentSuffix},
	} {
		t.Run(name, func(t *testing.T) {
			hooks := &stagedRejectHooks{rejectOne: true, rejectErr: tc.err}
			h := newDeferredHarness(t, hooks)
			empty := encodeEmptyClientData(t)
			sample := encodeServerSampleDataPacket(t, deferredTestRev)

			upDone := make(chan error, 1)
			go func() { upDone <- serveStagedRejectUpstream(t, h.upstreamProxy) }()

			writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", signedSettings()))
			writeAllConn(t, h.clientProxy, empty)
			if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
				t.Fatalf("client sample block = %x, want %x", got, sample)
			}
			writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
			writeAllConn(t, h.clientProxy, empty)

			exc := readServerException(t, h.clientProxy)
			if exc.Code != proto.Error(chproto.CodeTooManyParts) || exc.Message != tc.want {
				t.Fatalf("exception = %d %q, want 252 %q", exc.Code, exc.Message, tc.want)
			}
			waitForRejectCounts(t, hooks)

			writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
			writeAllConn(t, h.clientProxy, empty)
			if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
				t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
			}
			if err := <-upDone; err != nil {
				t.Fatalf("upstream flow: %v", err)
			}
		})
	}
}

// Spec 2026-10-09 §6.6 (1): once an AgentPrepare preparation has started,
// the preparation and the forward authorization may sequence the statement
// upstream, so Relay leaves every later refusal of a signed Query unmarked
// (final review M3 (a)). The prepared Query carries the statement token too,
// so marking at any of these sites would show.
func TestRelay_AgentPreparePostPreparationRefusalIsUnmarked(t *testing.T) {
	for name, hooks := range map[string]relayPrepareHooks{
		"preparation fails": {
			prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
				return plugin.PreparedAgentQuery{}, errors.New("storage_integrity: preparation refused")
			},
		},
		"authorization fails": {
			prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
				return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "INSERT INTO db.t FORMAT Native", Settings: signedSettings()}}, nil
			},
			authorize: func(context.Context, plugin.PreparedAgentQuery) error {
				return errors.New("storage_integrity: forward authorization refused")
			},
		},
		"continuation fails": {
			prepare: func(context.Context) (plugin.PreparedAgentQuery, error) {
				return plugin.PreparedAgentQuery{Query: &chproto.Query{ID: "prepared", Body: "INSERT INTO db.t FORMAT Native", Settings: signedSettings()}}, nil
			},
			onResume: func(*plugin.QueryContext) error { return errors.New("storage_integrity: continuation refused") },
		},
	} {
		t.Run(name, func(t *testing.T) {
			if hooks.intent == nil {
				hooks.intent = func(context.Context, plugin.PreparedAgentQuery) error { return nil }
			}
			if hooks.authorize == nil {
				hooks.authorize = func(context.Context, plugin.PreparedAgentQuery) error { return nil }
			}
			hooks.unknown = func(context.Context, plugin.PreparedAgentQuery) error { return nil }
			clientPeer, clientProxy := net.Pipe()
			upstreamPeer, upstreamProxy := net.Pipe()
			defer clientPeer.Close()
			defer upstreamPeer.Close()
			const rev = chproto.MaxSupportedRevision
			sess := chsession.New(1, clientProxy)
			sess.Client().SetRevision(rev)
			up := chproto.NewCodec(upstreamProxy, chproto.DirToUpstream)
			up.SetRevision(rev)
			if err := sess.BindUpstream(context.Background(), up); err != nil {
				t.Fatalf("BindUpstream: %v", err)
			}
			r := NewRelay(sess, hooks, nil, nil)
			client := chproto.NewCodec(clientPeer, chproto.DirToUpstream)
			client.SetRevision(rev)
			run := make(chan error, 1)
			go func() { run <- r.clientToUpstream(context.Background()) }()
			if err := client.WriteQuery(&chproto.Query{ID: "signed", Body: "INSERT INTO db.t FORMAT Native", Settings: signedSettings()}); err != nil {
				t.Fatalf("write query: %v", err)
			}
			_ = clientPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
			pkt, err := client.ReadPacket(uint64(chproto.ServerExceptionCode))
			if err != nil {
				t.Fatalf("read exception: %v", err)
			}
			exc, ok := pkt.Decoded.(*chproto.Exception)
			want := map[string]string{"preparation fails": "preparation refused", "authorization fails": "forward authorization refused", "continuation fails": "continuation refused"}[name]
			if !ok || !strings.Contains(exc.Message, want) {
				t.Fatalf("client got %#v, want the post-preparation refusal", pkt.Decoded)
			}
			if chproto.HasSeqUnspentSuffix(exc.Message) {
				t.Fatalf("post-preparation refusal carries the unspent marker: %q", exc.Message)
			}
			_ = clientPeer.Close()
			select {
			case <-run:
			case <-time.After(2 * time.Second):
				t.Fatal("relay did not finish")
			}
		})
	}
}

// exceptionRecordingDeferredHooks records each upstream Exception the chain
// sees in the deferred hooks' lifecycle, next to abort/complete/success.
type exceptionRecordingDeferredHooks struct {
	*deferredInsertHooks
}

func (h exceptionRecordingDeferredHooks) OnException(_ context.Context, _ chsession.Session, exc *chproto.Exception) error {
	h.mu.Lock()
	h.lifecycle = append(h.lifecycle, "exception:"+exc.Message)
	h.mu.Unlock()
	return nil
}

// A server that refuses a signed INSERT in its OnQuery chain answers the
// agent's forwarded Query with a marked Exception where the agent's deferred
// lane expects the sample block. The agent reserved the seq at its strict
// input-complete hook before forwarding, so that Exception must reach
// OnException, which recycles the seq, before OnQueryComplete burns any seq
// still outstanding (final review M3 (c)). Relay fires OnQueryAbort first,
// when it stops the deferred input; sistatement's abort hook leaves the
// reservation alone, which TestLateReservation_SampleStepHookOrderRecycles
// pins with exactly the order asserted here.
func TestRelay_DeferredInsert_MarkedSampleStepExceptionReachesOnException(t *testing.T) {
	hooks := exceptionRecordingDeferredHooks{&deferredInsertHooks{}}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	refusal := "storage_integrity: 0xa is not a writer of database db" + chproto.SeqUnspentSuffix

	upDone := make(chan error, 1)
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
		upDone <- codec.WriteException(&chproto.Exception{Code: 497, Name: "DB::Exception", Message: refusal})
		_, _ = codec.ReadPacket()
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	exc := readServerException(t, h.clientProxy)
	if exc.Code != 497 || exc.Message != refusal {
		t.Fatalf("client got %d %q, want the marked refusal unchanged", exc.Code, exc.Message)
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if _, strictComplete, _, _, _ := hooks.counts(); strictComplete != 1 {
		t.Fatalf("strict input-complete hook ran %d times, want 1: the seq is reserved before forwarding", strictComplete)
	}
	_, lifecycle := hooks.terminalCounts()
	if want := []string{"abort", "exception:" + refusal, "complete"}; strings.Join(lifecycle, "|") != strings.Join(want, "|") {
		t.Fatalf("lifecycle = %q, want %q: the marked Exception reaches OnException before OnQueryComplete", lifecycle, want)
	}
}
