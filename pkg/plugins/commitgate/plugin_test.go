package commitgate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/sqlmeta"
)

// fakeObserver records each BeforeStatement call so tests can
// assert dispatch order, payload contents, and ctx cancellation.
type fakeObserver struct {
	types []sqlmeta.StatementType
	// returnErr, when non-nil, is returned from BeforeStatement.
	returnErr error
	// calls accumulates the events received in invocation order.
	calls []*Event
	// ctxs accumulates the contexts received (parallel to calls).
	ctxs []context.Context

	// excCalls / excEvents / excExcs capture OnStatementException
	// dispatches in invocation order.
	excCalls  int
	excEvents []*Event
	excExcs   []*chproto.Exception
	// panicOnException, when true, makes OnStatementException panic.
	panicOnException bool
}

func (f *fakeObserver) SubscribedTypes() []sqlmeta.StatementType { return f.types }

func (f *fakeObserver) BeforeStatement(ctx context.Context, ev *Event) error {
	f.calls = append(f.calls, ev)
	f.ctxs = append(f.ctxs, ctx)
	return f.returnErr
}

func (f *fakeObserver) OnStatementException(_ context.Context, ev *Event, exc *chproto.Exception) {
	f.excCalls++
	f.excEvents = append(f.excEvents, ev)
	f.excExcs = append(f.excExcs, exc)
	if f.panicOnException {
		panic("fakeObserver: simulated OnStatementException panic")
	}
}

// stateOnlySession is a minimal Session backed by SessionState.
// Mirrors the same pattern in pkg/plugins/sessionstate/tracker_test.go.
type stateOnlySession struct {
	state *chsession.SessionState
}

func (s *stateOnlySession) ID() int64                                          { return 0 }
func (s *stateOnlySession) State() *chsession.SessionState                     { return s.state }
func (s *stateOnlySession) Client() *chproto.Codec                             { return nil }
func (s *stateOnlySession) Upstream() *chproto.Codec                           { return nil }
func (s *stateOnlySession) RemoteAddr() net.Addr                               { return nil }
func (s *stateOnlySession) Close() error                                       { return nil }
func (s *stateOnlySession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *stateOnlySession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *stateOnlySession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *stateOnlySession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

func newSession(user string) chsession.Session {
	st := chsession.NewSessionState()
	st.Identity.UserID = user
	return &stateOnlySession{state: st}
}

// newQctx returns a QueryContext usable by the plugin.
func newQctx(t sqlmeta.StatementType, tables []sqlmeta.AccessedTable) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:        newSession("alice"),
		OriginalSQL:    "<original>",
		RewrittenSQL:   "<rewritten>",
		Query:          &chproto.Query{ID: "qid-1"},
		StatementType:  t,
		AccessedTables: tables,
	}
}

// newPrivQctx returns a QueryContext for a GRANT/REVOKE statement
// driven by privileges_deltas instead of accessed_tables.
func newPrivQctx(t sqlmeta.StatementType, deltas []sqlmeta.PrivilegeDelta) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:          newSession("alice"),
		OriginalSQL:      "<original>",
		RewrittenSQL:     "<rewritten>",
		Query:            &chproto.Query{ID: "qid-1"},
		StatementType:    t,
		PrivilegesDeltas: deltas,
	}
}

func tableEntry(origDB, origTbl, logDB string) sqlmeta.AccessedTable {
	return sqlmeta.AccessedTable{
		OriginalDatabase: origDB,
		OriginalTable:    origTbl,
		LogicalDatabase:  logDB,
	}
}

// 1. Dispatch by StatementType. Observer subscribed only to CREATE_TABLE
//    fires once for CREATE_TABLE and never for the other types.
func TestOnQuery_DispatchByStatementType(t *testing.T) {
	cases := []struct {
		name     string
		stmtType sqlmeta.StatementType
		wantHits int
	}{
		{"unspecified", sqlmeta.StatementTypeUnspecified, 0},
		{"select", sqlmeta.StatementTypeSelect, 0},
		{"create_table", sqlmeta.StatementTypeCreateTable, 1},
		{"drop_table", sqlmeta.StatementTypeDropTable, 0},
		{"create_database", sqlmeta.StatementTypeCreateDatabase, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
			p := NewPlugin([]Observer{obs})
			qctx := newQctx(tc.stmtType, []sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("OnQuery returned error: %v", err)
			}
			if got := len(obs.calls); got != tc.wantHits {
				t.Errorf("observer fired %d times, want %d", got, tc.wantHits)
			}
		})
	}
}

// 2. Multiple observers subscribed to the same type fire in
//    registration order; first error short-circuits.
func TestOnQuery_MultipleObservers_OrderAndShortCircuit(t *testing.T) {
	first := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		returnErr: errors.New("first failed"),
	}
	second := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{first, second})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "t", "logical1")})
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("expected error from first observer to propagate")
	}
	if len(first.calls) != 1 {
		t.Errorf("first observer fired %d times, want 1", len(first.calls))
	}
	if len(second.calls) != 0 {
		t.Errorf("second observer fired %d times, want 0 (short-circuit)", len(second.calls))
	}

	// Now happy path: both observers fire in order.
	first.returnErr = nil
	first.calls = nil
	second.calls = nil
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(first.calls) != 1 || len(second.calls) != 1 {
		t.Fatalf("got first=%d second=%d, want 1 each", len(first.calls), len(second.calls))
	}
}

// 3. Veto: observer returns an error → plugin wraps it with the
//    statement-type label and leaves the original error reachable
//    via errors.Is.
func TestOnQuery_VetoErrorWrapped(t *testing.T) {
	sentinel := errors.New("on-chain failed")
	obs := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		returnErr: sentinel,
	}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "t", "logical1")})
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain lost the sentinel: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "commitgate (CREATE_TABLE)") {
		t.Errorf("error message missing statement-type tag: %q", msg)
	}
	if !strings.Contains(msg, "on-chain failed") {
		t.Errorf("error message missing observer cause: %q", msg)
	}
}

//  4. Event payload pass-through across the supported DDL shapes.
//     buildEvent does not validate content; AccessedTables flows
//     through verbatim from the rewriter and the observer iterates
//     to apply policy. Covers qualified, unqualified-but-resolved,
//     and database-only forms.
func TestOnQuery_EventExtraction(t *testing.T) {
	cases := []struct {
		name     string
		stmtType sqlmeta.StatementType
		entry    sqlmeta.AccessedTable
	}{
		{
			name:     "create_table_qualified",
			stmtType: sqlmeta.StatementTypeCreateTable,
			entry:    tableEntry("logical1", "events", "logical1"),
		},
		{
			name:     "drop_table_qualified",
			stmtType: sqlmeta.StatementTypeDropTable,
			entry:    tableEntry("logical1", "events", "logical1"),
		},
		{
			name:     "create_table_unqualified_resolved",
			stmtType: sqlmeta.StatementTypeCreateTable,
			entry:    tableEntry("", "events", "logical1"),
		},
		{
			name:     "create_database_qualified",
			stmtType: sqlmeta.StatementTypeCreateDatabase,
			entry:    tableEntry("logical1", "", "logical1"),
		},
		{
			name:     "drop_database_qualified",
			stmtType: sqlmeta.StatementTypeDropDatabase,
			entry:    tableEntry("logical1", "", "logical1"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := &fakeObserver{types: []sqlmeta.StatementType{tc.stmtType}}
			p := NewPlugin([]Observer{obs})
			qctx := newQctx(tc.stmtType, []sqlmeta.AccessedTable{tc.entry})
			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("OnQuery: %v", err)
			}
			if len(obs.calls) != 1 {
				t.Fatalf("observer fired %d times, want 1", len(obs.calls))
			}
			ev := obs.calls[0]
			if ev.Type != tc.stmtType {
				t.Errorf("Type=%s, want %s", ev.Type, tc.stmtType)
			}
			if len(ev.AccessedTables) != 1 {
				t.Fatalf("AccessedTables len=%d, want 1", len(ev.AccessedTables))
			}
			if ev.AccessedTables[0] != tc.entry {
				t.Errorf("AccessedTables[0]=%+v, want %+v", ev.AccessedTables[0], tc.entry)
			}
			if ev.User != "alice" {
				t.Errorf("User=%q, want alice", ev.User)
			}
			if ev.QueryID != "qid-1" {
				t.Errorf("QueryID=%q, want qid-1", ev.QueryID)
			}
			if ev.OriginalSQL != "<original>" {
				t.Errorf("OriginalSQL=%q, want <original>", ev.OriginalSQL)
			}
			if ev.RewrittenSQL != "<rewritten>" {
				t.Errorf("RewrittenSQL=%q, want <rewritten>", ev.RewrittenSQL)
			}
		})
	}
}

// TestOnQuery_OwnerFromPayerSetting verifies that when the sidecar
// injects SQL_x_payer (operator-on-behalf-of-owner mode), buildEvent
// surfaces the owner address as Event.Owner — case-folded, quote-
// stripped, and zeroed-out when it equals the JWS signer.
func TestOnQuery_OwnerFromPayerSetting(t *testing.T) {
	cases := []struct {
		name      string
		settings  []chproto.Setting
		signer    string
		wantOwner string
	}{
		{
			name:      "no_payer_setting",
			settings:  nil,
			signer:    "0xa1e4252cfc8f1a14350d4b25ee2f97809a177117",
			wantOwner: "",
		},
		{
			name: "payer_quote_wrapped",
			settings: []chproto.Setting{{
				Key:    "SQL_x_payer",
				Value:  "'0x8bc30C01497Bec83901E7d2D63502aC311370161'",
				Custom: true,
			}},
			signer:    "0xa1e4252cfc8f1a14350d4b25ee2f97809a177117",
			wantOwner: "0x8bc30c01497bec83901e7d2d63502ac311370161",
		},
		{
			name: "payer_equals_signer_zeroed",
			settings: []chproto.Setting{{
				Key:    "SQL_x_payer",
				Value:  "'0xA1E4252cfC8f1a14350d4B25Ee2f97809a177117'",
				Custom: true,
			}},
			signer:    "0xa1e4252cfc8f1a14350d4b25ee2f97809a177117",
			wantOwner: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateDatabase}}
			p := NewPlugin([]Observer{obs})
			qctx := newQctx(sqlmeta.StatementTypeCreateDatabase, []sqlmeta.AccessedTable{tableEntry("logical1", "", "logical1")})
			qctx.Session = newSession(tc.signer)
			qctx.Query.Settings = tc.settings
			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("OnQuery: %v", err)
			}
			if len(obs.calls) != 1 {
				t.Fatalf("observer fired %d times, want 1", len(obs.calls))
			}
			ev := obs.calls[0]
			if ev.Owner != tc.wantOwner {
				t.Errorf("Owner=%q, want %q", ev.Owner, tc.wantOwner)
			}
			// User stays the JWS signer regardless of owner claim.
			if ev.User != tc.signer {
				t.Errorf("User=%q, want %q", ev.User, tc.signer)
			}
		})
	}
}

//  5. Empty AccessedTables / empty LogicalDatabase no longer abort at
//     the framework level — the observer sees the empty shape and
//     decides. Mirrors the SELECT-1 / FROM-less compute path that the
//     PermissionObserver allows but a stricter observer could reject.
func TestOnQuery_EmptyAccessedTables_DispatchesToObserver(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeSelect}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeSelect, nil)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times; want 1 (framework should not pre-reject empty AccessedTables)", len(obs.calls))
	}
	if got := obs.calls[0].AccessedTables; len(got) != 0 {
		t.Errorf("AccessedTables=%+v, want empty", got)
	}
}

// 7. StatementTypeUnspecified is a no-op even when an observer would
//    otherwise be subscribed — buildEvent is never called, so an
//    invalid AccessedTables shape doesn't surface as an error.
func TestOnQuery_StatementTypeUnspecified_NoOp(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	// Empty AccessedTables would normally error; with Unspecified
	// we never enter buildEvent, so this should pass cleanly.
	qctx := newQctx(sqlmeta.StatementTypeUnspecified, nil)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 0 {
		t.Errorf("observer fired %d times; want 0", len(obs.calls))
	}
}

// stashedEvent is a helper that exercises OnQuery and returns the
// Event that was stashed on the session (or nil if none was).
func stashedEvent(sess chsession.Session) *Event {
	raw := sess.State().Snapshot().CommitGateEvent
	if raw == nil {
		return nil
	}
	ev, _ := raw.(*Event)
	return ev
}

// 9. OnQuery stashes the Event on success.
func TestOnQuery_StashesEventOnSuccess(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	stashed := stashedEvent(qctx.Session)
	if stashed == nil {
		t.Fatal("CommitGateEvent not stashed after successful OnQuery")
	}
	if stashed.Type != sqlmeta.StatementTypeCreateTable {
		t.Errorf("stashed.Type=%s, want CREATE_TABLE", stashed.Type)
	}
	if stashed.Values == nil {
		t.Error("stashed.Values is nil; want non-nil scratch map")
	}
}

// 10. OnQuery does NOT stash the Event when an observer vetoes.
func TestOnQuery_VetoDoesNotStash(t *testing.T) {
	obs := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		returnErr: errors.New("veto"),
	}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("expected veto error")
	}
	if got := stashedEvent(qctx.Session); got != nil {
		t.Errorf("CommitGateEvent stashed despite veto: %+v", got)
	}
}

// 11. OnException dispatches the stashed Event to subscribed
//
//	observers with the original Exception.
func TestOnException_DispatchesToObservers(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	exc := &chproto.Exception{Code: 47, Name: "DB::Exception", Message: "boom"}
	if err := p.OnException(context.Background(), qctx.Session, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if obs.excCalls != 1 {
		t.Fatalf("OnStatementException fired %d times, want 1", obs.excCalls)
	}
	if obs.excEvents[0].Type != sqlmeta.StatementTypeCreateTable {
		t.Errorf("dispatched ev.Type=%s, want CREATE_TABLE", obs.excEvents[0].Type)
	}
	if obs.excExcs[0] != exc {
		t.Errorf("dispatched exc=%p, want %p (same pointer)", obs.excExcs[0], exc)
	}
}

// 12. OnException with no stashed Event is a no-op (e.g. empty
//
//	session, statement type wasn't subscribed, or chain aborted
//	before stash).
func TestOnException_NoStash_NoOp(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	sess := newSession("alice")

	exc := &chproto.Exception{Code: 1, Message: "x"}
	if err := p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if obs.excCalls != 0 {
		t.Errorf("OnStatementException fired %d times; want 0", obs.excCalls)
	}
}

// 13. OnException with a stashed Event whose Type has no subscribed
//
//	observers does not fire any observer.
func TestOnException_DispatchesByType(t *testing.T) {
	createObs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	dropObs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeDropTable}}
	p := NewPlugin([]Observer{createObs, dropObs})

	// Stash a DropTable Event via OnQuery on the dropObs subscription.
	qctx := newQctx(sqlmeta.StatementTypeDropTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	exc := &chproto.Exception{Code: 60, Message: "x"}
	if err := p.OnException(context.Background(), qctx.Session, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if createObs.excCalls != 0 {
		t.Errorf("CreateTable observer fired %d times for DropTable event; want 0", createObs.excCalls)
	}
	if dropObs.excCalls != 1 {
		t.Errorf("DropTable observer fired %d times; want 1", dropObs.excCalls)
	}
}

// 14. OnQueryComplete clears the stashed Event.
func TestOnQueryComplete_ClearsStash(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if stashedEvent(qctx.Session) == nil {
		t.Fatal("expected stashed event before OnQueryComplete")
	}
	p.OnQueryComplete(context.Background(), qctx.Session)
	if got := stashedEvent(qctx.Session); got != nil {
		t.Errorf("OnQueryComplete did not clear stash: %+v", got)
	}
}

// 15. A panicking observer's OnStatementException does not abort
//
//	dispatch to the next observer, and OnException returns nil.
func TestOnException_ObserverPanic_Contained(t *testing.T) {
	bad := &fakeObserver{
		types:            []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		panicOnException: true,
	}
	good := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{bad, good})

	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	exc := &chproto.Exception{Code: 1, Message: "x"}
	if err := p.OnException(context.Background(), qctx.Session, exc); err != nil {
		t.Fatalf("OnException returned error despite recover: %v", err)
	}
	if bad.excCalls != 1 {
		t.Errorf("panicking observer fired %d times; want 1", bad.excCalls)
	}
	if good.excCalls != 1 {
		t.Errorf("good observer fired %d times after panic; want 1 (panic should not abort dispatch)", good.excCalls)
	}
}

// 16. ErrAbortWithSuccess sentinel: BeforeStatement returns the sentinel,
//
//	OnQuery returns nil (no error surfaced to the relay) and flips
//	qctx.AbortWithSuccess so the relay writes a synthetic EndOfStream
//	instead of forwarding to ClickHouse.
func TestOnQuery_AbortWithSuccessSentinel(t *testing.T) {
	obs := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateDatabase},
		returnErr: ErrAbortWithSuccess,
	}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateDatabase,
		[]sqlmeta.AccessedTable{tableEntry("tenant1", "", "tenant1")})

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if !qctx.AbortWithSuccess {
		t.Errorf("qctx.AbortWithSuccess=false, want true")
	}
}

// 17. ErrAbortWithSuccess sentinel survives %w-wrapping: a wrapped
//
//	error is still detected via errors.Is and produces the same
//	soft-success outcome.
func TestOnQuery_AbortWithSuccessSentinel_Wrapped(t *testing.T) {
	obs := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateDatabase},
		returnErr: fmt.Errorf("create-db: %w", ErrAbortWithSuccess),
	}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateDatabase,
		[]sqlmeta.AccessedTable{tableEntry("tenant1", "", "tenant1")})

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if !qctx.AbortWithSuccess {
		t.Errorf("qctx.AbortWithSuccess=false, want true")
	}
}

// 18. Maintenance sessions short-circuit the dispatch: even when a
//     subscribed observer is registered, OnQuery must NOT fire it. The
//     guard exists so the GC path (which executes its DDL under a
//     SQL_sentio_maintenance flag) cannot retrigger host observers
//     against state they have already committed.
func TestOnQuery_MaintenanceSkipsObservers(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateDatabase}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateDatabase,
		[]sqlmeta.AccessedTable{tableEntry("tenant1", "", "tenant1")})
	qctx.Session.State().SetMaintenance(true)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 0 {
		t.Errorf("observer fired %d times under Maintenance, want 0", len(obs.calls))
	}
	if got := stashedEvent(qctx.Session); got != nil {
		t.Errorf("CommitGateEvent stashed under Maintenance: %+v", got)
	}
	if qctx.AbortWithSuccess {
		t.Errorf("qctx.AbortWithSuccess=true under Maintenance, want false (skip != soft-success)")
	}
}

// 19. Event.Settings carries the per-query Settings the client supplied
//     (e.g. x_auth_token, SQL_sentio_routed) so observers can branch on
//     trust-domain markers without reaching back to the Query packet.
func TestOnQuery_PopulatesEventSettings(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	qctx.Query.Settings = []chproto.Setting{
		{Key: "SQL_sentio_routed", Value: "1"},
		{Key: "x_auth_token", Value: "tok-abc"},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	got := obs.calls[0].Settings
	if got == nil {
		t.Fatalf("Event.Settings is nil; want populated map")
	}
	if got["SQL_sentio_routed"] != "1" {
		t.Errorf("Event.Settings[SQL_sentio_routed]=%q, want %q", got["SQL_sentio_routed"], "1")
	}
	if got["x_auth_token"] != "tok-abc" {
		t.Errorf("Event.Settings[x_auth_token]=%q, want %q", got["x_auth_token"], "tok-abc")
	}
	if len(got) != 2 {
		t.Errorf("Event.Settings has %d entries, want 2: %+v", len(got), got)
	}
}

// 8. Cancelled ctx is propagated to BeforeStatement.
func TestOnQuery_ContextCancellation_Propagates(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeCreateTable,
		[]sqlmeta.AccessedTable{tableEntry("", "events", "logical1")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery returned error: %v", err)
	}
	if len(obs.ctxs) != 1 {
		t.Fatalf("observer received %d contexts, want 1", len(obs.ctxs))
	}
	if obs.ctxs[0].Err() == nil {
		t.Errorf("observer ctx not cancelled (Err()=%v)", obs.ctxs[0].Err())
	}
}

func TestPlugin_RunOnForward_False(t *testing.T) {
	var p plugin.ForwardAware = (*Plugin)(nil)
	if p.RunOnForward() {
		t.Errorf("commitgate must opt out of forwarded sessions")
	}
}

// Peer-trusted sessions arrive on the inbound side of a proxy-to-proxy
// remote() loopback. The originating proxy already ran rewriter +
// commitgate; the inner SQL here is unclassified (rewrite is also
// PeerTrust-opt-out), so commitgate must skip itself rather than reject
// every peer-trusted query on Unspecified.
func TestPlugin_RunOnPeerTrust_False(t *testing.T) {
	var p plugin.PeerTrustAware = (*Plugin)(nil)
	if p.RunOnPeerTrust() {
		t.Errorf("commitgate must opt out of peer-trusted sessions")
	}
}

// 20. GRANT dispatches via privileges_deltas: Database / Table mirror
//
//	the first delta's resolved logical/original-table values, and the
//	full deltas slice is surfaced on the Event for observers to consume.
func TestOnQuery_GrantDispatchesViaDeltas(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeGrant}}
	p := NewPlugin([]Observer{obs})
	deltas := []sqlmeta.PrivilegeDelta{
		{
			Action:           sqlmeta.PrivilegeActionGrant,
			Scope:            sqlmeta.PrivilegeScopeTable,
			OriginalDatabase: "tenant1",
			LogicalDatabase:  "tenant1",
			PhysicalDatabase: "testnet",
			OriginalTable:    "events",
			PhysicalTable:    "tenant1_events",
			Privileges:       []string{"SELECT"},
			Category:         sqlmeta.PrivilegeCategoryRead,
			Grantees:         []sqlmeta.PrivilegeGrantee{{Name: "0xabc"}},
		},
		{
			Action:           sqlmeta.PrivilegeActionGrant,
			Scope:            sqlmeta.PrivilegeScopeTable,
			OriginalDatabase: "tenant1",
			LogicalDatabase:  "tenant1",
			PhysicalDatabase: "testnet",
			OriginalTable:    "events",
			PhysicalTable:    "tenant1_events",
			Privileges:       []string{"INSERT"},
			Category:         sqlmeta.PrivilegeCategoryWrite,
			Grantees:         []sqlmeta.PrivilegeGrantee{{Name: "0xabc"}},
		},
	}
	qctx := newPrivQctx(sqlmeta.StatementTypeGrant, deltas)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	ev := obs.calls[0]
	if ev.Type != sqlmeta.StatementTypeGrant {
		t.Errorf("ev.Type=%s, want GRANT", ev.Type)
	}
	if len(ev.AccessedTables) != 1 {
		t.Fatalf("ev.AccessedTables len=%d, want 1 (mirrored from first delta)", len(ev.AccessedTables))
	}
	if got := ev.AccessedTables[0]; got.LogicalDatabase != "tenant1" || got.OriginalTable != "events" {
		t.Errorf("ev.AccessedTables[0] = %+v, want LogicalDatabase=tenant1 OriginalTable=events", got)
	}
	if len(ev.PrivilegesDeltas) != 2 {
		t.Fatalf("ev.PrivilegesDeltas len=%d, want 2", len(ev.PrivilegesDeltas))
	}
	if ev.PrivilegesDeltas[0].Privileges[0] != "SELECT" || ev.PrivilegesDeltas[1].Privileges[0] != "INSERT" {
		t.Errorf("privileges=%v / %v, want SELECT / INSERT",
			ev.PrivilegesDeltas[0].Privileges, ev.PrivilegesDeltas[1].Privileges)
	}
	wantCat := sqlmeta.PrivilegeCategoryRead | sqlmeta.PrivilegeCategoryWrite
	if ev.PrivilegesCategory != wantCat {
		t.Errorf("ev.PrivilegesCategory=%s, want %s", ev.PrivilegesCategory, wantCat)
	}
}

// 21. REVOKE on database scope works without a table: OriginalTable is
//
//	empty, scope is DATABASE, and the Event surfaces accordingly.
func TestOnQuery_RevokeDatabaseScope(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeRevoke}}
	p := NewPlugin([]Observer{obs})
	deltas := []sqlmeta.PrivilegeDelta{
		{
			Action:           sqlmeta.PrivilegeActionRevoke,
			Scope:            sqlmeta.PrivilegeScopeDatabase,
			OriginalDatabase: "tenant1",
			LogicalDatabase:  "tenant1",
			PhysicalDatabase: "testnet",
			Privileges:       []string{"SELECT"},
			Grantees:         []sqlmeta.PrivilegeGrantee{{Name: "0xabc"}},
		},
	}
	qctx := newPrivQctx(sqlmeta.StatementTypeRevoke, deltas)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	ev := obs.calls[0]
	if len(ev.AccessedTables) != 1 {
		t.Fatalf("ev.AccessedTables len=%d, want 1 (mirrored from first delta)", len(ev.AccessedTables))
	}
	if got := ev.AccessedTables[0]; got.LogicalDatabase != "tenant1" || got.OriginalTable != "" {
		t.Errorf("ev.AccessedTables[0] = %+v, want LogicalDatabase=tenant1 OriginalTable=\"\"", got)
	}
	if len(ev.PrivilegesDeltas) != 1 || ev.PrivilegesDeltas[0].Scope != sqlmeta.PrivilegeScopeDatabase {
		t.Errorf("ev.PrivilegesDeltas=%+v", ev.PrivilegesDeltas)
	}
}

// 22. Empty privileges_deltas no longer abort at the framework level —
//
//	the observer sees the empty Event and decides. Mirrors the
//	"observer owns policy" contract introduced when buildEvent
//	stopped enforcing rewriter-contract checks.
func TestOnQuery_GrantEmptyDeltas_DispatchesToObserver(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeGrant}}
	p := NewPlugin([]Observer{obs})
	qctx := newPrivQctx(sqlmeta.StatementTypeGrant, nil)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times; want 1", len(obs.calls))
	}
	if got := obs.calls[0]; len(got.PrivilegesDeltas) != 0 || len(got.AccessedTables) != 0 {
		t.Errorf("expected fully-empty Event; got AccessedTables=%+v PrivilegesDeltas=%+v",
			got.AccessedTables, got.PrivilegesDeltas)
	}
}

// 25. OnException for a stashed GRANT event dispatches to subscribed
//
//	observers with the original Exception, exercising the same replay
//	path as the DDL types.
func TestOnException_GrantDispatchesToObservers(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeGrant}}
	p := NewPlugin([]Observer{obs})
	deltas := []sqlmeta.PrivilegeDelta{
		{
			Action:           sqlmeta.PrivilegeActionGrant,
			Scope:            sqlmeta.PrivilegeScopeTable,
			OriginalDatabase: "tenant1",
			LogicalDatabase:  "tenant1",
			OriginalTable:    "events",
			Privileges:       []string{"SELECT"},
			Grantees:         []sqlmeta.PrivilegeGrantee{{Name: "0xabc"}},
		},
	}
	qctx := newPrivQctx(sqlmeta.StatementTypeGrant, deltas)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	exc := &chproto.Exception{Code: 497, Name: "DB::Exception", Message: "access denied"}
	if err := p.OnException(context.Background(), qctx.Session, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if obs.excCalls != 1 {
		t.Fatalf("OnStatementException fired %d times, want 1", obs.excCalls)
	}
	if obs.excEvents[0].Type != sqlmeta.StatementTypeGrant {
		t.Errorf("dispatched ev.Type=%s, want GRANT", obs.excEvents[0].Type)
	}
	if len(obs.excEvents[0].PrivilegesDeltas) != 1 {
		t.Errorf("dispatched ev.PrivilegesDeltas=%v", obs.excEvents[0].PrivilegesDeltas)
	}
}

// 26. Multi-target SELECT (e.g. UNION across two databases) flows
//
//	through with every entry surfaced in AccessedTables. The
//	dispatcher must not collapse to a single entry — observers
//	rely on the full list to do per-DB permission checks.
func TestOnQuery_MultiTarget_SelectUnion(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeSelect}}
	p := NewPlugin([]Observer{obs})
	tables := []sqlmeta.AccessedTable{
		tableEntry("d1", "transfer", "d1"),
		tableEntry("d2", "transfer", "d2"),
	}
	qctx := newQctx(sqlmeta.StatementTypeSelect, tables)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	got := obs.calls[0].AccessedTables
	if len(got) != 2 {
		t.Fatalf("AccessedTables len=%d, want 2 (no collapse)", len(got))
	}
	if got[0].LogicalDatabase != "d1" || got[1].LogicalDatabase != "d2" {
		t.Errorf("AccessedTables order/contents = %+v, want d1 then d2", got)
	}
}

// 27. USE / SHOW TABLES synthesise an AccessedTables entry from
//
//	qctx.DatabaseRewrites — the rewriter's contract for these
//	statement kinds (it returns the target in database_rewrites,
//	not original_accessed_tables).
func TestOnQuery_UseDatabase_SynthesisesAccessedTable(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeUse}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeUse, nil)
	qctx.DatabaseRewrites = map[string]string{"chen_test": "testnet"}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	got := obs.calls[0].AccessedTables
	if len(got) != 1 {
		t.Fatalf("AccessedTables len=%d, want 1 (synthesised from DatabaseRewrites)", len(got))
	}
	if got[0].LogicalDatabase != "chen_test" || got[0].PhysicalDatabase != "testnet" {
		t.Errorf("synthesised AccessedTables[0] = %+v, want LogicalDatabase=chen_test PhysicalDatabase=testnet", got[0])
	}
	if got[0].OriginalTable != "" {
		t.Errorf("USE should leave OriginalTable empty; got %q", got[0].OriginalTable)
	}
}

// 28. Known-physical USE (the rewriter forwards the SQL unchanged
//
//	and leaves database_rewrites empty) falls back to the session's
//	LogicalDatabaseName, which the rewrite plugin's
//	maybeUpdateLogicalDatabase has already populated.
func TestOnQuery_UseDatabase_KnownPhysicalFallback(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeUse}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeUse, nil)
	// No DatabaseRewrites — simulates known-physical USE.
	qctx.Session.State().SetLogicalDatabase("testnet")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := obs.calls[0].AccessedTables
	if len(got) != 1 || got[0].LogicalDatabase != "testnet" {
		t.Fatalf("expected synthesis from session.LogicalDatabase=testnet; got %+v", got)
	}
}

// 29. UNSPECIFIED dispatches to a subscribed observer (used by
//
//	PermissionCommitGateObserver to fail-closed on unclassified
//	statements). The dispatcher does NOT pre-empt with an
//	"accessed tables missing" error; the observer's own message
//	surfaces.
func TestOnQuery_Unspecified_DispatchesToSubscribedObserver(t *testing.T) {
	obs := &fakeObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeUnspecified},
		returnErr: fmt.Errorf("permission: rewriter did not classify statement"),
	}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeUnspecified, nil)
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("expected error from observer")
	}
	if !strings.Contains(err.Error(), "did not classify") {
		t.Errorf("observer's message should surface; got %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times; want 1", len(obs.calls))
	}
}

// 30. SELECT with empty AccessedTables (e.g. `SELECT 1`,
//
//	`SELECT version()`) flows through to the observer; the
//	dispatcher does not reject empty entries — observers decide.
func TestOnQuery_FromlessSelect_DispatchesToObserver(t *testing.T) {
	obs := &fakeObserver{types: []sqlmeta.StatementType{sqlmeta.StatementTypeSelect}}
	p := NewPlugin([]Observer{obs})
	qctx := newQctx(sqlmeta.StatementTypeSelect, nil)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times; want 1", len(obs.calls))
	}
	if got := obs.calls[0].AccessedTables; len(got) != 0 {
		t.Errorf("AccessedTables=%+v, want empty (FROM-less SELECT)", got)
	}
}
