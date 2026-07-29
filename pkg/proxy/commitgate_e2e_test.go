package proxy

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// commitgate_e2e_test exercises the commitgate plugin in the context of a
// real *plugin.PluginChain — the same chain Relay uses in production —
// with a vetoing observer to verify that:
//
//   - Success path: the chain dispatches OnQuery to commitgate, the
//     observer subscribed to the rewriter-classified StatementType is
//     invoked, and the chain returns nil.
//   - Veto path: the observer's error is wrapped by commitgate (with the
//     statement-type tag) and surfaced through PluginChain.OnQuery.
//
// We intentionally drive the chain directly (Approach 1 from the Phase 5
// plan) rather than the full Relay loop. The Relay's behavior on
// PluginChain.OnQuery error — synthesizing a ClickHouse Exception toward
// the client and skipping upstream forwarding — lives in
// Relay.clientToUpstream and is exercised by the existing relay tests.
// What is new for commitgate is the contract that, when wired into a
// PluginChain, the plugin's veto cleanly propagates back to the chain
// caller without false positives or swallowed errors. That is what this
// test pins down.

// recordingObserver is a minimal commitgate.Observer for tests.
type recordingObserver struct {
	types     []sqlmeta.StatementType
	returnErr error
	calls     []*commitgate.Event
}

func (r *recordingObserver) SubscribedTypes() []sqlmeta.StatementType { return r.types }

func (r *recordingObserver) BeforeStatement(_ context.Context, ev *commitgate.Event) error {
	r.calls = append(r.calls, ev)
	return r.returnErr
}

func (r *recordingObserver) OnStatementException(_ context.Context, _ *commitgate.Event, _ *chproto.Exception) {
}

// commitgateTestSession is a minimal chsession.Session backed by a
// SessionState. The chain reads State() (for IsRouted / Account) and
// nothing else.
type commitgateTestSession struct {
	state *chsession.SessionState
}

func (s *commitgateTestSession) ID() int64                      { return 0 }
func (s *commitgateTestSession) State() *chsession.SessionState { return s.state }
func (s *commitgateTestSession) Client() *chproto.Codec         { return nil }
func (s *commitgateTestSession) Upstream() *chproto.Codec       { return nil }
func (s *commitgateTestSession) RemoteAddr() net.Addr           { return nil }
func (s *commitgateTestSession) Close() error                   { return nil }
func (s *commitgateTestSession) BindUpstream(context.Context, *chproto.Codec) error {
	return nil
}
func (s *commitgateTestSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *commitgateTestSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *commitgateTestSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

func newCommitgateSession(user string) chsession.Session {
	st := chsession.NewSessionState()
	st.Identity.UserID = user
	return &commitgateTestSession{state: st}
}

// buildCreateTableQctx mimics what the rewrite plugin would populate on
// a CREATE TABLE that resolves to (logical "foo", table "t"). The
// commitgate plugin reads StatementType + AccessedTables; nothing else
// from the rewriter pipeline matters for this test.
func buildCreateTableQctx() *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:       newCommitgateSession("alice"),
		OriginalSQL:   "CREATE TABLE t (id UInt64) ENGINE=MergeTree ORDER BY id",
		RewrittenSQL:  "CREATE TABLE `foo`.`t` (id UInt64) ENGINE=MergeTree ORDER BY id",
		Query:         &chproto.Query{ID: "qid-e2e"},
		StatementType: sqlmeta.StatementTypeCreateTable,
		AccessedTables: []sqlmeta.AccessedTable{
			{
				OriginalDatabase: "",
				OriginalTable:    "t",
				LogicalDatabase:  "foo",
				PhysicalDatabase: "physfoo",
			},
		},
	}
}

// Test A: success path through PluginChain.OnQuery.
func TestCommitGate_ChainE2E_SuccessPath(t *testing.T) {
	obs := &recordingObserver{
		types: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
	}
	chain := &plugin.PluginChain{
		QueryPlugins: []plugin.QueryPlugin{commitgate.NewPlugin([]commitgate.Observer{obs})},
	}

	qctx := buildCreateTableQctx()
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("chain.OnQuery returned error on success path: %v", err)
	}
	if len(obs.calls) != 1 {
		t.Fatalf("observer fired %d times, want 1", len(obs.calls))
	}
	ev := obs.calls[0]
	if ev.Type != sqlmeta.StatementTypeCreateTable {
		t.Errorf("Event.Type=%s, want CREATE_TABLE", ev.Type)
	}
	if len(ev.AccessedTables) != 1 {
		t.Fatalf("Event.AccessedTables len=%d, want 1", len(ev.AccessedTables))
	}
	if got := ev.AccessedTables[0]; got.LogicalDatabase != "foo" || got.OriginalTable != "t" {
		t.Errorf("Event.AccessedTables[0] = %+v, want LogicalDatabase=foo OriginalTable=t", got)
	}
	if ev.QueryID != "qid-e2e" {
		t.Errorf("Event.QueryID=%q, want qid-e2e", ev.QueryID)
	}
}

// Test B: veto path — observer returns an error; PluginChain.OnQuery
// surfaces it back to the caller wrapped with the "commitgate
// (CREATE_TABLE):" prefix. This is the contract Relay relies on to
// decide that the query must NOT be forwarded upstream and that an
// Exception should be synthesized to the client.
func TestCommitGate_ChainE2E_VetoPath(t *testing.T) {
	sentinel := errors.New("on-chain registration failed")
	obs := &recordingObserver{
		types:     []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		returnErr: sentinel,
	}
	chain := &plugin.PluginChain{
		QueryPlugins: []plugin.QueryPlugin{commitgate.NewPlugin([]commitgate.Observer{obs})},
	}

	qctx := buildCreateTableQctx()
	err := chain.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("chain.OnQuery returned nil; want vetoing error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain dropped sentinel: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "commitgate") {
		t.Errorf("error message missing 'commitgate' prefix: %q", msg)
	}
	if !strings.Contains(msg, "CREATE_TABLE") {
		t.Errorf("error message missing statement-type tag: %q", msg)
	}
	if !strings.Contains(msg, sentinel.Error()) {
		t.Errorf("error message missing observer cause: %q", msg)
	}
	// Observer was invoked exactly once before the chain bailed.
	if len(obs.calls) != 1 {
		t.Errorf("observer fired %d times on veto, want 1", len(obs.calls))
	}
}
