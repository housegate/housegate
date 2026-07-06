package plugin

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
)

// fakeSession is a minimal chsession.Session used by chain tests. Only
// State() and ID() are wired up; the codec/upstream methods return nil
// because chain logic never touches them.
type fakeSession struct {
	id    int64
	state *chsession.SessionState
}

func newFakeSession() *fakeSession {
	return &fakeSession{state: chsession.NewSessionState()}
}

func (f *fakeSession) ID() int64                      { return f.id }
func (f *fakeSession) State() *chsession.SessionState { return f.state }
func (f *fakeSession) Client() *chproto.Codec         { return nil }
func (f *fakeSession) Upstream() *chproto.Codec       { return nil }
func (f *fakeSession) RemoteAddr() net.Addr           { return nil }
func (f *fakeSession) Close() error                   { return nil }
func (f *fakeSession) BindUpstream(context.Context, *chproto.Codec) error {
	return nil
}
func (f *fakeSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (f *fakeSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (f *fakeSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

type fakeConnLifecyclePlugin struct {
	name        string
	connects    *[]string
	disconnects *[]string
	connectErr  error
}

func (f *fakeConnLifecyclePlugin) OnConnect(_ context.Context, _ chsession.Session) error {
	*f.connects = append(*f.connects, f.name)
	return f.connectErr
}

func (f *fakeConnLifecyclePlugin) OnDisconnect(_ chsession.Session) {
	*f.disconnects = append(*f.disconnects, f.name)
}

type fakeQueryPlugin struct {
	name       string
	called     *[]string
	returnErr  error
	sqlMutator func(*chproto.Query)
}

func (f *fakeQueryPlugin) OnQuery(_ context.Context, qctx *QueryContext) error {
	*f.called = append(*f.called, f.name)
	if f.sqlMutator != nil {
		f.sqlMutator(qctx.Query)
	}
	return f.returnErr
}

type fakeDataRewritePlugin struct {
	seen    [][]byte
	rewrite []byte
}

func (f *fakeDataRewritePlugin) OnClientData(_ context.Context, _ *QueryContext, raw []byte) error {
	f.seen = append(f.seen, append([]byte(nil), raw...))
	return nil
}

func (f *fakeDataRewritePlugin) RewriteClientData(_ context.Context, _ *QueryContext, raw []byte) ([]byte, error) {
	if f.rewrite == nil {
		return raw, nil
	}
	return append([]byte(nil), f.rewrite...), nil
}

func TestPluginChain_RewriteClientDataFeedsNextPluginAndReturnsRewrittenPacket(t *testing.T) {
	first := &fakeDataRewritePlugin{rewrite: []byte("rewritten")}
	second := &fakeDataRewritePlugin{}
	chain := &PluginChain{DataPlugins: []DataPlugin{first, second}}
	qctx := &QueryContext{Session: newFakeSession(), Query: &chproto.Query{Body: "INSERT INTO t"}}

	got, err := chain.RewriteClientData(context.Background(), qctx, []byte("original"))
	if err != nil {
		t.Fatalf("RewriteClientData: %v", err)
	}
	if string(got) != "rewritten" {
		t.Fatalf("rewritten packet = %q, want rewritten", got)
	}
	if len(first.seen) != 1 || string(first.seen[0]) != "original" {
		t.Fatalf("first plugin saw %q, want original", first.seen)
	}
	if len(second.seen) != 1 || string(second.seen[0]) != "rewritten" {
		t.Fatalf("second plugin saw %q, want rewritten", second.seen)
	}
}

func TestPluginChain_OnQuery_RunsInOrder(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeQueryPlugin{name: "a", called: &called},
			&fakeQueryPlugin{name: "b", called: &called, sqlMutator: func(q *chproto.Query) {
				q.Body = "mutated"
			}},
			&fakeQueryPlugin{name: "c", called: &called},
		},
	}
	qctx := &QueryContext{Session: newFakeSession(), Query: &chproto.Query{Body: "original"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := called; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("order=%v, want [a b c]", got)
	}
	if qctx.Query.Body != "mutated" {
		t.Fatalf("Body=%q, want mutated", qctx.Query.Body)
	}
}

func TestPluginChain_OnConnect_RunsInOrderAndShortCircuits(t *testing.T) {
	var connects, disconnects []string
	chain := &PluginChain{
		ConnLifecyclePlugins: []ConnLifecyclePlugin{
			&fakeConnLifecyclePlugin{name: "a", connects: &connects, disconnects: &disconnects},
			&fakeConnLifecyclePlugin{name: "b", connects: &connects, disconnects: &disconnects},
		},
	}
	if err := chain.OnConnect(context.Background(), nil); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	if len(connects) != 2 || connects[0] != "a" || connects[1] != "b" {
		t.Fatalf("connects=%v, want [a b]", connects)
	}
	chain.OnDisconnect(nil)
	if len(disconnects) != 2 || disconnects[0] != "a" || disconnects[1] != "b" {
		t.Fatalf("disconnects=%v, want [a b]", disconnects)
	}

	// Short-circuit on error: b must not run.
	connects = nil
	chain = &PluginChain{
		ConnLifecyclePlugins: []ConnLifecyclePlugin{
			&fakeConnLifecyclePlugin{name: "a", connects: &connects, disconnects: &disconnects, connectErr: errors.New("reject")},
			&fakeConnLifecyclePlugin{name: "b", connects: &connects, disconnects: &disconnects},
		},
	}
	err := chain.OnConnect(context.Background(), nil)
	if err == nil || err.Error() != "reject" {
		t.Fatalf("err=%v, want reject", err)
	}
	if len(connects) != 1 {
		t.Fatalf("connects=%v, expected to stop after a", connects)
	}
}

func TestPluginChain_OnQuery_ShortCircuitsOnError(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeQueryPlugin{name: "a", called: &called},
			&fakeQueryPlugin{name: "b", called: &called, returnErr: errors.New("reject")},
			&fakeQueryPlugin{name: "c", called: &called}, // must not run
		},
	}
	qctx := &QueryContext{Session: newFakeSession(), Query: &chproto.Query{}}
	err := chain.OnQuery(context.Background(), qctx)
	if err == nil || err.Error() != "reject" {
		t.Fatalf("err=%v, want reject", err)
	}
	if len(called) != 2 {
		t.Fatalf("called=%v, expected to stop after b", called)
	}
}

// --- Route-aware behaviour ---

// fakeRoutedQueryPlugin is a QueryPlugin that opts into routed sessions
// via RouteAware. Used to verify the chain runs route-aware plugins on
// routed sessions while skipping plain ones.
type fakeRoutedQueryPlugin struct {
	name   string
	called *[]string
}

func (f *fakeRoutedQueryPlugin) OnQuery(_ context.Context, _ *QueryContext) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeRoutedQueryPlugin) RunOnRouted() bool { return true }

type fakeHelloPlugin struct {
	name   string
	called *[]string
	mark   bool // if true, mark session as routed when invoked (simulates Stripper)
}

func (f *fakeHelloPlugin) OnHello(_ context.Context, sess chsession.Session, _ *chproto.ClientHello) error {
	*f.called = append(*f.called, f.name)
	if f.mark {
		sess.State().SetRouteTarget("peer:9000")
	}
	return nil
}

type fakeRoutedHelloPlugin struct {
	fakeHelloPlugin
}

func (*fakeRoutedHelloPlugin) RunOnRouted() bool { return true }

type fakeQueryCompletePlugin struct {
	name   string
	called *[]string
}

func (f *fakeQueryCompletePlugin) OnQueryComplete(_ context.Context, _ chsession.Session) {
	*f.called = append(*f.called, f.name)
}

type fakeRoutedQueryCompletePlugin struct {
	fakeQueryCompletePlugin
}

func (*fakeRoutedQueryCompletePlugin) RunOnRouted() bool { return true }

type fakeExceptionPlugin struct {
	name   string
	called *[]string
}

func (f *fakeExceptionPlugin) OnException(_ context.Context, _ chsession.Session, _ *chproto.Exception) error {
	*f.called = append(*f.called, f.name)
	return nil
}

type fakeHandshakeCompletePlugin struct {
	name   string
	called *[]string
}

func (f *fakeHandshakeCompletePlugin) OnHandshakeComplete(_ context.Context, _ chsession.Session, _ time.Duration) {
	*f.called = append(*f.called, f.name)
}

type fakeRoutedHandshakeCompletePlugin struct {
	fakeHandshakeCompletePlugin
}

func (*fakeRoutedHandshakeCompletePlugin) RunOnRouted() bool { return true }

func TestPluginChain_OnQuery_SkipsNonRouteAwareOnRoutedSession(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeQueryPlugin{name: "auth", called: &called},
			&fakeQueryPlugin{name: "rewrite", called: &called},
			&fakeRoutedQueryPlugin{name: "signer", called: &called},
			&fakeRoutedQueryPlugin{name: "metrics", called: &called},
		},
	}

	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")
	qctx := &QueryContext{Session: sess, Query: &chproto.Query{Body: "x"}}

	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(called) != 2 || called[0] != "signer" || called[1] != "metrics" {
		t.Fatalf("called=%v, want [signer metrics]", called)
	}

	// Same chain, regular session: every plugin runs.
	called = nil
	regular := newFakeSession()
	qctx = &QueryContext{Session: regular, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(called) != 4 || called[0] != "auth" || called[1] != "rewrite" || called[2] != "signer" || called[3] != "metrics" {
		t.Fatalf("called=%v, want [auth rewrite signer metrics]", called)
	}
}

func TestPluginChain_OnHello_StopsOnceStripperMarksRouted(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HelloPlugins: []HelloPlugin{
			// "stripper" is route-aware AND marks the session routed.
			&fakeRoutedHelloPlugin{fakeHelloPlugin{name: "stripper", called: &called, mark: true}},
			// These are NOT route-aware: they must be skipped.
			&fakeHelloPlugin{name: "credential", called: &called},
			&fakeHelloPlugin{name: "state", called: &called},
			&fakeHelloPlugin{name: "rewrite", called: &called},
		},
	}
	sess := newFakeSession()
	if err := chain.OnHello(context.Background(), sess, &chproto.ClientHello{}); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if len(called) != 1 || called[0] != "stripper" {
		t.Fatalf("called=%v, want [stripper] only", called)
	}
	if !sess.State().IsRouted() {
		t.Fatalf("expected session to be marked routed")
	}
}

func TestPluginChain_OnHello_RegularSessionRunsAll(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HelloPlugins: []HelloPlugin{
			// Route-aware Stripper that does NOT find a prefix and so
			// does not mark the session — subsequent plugins must run.
			&fakeRoutedHelloPlugin{fakeHelloPlugin{name: "stripper", called: &called, mark: false}},
			&fakeHelloPlugin{name: "credential", called: &called},
			&fakeHelloPlugin{name: "state", called: &called},
		},
	}
	sess := newFakeSession()
	if err := chain.OnHello(context.Background(), sess, &chproto.ClientHello{}); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if len(called) != 3 || called[0] != "stripper" || called[1] != "credential" || called[2] != "state" {
		t.Fatalf("called=%v, want all three", called)
	}
}

func TestPluginChain_OnQueryComplete_SkipsNonRouteAwareOnRouted(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryCompletePlugins: []QueryCompletePlugin{
			&fakeQueryCompletePlugin{name: "concurrency", called: &called},
			&fakeRoutedQueryCompletePlugin{fakeQueryCompletePlugin{name: "metrics", called: &called}},
		},
	}
	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")
	chain.OnQueryComplete(context.Background(), sess)
	if len(called) != 1 || called[0] != "metrics" {
		t.Fatalf("called=%v, want [metrics] only", called)
	}
}

func TestPluginChain_OnException_SkipsNonRouteAwareOnRouted(t *testing.T) {
	var called []string
	chain := &PluginChain{
		ExceptionPlugins: []ExceptionPlugin{
			&fakeExceptionPlugin{name: "rewrite-reverse-map", called: &called},
		},
	}
	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")
	if err := chain.OnException(context.Background(), sess, &chproto.Exception{}); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if len(called) != 0 {
		t.Fatalf("called=%v, expected nothing on routed session", called)
	}
}

func TestPluginChain_OnHandshakeComplete_SkipsNonRouteAwareOnRouted(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HandshakeCompletePlugins: []HandshakeCompletePlugin{
			&fakeHandshakeCompletePlugin{name: "audit", called: &called},
			&fakeRoutedHandshakeCompletePlugin{fakeHandshakeCompletePlugin{name: "metrics", called: &called}},
		},
	}
	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")
	chain.OnHandshakeComplete(context.Background(), sess, time.Millisecond)
	if len(called) != 1 || called[0] != "metrics" {
		t.Fatalf("called=%v, want [metrics] only", called)
	}
}

// Lifecycle hooks (OnConnect / OnDisconnect / OnClose) intentionally
// run unconditionally — see the RouteAware doc for why.
func TestPluginChain_LifecycleHooksAlwaysRun_EvenOnRoutedSession(t *testing.T) {
	var connects, disconnects []string
	chain := &PluginChain{
		ConnLifecyclePlugins: []ConnLifecyclePlugin{
			&fakeConnLifecyclePlugin{name: "rewrite", connects: &connects, disconnects: &disconnects},
		},
	}

	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")

	if err := chain.OnConnect(context.Background(), sess); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	chain.OnDisconnect(sess)

	if len(connects) != 1 || connects[0] != "rewrite" {
		t.Fatalf("connects=%v, want [rewrite] regardless of route status", connects)
	}
	if len(disconnects) != 1 || disconnects[0] != "rewrite" {
		t.Fatalf("disconnects=%v, want [rewrite] regardless of route status", disconnects)
	}
}

// --- Peer-trust marker behaviour ---

// fakeOptOutPeerQueryPlugin opts OUT of peer-trusted sessions.
type fakeOptOutPeerQueryPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutPeerQueryPlugin) OnQuery(_ context.Context, _ *QueryContext) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptOutPeerQueryPlugin) RunOnPeerTrust() bool { return false }

func TestPluginChain_OnQuery_PeerTrustOptOutSkipsMarkedPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeOptOutPeerQueryPlugin{name: "auth", called: &called},
			&fakeQueryPlugin{name: "concurrency", called: &called},
			&fakeOptOutPeerQueryPlugin{name: "rewrite", called: &called},
			&fakeQueryPlugin{name: "metrics", called: &called},
		},
	}

	// Peer-trusted session: marked plugins skipped, default-run
	// plugins fire normally.
	sess := newFakeSession()
	sess.State().SetPeerTrust("0xpeer")
	qctx := &QueryContext{Session: sess, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(called) != 2 || called[0] != "concurrency" || called[1] != "metrics" {
		t.Fatalf("called=%v, want [concurrency metrics]", called)
	}

	// Regular (non-peer-trust) session: every plugin runs.
	called = nil
	regular := newFakeSession()
	qctx = &QueryContext{Session: regular, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery non-peer: %v", err)
	}
	if len(called) != 4 {
		t.Fatalf("called=%v, want 4 plugins on regular session", called)
	}
}

// PeerTrustAware applies on top of RouteAware: a routed session's
// route filter is the dominant filter (it must skip non-RouteAware
// plugins), and the peer-trust filter further narrows the survivors
// when both flags are set on the same session.
func TestPluginChain_OnQuery_PeerTrustAndRouteCombine(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeQueryPlugin{name: "default", called: &called},             // skipped (not RouteAware)
			&fakeRoutedQueryPlugin{name: "signer-routed", called: &called}, // runs
			&fakeRoutedPeerOptOutQueryPlugin{ // route-aware but opts out of peer
				fakeRoutedQueryPlugin: fakeRoutedQueryPlugin{name: "auth-routed-peeropt", called: &called},
			},
		},
	}
	sess := newFakeSession()
	sess.State().SetRouteTarget("peer:9000")
	sess.State().SetPeerTrust("0xpeer")
	qctx := &QueryContext{Session: sess, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(called) != 1 || called[0] != "signer-routed" {
		t.Fatalf("called=%v, want [signer-routed]", called)
	}
}

type fakeRoutedPeerOptOutQueryPlugin struct {
	fakeRoutedQueryPlugin
}

func (*fakeRoutedPeerOptOutQueryPlugin) RunOnPeerTrust() bool { return false }

// --- Forward-aware marker behaviour ---

// fakeOptOutForwardQueryPlugin opts OUT of forwarded sessions.
type fakeOptOutForwardQueryPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutForwardQueryPlugin) OnQuery(_ context.Context, _ *QueryContext) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptOutForwardQueryPlugin) RunOnForward() bool { return false }

// fakeOptInForwardQueryPlugin explicitly opts IN to forwarded sessions.
type fakeOptInForwardQueryPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptInForwardQueryPlugin) OnQuery(_ context.Context, _ *QueryContext) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptInForwardQueryPlugin) RunOnForward() bool { return true }

// compile-time interface assertions
var _ QueryPlugin = (*fakeOptOutForwardQueryPlugin)(nil)
var _ QueryPlugin = (*fakeOptInForwardQueryPlugin)(nil)
var _ ForwardAware = (*fakeOptOutForwardQueryPlugin)(nil)
var _ ForwardAware = (*fakeOptInForwardQueryPlugin)(nil)

// fakeOptOutForwardHandshakePlugin opts OUT of forwarded sessions.
type fakeOptOutForwardHandshakePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutForwardHandshakePlugin) OnHandshakeComplete(_ context.Context, _ chsession.Session, _ time.Duration) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptOutForwardHandshakePlugin) RunOnForward() bool { return false }

// fakeOptInForwardHandshakePlugin explicitly opts IN to forwarded sessions.
type fakeOptInForwardHandshakePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptInForwardHandshakePlugin) OnHandshakeComplete(_ context.Context, _ chsession.Session, _ time.Duration) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptInForwardHandshakePlugin) RunOnForward() bool { return true }

// fakeOptOutForwardExceptionPlugin opts OUT of forwarded sessions.
type fakeOptOutForwardExceptionPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutForwardExceptionPlugin) OnException(_ context.Context, _ chsession.Session, _ *chproto.Exception) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptOutForwardExceptionPlugin) RunOnForward() bool { return false }

// fakeOptInForwardExceptionPlugin explicitly opts IN to forwarded sessions.
type fakeOptInForwardExceptionPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptInForwardExceptionPlugin) OnException(_ context.Context, _ chsession.Session, _ *chproto.Exception) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptInForwardExceptionPlugin) RunOnForward() bool { return true }

// fakeOptOutForwardQueryCompletePlugin opts OUT of forwarded sessions.
type fakeOptOutForwardQueryCompletePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutForwardQueryCompletePlugin) OnQueryComplete(_ context.Context, _ chsession.Session) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptOutForwardQueryCompletePlugin) RunOnForward() bool { return false }

// fakeOptInForwardQueryCompletePlugin explicitly opts IN to forwarded sessions.
type fakeOptInForwardQueryCompletePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptInForwardQueryCompletePlugin) OnQueryComplete(_ context.Context, _ chsession.Session) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptInForwardQueryCompletePlugin) RunOnForward() bool { return true }

func TestPluginChain_OnQuery_ForwardOptOutSkipsMarkedPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeOptOutForwardQueryPlugin{name: "rewrite", called: &called},
			&fakeQueryPlugin{name: "concurrency", called: &called},
			&fakeOptInForwardQueryPlugin{name: "metrics", called: &called},
		},
	}

	// Forwarding session: opt-out plugin skipped, default-run and opt-in
	// plugins fire normally.
	sess := newFakeSession()
	sess.State().IsForwarding = true
	qctx := &QueryContext{Session: sess, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(called) != 2 || called[0] != "concurrency" || called[1] != "metrics" {
		t.Fatalf("called=%v, want [concurrency metrics]", called)
	}

	// Regular (non-forwarding) session: every plugin runs.
	called = nil
	regular := newFakeSession()
	qctx = &QueryContext{Session: regular, Query: &chproto.Query{Body: "x"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery non-forward: %v", err)
	}
	if len(called) != 3 {
		t.Fatalf("called=%v, want 3 plugins on regular session", called)
	}
}

func TestPluginChain_OnHandshakeComplete_SkipsForwardOptOutWhenIsForwarding(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HandshakeCompletePlugins: []HandshakeCompletePlugin{
			&fakeOptOutForwardHandshakePlugin{name: "dbrewriter", called: &called},
			&fakeHandshakeCompletePlugin{name: "audit", called: &called},
			&fakeOptInForwardHandshakePlugin{name: "metrics", called: &called},
		},
	}

	// Forwarding session: opt-out plugin skipped, default-run and opt-in fire.
	sess := newFakeSession()
	sess.State().IsForwarding = true
	chain.OnHandshakeComplete(context.Background(), sess, time.Millisecond)
	if len(called) != 2 || called[0] != "audit" || called[1] != "metrics" {
		t.Fatalf("called=%v, want [audit metrics]", called)
	}

	// Regular (non-forwarding) session: every plugin runs.
	called = nil
	regular := newFakeSession()
	chain.OnHandshakeComplete(context.Background(), regular, time.Millisecond)
	if len(called) != 3 {
		t.Fatalf("called=%v, want 3 plugins on regular session", called)
	}
}

func TestPluginChain_OnException_SkipsForwardOptOutWhenIsForwarding(t *testing.T) {
	var called []string
	chain := &PluginChain{
		ExceptionPlugins: []ExceptionPlugin{
			&fakeOptOutForwardExceptionPlugin{name: "rewrite-reverse-map", called: &called},
			&fakeExceptionPlugin{name: "usage", called: &called},
			&fakeOptInForwardExceptionPlugin{name: "commitgate", called: &called},
		},
	}

	// Forwarding session: opt-out plugin skipped, default-run and opt-in fire.
	sess := newFakeSession()
	sess.State().IsForwarding = true
	if err := chain.OnException(context.Background(), sess, &chproto.Exception{}); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if len(called) != 2 || called[0] != "usage" || called[1] != "commitgate" {
		t.Fatalf("called=%v, want [usage commitgate]", called)
	}

	// Regular (non-forwarding) session: every plugin runs.
	called = nil
	regular := newFakeSession()
	if err := chain.OnException(context.Background(), regular, &chproto.Exception{}); err != nil {
		t.Fatalf("OnException non-forward: %v", err)
	}
	if len(called) != 3 {
		t.Fatalf("called=%v, want 3 plugins on regular session", called)
	}
}

func TestPluginChain_OnQueryComplete_SkipsForwardOptOutWhenIsForwarding(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryCompletePlugins: []QueryCompletePlugin{
			&fakeOptOutForwardQueryCompletePlugin{name: "commitgate", called: &called},
			&fakeQueryCompletePlugin{name: "concurrency", called: &called},
			&fakeOptInForwardQueryCompletePlugin{name: "metrics", called: &called},
		},
	}

	// Forwarding session: opt-out plugin skipped, default-run and opt-in fire.
	sess := newFakeSession()
	sess.State().IsForwarding = true
	chain.OnQueryComplete(context.Background(), sess)
	if len(called) != 2 || called[0] != "concurrency" || called[1] != "metrics" {
		t.Fatalf("called=%v, want [concurrency metrics]", called)
	}

	// Regular (non-forwarding) session: every plugin runs.
	called = nil
	regular := newFakeSession()
	chain.OnQueryComplete(context.Background(), regular)
	if len(called) != 3 {
		t.Fatalf("called=%v, want 3 plugins on regular session", called)
	}
}

// pivotingQueryPlugin mimics forward.Plugin's OnQuery: it flips
// IsForwarding=true on the session's state when its hook runs. Used to
// drive the regression test below — confirms the chain re-evaluates
// filter state mid-iteration so subsequent plugins see the new flag.
type pivotingQueryPlugin struct {
	name   string
	called *[]string
}

func (p *pivotingQueryPlugin) Name() string { return p.name }
func (p *pivotingQueryPlugin) OnQuery(_ context.Context, qctx *QueryContext) error {
	*p.called = append(*p.called, p.name)
	qctx.Session.State().SetForwarding(true)
	return nil
}
func (*pivotingQueryPlugin) RunOnForward() bool { return true }

var _ QueryPlugin = (*pivotingQueryPlugin)(nil)
var _ ForwardAware = (*pivotingQueryPlugin)(nil)

// TestPluginChain_OnQuery_PivotMidChainSkipsLaterOptOutPlugins is the
// regression for the stale-snapshot bug found 2026-04-29 by a user
// running a real agent→ProxyA→ProxyB pivot: when forward.Plugin's
// OnQuery fires SetForwarding(true) mid-chain, plugins that come after
// it in QueryPlugins and implement RunOnForward()=false (rewrite,
// commitgate, dbrewriter) MUST be skipped. The earlier code captured
// `forwarding := state.IsForwarding` once before the loop, so the
// captured value stayed false for the remainder of the loop and the
// rewriter ran on a session it had no business touching.
func TestPluginChain_OnQuery_PivotMidChainSkipsLaterOptOutPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryPlugins: []QueryPlugin{
			&fakeQueryPlugin{name: "auth", called: &called},
			&pivotingQueryPlugin{name: "forward", called: &called},
			&fakeOptOutForwardQueryPlugin{name: "rewrite", called: &called},
			&fakeOptInForwardQueryPlugin{name: "metrics", called: &called},
		},
	}

	// Session starts non-forwarding. After "forward" plugin runs, the
	// session is forwarding; "rewrite" must skip, "metrics" must run.
	sess := newFakeSession()
	if sess.State().IsForwarding {
		t.Fatalf("precondition: session must start non-forwarding")
	}
	qctx := &QueryContext{Session: sess, Query: &chproto.Query{Body: "USE remote_db"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	want := []string{"auth", "forward", "metrics"}
	if len(called) != len(want) {
		t.Fatalf("called=%v, want %v", called, want)
	}
	for i := range want {
		if called[i] != want[i] {
			t.Fatalf("called[%d]=%q, want %q (full=%v)", i, called[i], want[i], called)
		}
	}
	if !sess.State().IsForwarding {
		t.Fatalf("forward plugin must have set IsForwarding=true")
	}
}

// pivotingHandshakePlugin mirrors pivotingQueryPlugin for the
// OnHandshakeComplete hook.
type pivotingHandshakePlugin struct {
	name   string
	called *[]string
}

func (p *pivotingHandshakePlugin) Name() string { return p.name }
func (p *pivotingHandshakePlugin) OnHandshakeComplete(_ context.Context, sess chsession.Session, _ time.Duration) {
	*p.called = append(*p.called, p.name)
	sess.State().SetForwarding(true)
}
func (*pivotingHandshakePlugin) RunOnForward() bool { return true }

var _ HandshakeCompletePlugin = (*pivotingHandshakePlugin)(nil)
var _ ForwardAware = (*pivotingHandshakePlugin)(nil)

func TestPluginChain_OnHandshakeComplete_PivotMidChainSkipsLaterOptOutPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HandshakeCompletePlugins: []HandshakeCompletePlugin{
			&pivotingHandshakePlugin{name: "forward", called: &called},
			&fakeOptOutForwardHandshakePlugin{name: "dbrewriter", called: &called},
			&fakeOptInForwardHandshakePlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	chain.OnHandshakeComplete(context.Background(), sess, time.Millisecond)
	want := []string{"forward", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v, want %v", called, want)
	}
}

// pivotingExceptionPlugin mirrors pivotingQueryPlugin for the
// OnException hook.
type pivotingExceptionPlugin struct {
	name   string
	called *[]string
}

func (p *pivotingExceptionPlugin) Name() string { return p.name }
func (p *pivotingExceptionPlugin) OnException(_ context.Context, sess chsession.Session, _ *chproto.Exception) error {
	*p.called = append(*p.called, p.name)
	sess.State().SetForwarding(true)
	return nil
}
func (*pivotingExceptionPlugin) RunOnForward() bool { return true }

var _ ExceptionPlugin = (*pivotingExceptionPlugin)(nil)
var _ ForwardAware = (*pivotingExceptionPlugin)(nil)

func TestPluginChain_OnException_PivotMidChainSkipsLaterOptOutPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		ExceptionPlugins: []ExceptionPlugin{
			&pivotingExceptionPlugin{name: "forward", called: &called},
			&fakeOptOutForwardExceptionPlugin{name: "rewrite", called: &called},
			&fakeOptInForwardExceptionPlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	if err := chain.OnException(context.Background(), sess, &chproto.Exception{}); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	want := []string{"forward", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v, want %v", called, want)
	}
}

// pivotingQueryCompletePlugin mirrors pivotingQueryPlugin for the
// OnQueryComplete hook.
type pivotingQueryCompletePlugin struct {
	name   string
	called *[]string
}

func (p *pivotingQueryCompletePlugin) Name() string { return p.name }
func (p *pivotingQueryCompletePlugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	*p.called = append(*p.called, p.name)
	sess.State().SetForwarding(true)
}
func (*pivotingQueryCompletePlugin) RunOnForward() bool { return true }

var _ QueryCompletePlugin = (*pivotingQueryCompletePlugin)(nil)
var _ ForwardAware = (*pivotingQueryCompletePlugin)(nil)

func TestPluginChain_OnQueryComplete_PivotMidChainSkipsLaterOptOutPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryCompletePlugins: []QueryCompletePlugin{
			&pivotingQueryCompletePlugin{name: "forward", called: &called},
			&fakeOptOutForwardQueryCompletePlugin{name: "commitgate", called: &called},
			&fakeOptInForwardQueryCompletePlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	chain.OnQueryComplete(context.Background(), sess)
	want := []string{"forward", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v, want %v", called, want)
	}
}

// pivotingHelloPlugin mirrors pivotingQueryPlugin for the OnHello hook.
type pivotingHelloPlugin struct {
	name   string
	called *[]string
}

func (p *pivotingHelloPlugin) Name() string { return p.name }
func (p *pivotingHelloPlugin) OnHello(_ context.Context, sess chsession.Session, _ *chproto.ClientHello) error {
	*p.called = append(*p.called, p.name)
	sess.State().SetForwarding(true)
	return nil
}
func (*pivotingHelloPlugin) RunOnForward() bool { return true }

var _ HelloPlugin = (*pivotingHelloPlugin)(nil)
var _ ForwardAware = (*pivotingHelloPlugin)(nil)

// fakeOptOutForwardHelloPlugin opts OUT of forwarded sessions in OnHello.
type fakeOptOutForwardHelloPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutForwardHelloPlugin) Name() string { return f.name }
func (f *fakeOptOutForwardHelloPlugin) OnHello(_ context.Context, _ chsession.Session, _ *chproto.ClientHello) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptOutForwardHelloPlugin) RunOnForward() bool { return false }

var _ HelloPlugin = (*fakeOptOutForwardHelloPlugin)(nil)
var _ ForwardAware = (*fakeOptOutForwardHelloPlugin)(nil)

func TestPluginChain_OnHello_PivotMidChainSkipsLaterOptOutPlugins(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HelloPlugins: []HelloPlugin{
			&pivotingHelloPlugin{name: "forward", called: &called},
			&fakeOptOutForwardHelloPlugin{name: "rewrite", called: &called},
		},
	}
	sess := newFakeSession()
	if err := chain.OnHello(context.Background(), sess, &chproto.ClientHello{}); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	want := []string{"forward"}
	if len(called) != len(want) || called[0] != want[0] {
		t.Fatalf("called=%v, want %v", called, want)
	}
}

// fakeOptOutPeerHandshakePlugin opts OUT of peer-trusted sessions.
type fakeOptOutPeerHandshakePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutPeerHandshakePlugin) Name() string { return f.name }
func (f *fakeOptOutPeerHandshakePlugin) OnHandshakeComplete(_ context.Context, _ chsession.Session, _ time.Duration) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptOutPeerHandshakePlugin) RunOnPeerTrust() bool { return false }

var _ HandshakeCompletePlugin = (*fakeOptOutPeerHandshakePlugin)(nil)
var _ PeerTrustAware = (*fakeOptOutPeerHandshakePlugin)(nil)

// fakeOptOutPeerExceptionPlugin opts OUT of peer-trusted sessions.
type fakeOptOutPeerExceptionPlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutPeerExceptionPlugin) Name() string { return f.name }
func (f *fakeOptOutPeerExceptionPlugin) OnException(_ context.Context, _ chsession.Session, _ *chproto.Exception) error {
	*f.called = append(*f.called, f.name)
	return nil
}
func (*fakeOptOutPeerExceptionPlugin) RunOnPeerTrust() bool { return false }

var _ ExceptionPlugin = (*fakeOptOutPeerExceptionPlugin)(nil)
var _ PeerTrustAware = (*fakeOptOutPeerExceptionPlugin)(nil)

// fakeOptOutPeerQueryCompletePlugin opts OUT of peer-trusted sessions.
type fakeOptOutPeerQueryCompletePlugin struct {
	name   string
	called *[]string
}

func (f *fakeOptOutPeerQueryCompletePlugin) Name() string { return f.name }
func (f *fakeOptOutPeerQueryCompletePlugin) OnQueryComplete(_ context.Context, _ chsession.Session) {
	*f.called = append(*f.called, f.name)
}
func (*fakeOptOutPeerQueryCompletePlugin) RunOnPeerTrust() bool { return false }

var _ QueryCompletePlugin = (*fakeOptOutPeerQueryCompletePlugin)(nil)
var _ PeerTrustAware = (*fakeOptOutPeerQueryCompletePlugin)(nil)

// TestPluginChain_OnQuery_ForwardedFromPeerOverridesPeerTrustOptOut is the
// regression for the rewriter-skipped-on-forwarded-peer bug found 2026-04-29:
// when a session arrives via the __peer__|<addr>|forwarded envelope (i.e.
// forward-decision pivot, not rewriter remote() loopback), the SQL is the
// client's RAW form and rewrite/auth must run on this proxy. Plain
// IsPeerTrusted=true sessions still skip those plugins (legacy remote()
// contract — SQL is already rewritten upstream).
func TestPluginChain_OnQuery_ForwardedFromPeerOverridesPeerTrustOptOut(t *testing.T) {
	makeChain := func(called *[]string) *PluginChain {
		return &PluginChain{
			QueryPlugins: []QueryPlugin{
				&fakeOptOutPeerQueryPlugin{name: "rewrite", called: called},
				&fakeOptInForwardQueryPlugin{name: "metrics", called: called},
			},
		}
	}

	// Case 1: legacy peer-trust (remote() loopback). Rewrite skipped.
	var called1 []string
	chain1 := makeChain(&called1)
	sess1 := newFakeSession()
	sess1.State().SetPeerTrust("0xpeer")
	qctx1 := &QueryContext{Session: sess1, Query: &chproto.Query{Body: "x"}}
	if err := chain1.OnQuery(context.Background(), qctx1); err != nil {
		t.Fatalf("OnQuery legacy peer: %v", err)
	}
	if len(called1) != 1 || called1[0] != "metrics" {
		t.Fatalf("legacy peer-trust: called=%v want [metrics] (rewrite must skip)", called1)
	}

	// Case 2: forwarded-from-peer. Rewrite runs.
	var called2 []string
	chain2 := makeChain(&called2)
	sess2 := newFakeSession()
	sess2.State().SetPeerTrustForwarded("0xpeer", true)
	qctx2 := &QueryContext{Session: sess2, Query: &chproto.Query{Body: "x"}}
	if err := chain2.OnQuery(context.Background(), qctx2); err != nil {
		t.Fatalf("OnQuery forwarded peer: %v", err)
	}
	if len(called2) != 2 || called2[0] != "rewrite" || called2[1] != "metrics" {
		t.Fatalf("forwarded peer-trust: called=%v want [rewrite metrics] (rewrite must run)", called2)
	}
}

func TestPluginChain_OnHandshakeComplete_ForwardedFromPeerOverridesPeerTrustOptOut(t *testing.T) {
	var called []string
	chain := &PluginChain{
		HandshakeCompletePlugins: []HandshakeCompletePlugin{
			&fakeOptOutPeerHandshakePlugin{name: "rewrite", called: &called},
			&fakeOptInForwardHandshakePlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	sess.State().SetPeerTrustForwarded("0xpeer", true)
	chain.OnHandshakeComplete(context.Background(), sess, time.Millisecond)
	want := []string{"rewrite", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v want %v", called, want)
	}
}

func TestPluginChain_OnException_ForwardedFromPeerOverridesPeerTrustOptOut(t *testing.T) {
	var called []string
	chain := &PluginChain{
		ExceptionPlugins: []ExceptionPlugin{
			&fakeOptOutPeerExceptionPlugin{name: "rewrite", called: &called},
			&fakeOptInForwardExceptionPlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	sess.State().SetPeerTrustForwarded("0xpeer", true)
	if err := chain.OnException(context.Background(), sess, &chproto.Exception{}); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	want := []string{"rewrite", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v want %v", called, want)
	}
}

func TestPluginChain_OnQueryComplete_ForwardedFromPeerOverridesPeerTrustOptOut(t *testing.T) {
	var called []string
	chain := &PluginChain{
		QueryCompletePlugins: []QueryCompletePlugin{
			&fakeOptOutPeerQueryCompletePlugin{name: "rewrite", called: &called},
			&fakeOptInForwardQueryCompletePlugin{name: "metrics", called: &called},
		},
	}
	sess := newFakeSession()
	sess.State().SetPeerTrustForwarded("0xpeer", true)
	chain.OnQueryComplete(context.Background(), sess)
	want := []string{"rewrite", "metrics"}
	if len(called) != len(want) || called[0] != want[0] || called[1] != want[1] {
		t.Fatalf("called=%v want %v", called, want)
	}
}
