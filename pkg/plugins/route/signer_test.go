package routeplugin

import (
	"context"
	"testing"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
)

// stubSigner records every SignToken call for assertion. Returns the
// fixed token "stub-jws" so tests can verify whether Signer injected
// it into query.Settings.
type stubSigner struct {
	signCalls int
}

func (s *stubSigner) SignToken(_ string) (string, error) {
	s.signCalls++
	return "stub-jws", nil
}
func (s *stubSigner) Address() string { return "0xstub" }

var _ auth.Signer = (*stubSigner)(nil)

// TestSigner_OnQuery_DoesNotFireOnForwardPivot is the regression for
// the wrong-identity-on-peer bug 2026-04-29: forward.Plugin.pivotToPeer
// calls SetRouteTarget to memoize the current peer, but Signer's
// internal guard `if !state.IsRouted() return nil` would fire because
// IsRouted() naively returned RouteTarget != "". The downstream
// effect: Signer appended ProxyA's relay JWS to query.Settings, and
// ProxyB's auth plugin's settings-map last-wins parsing recovered the
// relay address as the user identity instead of the sidecar's.
//
// IsRouted() now excludes forward-pivoted sessions; this test asserts
// the chain semantics: a session with both RouteTarget set AND
// IsForwarding=true must NOT be signed.
func TestSigner_OnQuery_DoesNotFireOnForwardPivot(t *testing.T) {
	signer := &stubSigner{}
	p := &Signer{Signer: signer}

	sess := newTestSession(t)
	sess.State().SetRouteTarget("peer.internal:9001")
	sess.State().SetForwarding(true)

	q := &chproto.Query{Body: "SELECT 1"}
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: q.Body, Query: q}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if signer.signCalls != 0 {
		t.Errorf("Signer.OnQuery on forward-pivoted session must not call SignToken; got %d calls", signer.signCalls)
	}
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.AuthTokenSettingKey {
			t.Errorf("Signer must NOT inject %q on forward-pivoted session; got %q", s.Key, s.Value)
		}
	}
}

// TestSigner_OnQuery_FiresOnRoutedSession is a positive control: a
// genuine __route__ session (RouteTarget set, IsForwarding=false) must
// still be signed.
func TestSigner_OnQuery_FiresOnRoutedSession(t *testing.T) {
	signer := &stubSigner{}
	p := &Signer{Signer: signer}

	sess := newTestSession(t)
	sess.State().SetRouteTarget("peer.internal:9001")
	// IsForwarding remains false — this is a real __route__ session.

	q := &chproto.Query{Body: "SELECT 1"}
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: q.Body, Query: q}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if signer.signCalls != 1 {
		t.Errorf("Signer must sign genuine routed session once; got %d", signer.signCalls)
	}
	var found bool
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.AuthTokenSettingKey {
			found = true
		}
	}
	if !found {
		t.Errorf("Signer must inject %q on routed session", auth.AuthTokenSettingKey)
	}
}
