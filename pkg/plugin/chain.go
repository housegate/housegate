package plugin

import (
	"context"
	"time"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"sentioxyz/sentio-core/common/log"
)

// PluginChain composes lists of typed plugins into a single Hooks value.
//
// Per-stage execution rules:
//
//   - OnConnect / OnHello / OnQuery short-circuit on the first non-nil
//     error.
//   - OnException runs every plugin and returns the first non-nil error
//     (best-effort enrichment).
//   - OnQueryComplete, OnClose, and OnDisconnect run every plugin and
//     ignore errors.
//
// Routed sessions: once routeplugin.Stripper sets SessionState.RouteTarget
// during OnHello, the chain skips any subsequent OnHello plugins and any
// query-stage plugin (OnQuery / OnException / OnQueryComplete /
// OnHandshakeComplete) that does NOT satisfy RouteAware. The lifecycle
// hooks (OnConnect, OnDisconnect, OnClose) intentionally ignore route
// status — see the RouteAware doc for why.
//
// Peer-trusted sessions: once the credential plugin sets
// SessionState.IsPeerTrusted during OnHello (after validating an
// inbound peer-relay JWS), the chain skips any query-stage plugin
// that opts out via PeerTrustAware.RunOnPeerTrust() == false. Default
// is run; only auth / rewrite explicitly opt out. OnHello itself is
// NOT filtered because the credential plugin lives there and is what
// flips the flag in the first place.
//
// Forwarding sessions: once the forward-decision plugin sets
// SessionState.IsForwarding during OnHello, the chain skips any
// query-stage plugin that opts out via ForwardAware.RunOnForward() ==
// false. Default is run; plugins like rewrite / commitgate
// opt out because their work belongs on the receiving (host) proxy.
type PluginChain struct {
	ConnLifecyclePlugins     []ConnLifecyclePlugin
	HelloPlugins             []HelloPlugin
	HandshakeCompletePlugins []HandshakeCompletePlugin
	QueryPlugins             []QueryPlugin
	ExceptionPlugins         []ExceptionPlugin
	QueryCompletePlugins     []QueryCompletePlugin
	ClosePlugins             []ClosePlugin
}

// runsOnRouted reports whether p should fire on a routed session.
// Plugins that do not implement RouteAware are filtered out.
func runsOnRouted(p any) bool {
	ra, ok := p.(RouteAware)
	return ok && ra.RunOnRouted()
}

// runsOnPeerTrust reports whether p should fire on a peer-trusted
// session. The default for plugins that do not implement
// PeerTrustAware is to RUN — peer-trusted sessions are otherwise
// indistinguishable from regular client sessions; only auth/rewrite
// (and any future plugin that consumes the original-SQL semantics
// the upstream already executed against) explicitly opts out.
func runsOnPeerTrust(p any) bool {
	pa, ok := p.(PeerTrustAware)
	if !ok {
		return true
	}
	return pa.RunOnPeerTrust()
}

// runsOnForward returns whether p should run when SessionState.IsForwarding
// is true. Default (no marker) = true. Implementing ForwardAware lets a
// plugin opt out by returning false.
func runsOnForward(p any) bool {
	fa, ok := p.(ForwardAware)
	if !ok {
		return true
	}
	return fa.RunOnForward()
}

func (c *PluginChain) OnConnect(ctx context.Context, sess chsession.Session) error {
	for _, p := range c.ConnLifecyclePlugins {
		if err := p.OnConnect(ctx, sess); err != nil {
			return err
		}
	}
	return nil
}

func (c *PluginChain) OnDisconnect(sess chsession.Session) {
	for _, p := range c.ConnLifecyclePlugins {
		p.OnDisconnect(sess)
	}
}

func (c *PluginChain) OnHello(ctx context.Context, sess chsession.Session, h *chproto.ClientHello) error {
	ctx, logger := log.FromContext(ctx)
	state := sess.State()
	for _, p := range c.HelloPlugins {
		// State is read on every iteration because earlier hello plugins
		// can mutate it. Specifically: routeplugin.Stripper sets
		// IsRouted, and forward.Plugin sets IsForwarding when it pivots
		// to a peer. Plugins downstream of those mutators must see the
		// new value, otherwise they run against a session that has
		// already been routed/pivoted — corrupting the handshake.
		if state.IsRouted() && !runsOnRouted(p) {
			continue
		}
		if state.IsForwarding && !runsOnForward(p) {
			continue
		}
		if err := p.OnHello(ctx, sess, h); err != nil {
			return err
		}
	}
	logger.Debugw("hello plugins complete", "routed", state.IsRouted(), "forwarding", state.IsForwarding)
	return nil
}

func (c *PluginChain) OnHandshakeComplete(ctx context.Context, sess chsession.Session, d time.Duration) {
	// State is re-read on every iteration so plugins that flip filter
	// flags (forward.Plugin -> IsForwarding, credential.Plugin ->
	// IsPeerTrusted) take effect immediately on subsequent plugins.
	// See chain_test.go::TestPluginChain_*_PivotMidChainSkipsLaterOptOutPlugins.
	state := sess.State()
	for _, p := range c.HandshakeCompletePlugins {
		if state.IsRouted() && !runsOnRouted(p) {
			continue
		}
		// IsForwardedFromPeer overrides the peer-trust filter: a
		// forward-pivot session arrives with the client's raw SQL,
		// so rewrite/auth must run on this proxy. The legacy
		// remote() loopback path leaves IsForwardedFromPeer=false
		// and the original opt-out behaviour applies.
		if state.PeerTrusted() && !state.IsForwardedFromPeer && !runsOnPeerTrust(p) {
			continue
		}
		if state.IsForwarding && !runsOnForward(p) {
			continue
		}
		p.OnHandshakeComplete(ctx, sess, d)
	}
}

func (c *PluginChain) OnQuery(ctx context.Context, qctx *QueryContext) error {
	// State is re-read on every iteration. forward.Plugin's OnQuery
	// detects USE statements and may flip IsForwarding mid-loop;
	// downstream plugins (rewrite, commitgate, dbrewriter) implement
	// RunOnForward()=false and MUST be skipped on the same query that
	// triggered the pivot, otherwise the rewriter sees a USE targeting
	// a remote upstream and tries to rewrite SQL the entry proxy is
	// no longer responsible for.
	state := qctx.Session.State()
	for _, p := range c.QueryPlugins {
		if state.IsRouted() && !runsOnRouted(p) {
			continue
		}
		// See OnHandshakeComplete for the IsForwardedFromPeer override.
		if state.PeerTrusted() && !state.IsForwardedFromPeer && !runsOnPeerTrust(p) {
			continue
		}
		if state.IsForwarding && !runsOnForward(p) {
			continue
		}
		if err := p.OnQuery(ctx, qctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *PluginChain) OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error {
	// State is re-read on every iteration; see OnQuery for the reason.
	state := sess.State()
	var first error
	for _, p := range c.ExceptionPlugins {
		if state.IsRouted() && !runsOnRouted(p) {
			continue
		}
		// See OnHandshakeComplete for the IsForwardedFromPeer override.
		if state.PeerTrusted() && !state.IsForwardedFromPeer && !runsOnPeerTrust(p) {
			continue
		}
		if state.IsForwarding && !runsOnForward(p) {
			continue
		}
		if err := p.OnException(ctx, sess, exc); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *PluginChain) OnQueryComplete(ctx context.Context, sess chsession.Session) {
	// State is re-read on every iteration; see OnQuery for the reason.
	state := sess.State()
	for _, p := range c.QueryCompletePlugins {
		if state.IsRouted() && !runsOnRouted(p) {
			continue
		}
		// See OnHandshakeComplete for the IsForwardedFromPeer override.
		if state.PeerTrusted() && !state.IsForwardedFromPeer && !runsOnPeerTrust(p) {
			continue
		}
		if state.IsForwarding && !runsOnForward(p) {
			continue
		}
		p.OnQueryComplete(ctx, sess)
	}
}

func (c *PluginChain) OnClose(sess chsession.Session) {
	for _, p := range c.ClosePlugins {
		p.OnClose(sess)
	}
}

var _ Hooks = (*PluginChain)(nil)
