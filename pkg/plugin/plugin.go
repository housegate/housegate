package plugin

import (
	"context"
	"time"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
)

// HelloPlugin participates in the OnHello chain.
//
// Implementations may mutate the ClientHello before it is forwarded to the
// upstream — typical uses include credential substitution and stripping a
// route-encoding prefix from the user field.
type HelloPlugin interface {
	OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error
}

// QueryPlugin participates in the OnQuery chain.
//
// Plugins are invoked in slice order; the first non-nil error short-circuits
// the chain. Relay converts a returned error into a synthetic ClickHouse
// Exception toward the client.
type QueryPlugin interface {
	OnQuery(ctx context.Context, qctx *QueryContext) error
}

// QueryCompletePlugin participates in the OnQueryComplete chain.
//
// OnQueryComplete fires exactly once per client Query packet, when its
// lifecycle ends from Relay's perspective:
//
//   - The OnQuery chain rejected the query (it never reached the upstream).
//   - Forwarding to upstream failed.
//   - The upstream answered with EndOfStream or Exception.
//
// It is the natural pairing point for plugins that take per-query resources
// in OnQuery (concurrency permits, in-flight counters, tracing spans) and
// must release them in a deterministic place.
//
// All plugins run; errors are not propagated. Plugins that need a final
// safety-net (e.g. a client disconnect mid-query that prevents this hook
// from firing) should also implement ClosePlugin.
type QueryCompletePlugin interface {
	OnQueryComplete(ctx context.Context, sess chsession.Session)
}

// ExceptionPlugin participates in the OnException chain.
//
// Unlike OnQuery, every ExceptionPlugin runs even if an earlier plugin
// returns an error; the first error is reported back to the chain caller.
// This matches the typical "best-effort enrichment" pattern (e.g. error
// reverse-mapping followed by audit logging).
type ExceptionPlugin interface {
	OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error
}

// ClosePlugin participates in OnClose. OnClose runs every plugin
// unconditionally and returns no error.
type ClosePlugin interface {
	OnClose(sess chsession.Session)
}

// DataPlugin observes raw client Data packets — the streamed body of an
// INSERT — between OnQuery and OnQueryComplete. raw is the full on-wire
// packet (type varint + block name + block body) and MUST NOT be retained
// or mutated: the relay splices the very same bytes to the upstream after
// the hook returns. qctx is the QueryContext of the most recent Query on
// the session, or nil when data arrives without one (the chain skips
// dispatch in that case). Hook errors are logged by the relay and never
// abort the connection (fail-open) — implementations should also degrade
// internally rather than error per packet.
type DataPlugin interface {
	OnClientData(ctx context.Context, qctx *QueryContext, raw []byte) error
}

// DataRewritePlugin can replace raw client Data packets before the relay
// splices them upstream. It is used only for protocol-level payload
// materialization that must happen on the trusted client-side HouseGate before
// the server-side HouseGate observes the payload. Implementations must return
// a full on-wire packet (type varint + block name + block body).
type DataRewritePlugin interface {
	RewriteClientData(ctx context.Context, qctx *QueryContext, raw []byte) ([]byte, error)
}

type DataRewriteError struct {
	Err error
}

func (e DataRewriteError) Error() string { return e.Err.Error() }
func (e DataRewriteError) Unwrap() error { return e.Err }

// HandshakeCompletePlugin participates in OnHandshakeComplete, fired
// by Relay once the ClientHello / ServerHello / Addendum round-trip
// has finished successfully and the session is ready to serve Query
// packets. The duration covers the full round-trip, measured from the
// start of Relay.handshake (the first blocking read on the client
// codec) to the return of the final addendum exchange.
//
// All plugins run; errors are not propagated. This is strictly an
// observation point — plugins must not block the packet loop in their
// handler.
type HandshakeCompletePlugin interface {
	OnHandshakeComplete(ctx context.Context, sess chsession.Session, duration time.Duration)
}

// RouteAware is an opt-in marker for plugins that should keep firing on
// routed (proxy-to-proxy) sessions. Once routeplugin.Stripper sets the
// session's RouteTarget during OnHello, PluginChain skips every plugin
// in the query-stage hooks that does NOT implement RouteAware (or whose
// RunOnRouted() returns false). The two production plugins that opt in
// today are routeplugin.Signer (it's the whole reason the session went
// through routing) and metricsplugin.Plugin (operators want metrics on
// routed traffic too).
//
// Lifecycle hooks (OnConnect, OnDisconnect, OnClose) are intentionally
// NOT gated by RouteAware: OnConnect fires before the routed status is
// known, so its OnDisconnect / OnClose pair must run unconditionally to
// keep tear-up/tear-down symmetric and avoid leaks.
type RouteAware interface {
	RunOnRouted() bool
}

// PeerTrustAware is an opt-OUT marker for plugins that should NOT fire
// once the credential plugin has marked the session
// SessionState.IsPeerTrusted (an upstream housegate authenticated us
// via a peer-relay JWS in the ClientHello). It is the inverse of
// RouteAware in spirit: peer-trusted sessions are still "real" client
// sessions in most respects (they consume concurrency, get logged,
// track session state) so the default for plugins that do NOT
// implement this interface is to keep firing. Plugins implement this
// marker only to declare a deliberate skip.
//
// The two production plugins that opt out today are auth (the SQL
// here is the upstream rewriter's already-rewritten form, so qhash
// validation will always mismatch — auth was already done at OnHello
// when the peer-relay JWS validated) and rewrite (re-rewriting
// already-rewritten SQL would either no-op or, worse, double-prefix
// already-prefixed table names).
//
// Same lifecycle-hooks carve-out as RouteAware: OnConnect /
// OnDisconnect / OnClose fire unconditionally to keep tear-up /
// tear-down symmetric. OnHello is also exempted from this filter —
// the credential plugin lives there and its peer-trust validation
// depends on running in OnHello in the first place.
type PeerTrustAware interface {
	RunOnPeerTrust() bool
}

// ConnLifecyclePlugin participates in OnConnect / OnDisconnect — the
// TCP-connection-level hooks fired by Server around every accepted
// connection, regardless of whether the handshake or upstream dial
// succeeds.
//
// Unlike ClosePlugin.OnClose (which fires only after a session's
// upstream is bound and Relay runs), these fire for every TCP accept:
// OnConnect immediately after the Session is constructed, OnDisconnect
// in a defer as handle() returns. This makes them the right place for
// connection-lifecycle metrics (e.g. active-connection gauges) that
// must stay balanced even when dial or handshake fails.
//
// OnConnect may return an error to reject the connection without ever
// reaching the upstream dialer; OnDisconnect errors are not propagated.
type ConnLifecyclePlugin interface {
	OnConnect(ctx context.Context, sess chsession.Session) error
	OnDisconnect(sess chsession.Session)
}
