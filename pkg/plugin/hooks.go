package plugin

import (
	"context"
	"time"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
)

// Hooks is the surface Server/Relay calls out to at the significant
// moments of a session's life:
//
//   - OnConnect / OnDisconnect — TCP-connection-level (Server.handle).
//     Fires once per accepted connection regardless of handshake
//     outcome, so connection-lifecycle metrics stay balanced.
//   - OnHello — after ClientHello is decoded, before it is forwarded.
//   - OnHandshakeComplete — after ServerHello + addendum exchange.
//   - OnQuery — for every decoded client Query packet.
//   - OnClientData — for every raw client Data packet (INSERT body),
//     dispatched only when DataPlugins are registered; fail-open.
//   - OnException — when the upstream returns an Exception packet.
//   - OnQueryComplete — once per Query when its lifecycle ends
//     (rejected, forward failed, or upstream produced EndOfStream /
//     Exception).
//   - OnClose — once per session that successfully bound an upstream
//     and ran Relay (paired with OnHello, not OnConnect).
//
// PluginChain is the canonical implementation; NoopHooks exists for
// tests that exercise pure protocol plumbing.
type Hooks interface {
	OnConnect(ctx context.Context, sess chsession.Session) error
	OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error
	OnHandshakeComplete(ctx context.Context, sess chsession.Session, duration time.Duration)
	OnQuery(ctx context.Context, qctx *QueryContext) error
	OnClientData(ctx context.Context, qctx *QueryContext, raw []byte) error
	OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error
	OnQueryComplete(ctx context.Context, sess chsession.Session)
	OnClose(sess chsession.Session)
	OnDisconnect(sess chsession.Session)
}

// NoopHooks satisfies Hooks without doing anything.
type NoopHooks struct{}

func (NoopHooks) OnConnect(context.Context, chsession.Session) error { return nil }

func (NoopHooks) OnHello(context.Context, chsession.Session, *chproto.ClientHello) error {
	return nil
}

func (NoopHooks) OnHandshakeComplete(context.Context, chsession.Session, time.Duration) {}

func (NoopHooks) OnQuery(context.Context, *QueryContext) error { return nil }

func (NoopHooks) OnClientData(context.Context, *QueryContext, []byte) error { return nil }

func (NoopHooks) OnException(context.Context, chsession.Session, *chproto.Exception) error {
	return nil
}

func (NoopHooks) OnQueryComplete(context.Context, chsession.Session) {}

func (NoopHooks) OnClose(chsession.Session) {}

func (NoopHooks) OnDisconnect(chsession.Session) {}

var _ Hooks = NoopHooks{}
