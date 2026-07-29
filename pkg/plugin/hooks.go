package plugin

import (
	"context"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
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
//   - OnClientDataStrict — for correctness-critical Data capture before
//     upstream splice; errors are fail-closed.
//   - OnClientData — for every raw client Data packet (INSERT body),
//     dispatched only when DataPlugins are registered; fail-open.
//   - OnException — when the upstream returns an Exception packet.
//   - OnQueryInputComplete — after the terminating empty client Data block is
//     forwarded upstream. SuppressUpstreamExecution queries still forward the
//     terminator after staged input succeeds, but non-empty payload Data is
//     withheld from ordinary upstream.
//   - OnQueryAbort — when Relay rejects a query lifecycle, including after a
//     Query or earlier Data packets have already reached upstream.
//   - OnQuerySuccess — after an isolated upstream EndOfStream packet.
//   - OnQueryComplete — once per Query when its lifecycle ends
//     (rejected, forward failed, or upstream produced EndOfStream /
//     Exception, including cleanup-only opaque terminal detection).
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
	RejectUndecodableQuery(sess chsession.Session) bool
	ClientDataReadLimit(qctx *QueryContext) (maxBytes uint64, enforce bool)
	OnClientDataStrict(ctx context.Context, qctx *QueryContext, raw []byte) error
	OnClientData(ctx context.Context, qctx *QueryContext, raw []byte) error
	OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error
	OnQueryInputCompleteStrict(ctx context.Context, qctx *QueryContext) error
	OnQueryInputComplete(ctx context.Context, qctx *QueryContext)
	OnQueryAbort(ctx context.Context, qctx *QueryContext)
	OnQuerySuccess(ctx context.Context, sess chsession.Session, queryID string)
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

func (NoopHooks) RejectUndecodableQuery(chsession.Session) bool { return false }

func (NoopHooks) ClientDataReadLimit(*QueryContext) (uint64, bool) { return 0, false }

func (NoopHooks) OnClientDataStrict(context.Context, *QueryContext, []byte) error {
	return nil
}

func (NoopHooks) OnClientData(context.Context, *QueryContext, []byte) error { return nil }

func (NoopHooks) OnException(context.Context, chsession.Session, *chproto.Exception) error {
	return nil
}

func (NoopHooks) OnQueryInputCompleteStrict(context.Context, *QueryContext) error { return nil }

func (NoopHooks) OnQueryInputComplete(context.Context, *QueryContext) {}

func (NoopHooks) OnQueryAbort(context.Context, *QueryContext) {}

func (NoopHooks) OnQuerySuccess(context.Context, chsession.Session, string) {}

func (NoopHooks) OnQueryComplete(context.Context, chsession.Session) {}

func (NoopHooks) OnClose(chsession.Session) {}

func (NoopHooks) OnDisconnect(chsession.Session) {}

var _ Hooks = NoopHooks{}
