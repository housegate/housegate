package chsession

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
)

// Session carries per-connection state and the two codecs (client + upstream).
// It is passive — it does not run the packet loop. Relay does.
//
// MVP: BindUpstream is called once at session start; RebindUpstream is
// never called in production code paths but is implemented and tested so
// future C3 (per-query pool borrow with state replay) can adopt it without
// interface churn.
type Session interface {
	ID() int64
	State() *SessionState
	Client() *chproto.Codec
	Upstream() *chproto.Codec
	RemoteAddr() net.Addr
	Close() error

	BindUpstream(ctx context.Context, up *chproto.Codec) error
	RebindUpstream(ctx context.Context, newUp *chproto.Codec, replayState bool) error

	// RebindToPeer performs the peer handshake on newUp (writing ClientHello,
	// reading ServerHello, negotiating notchunked addendum), then atomically
	// swaps the bound upstream to newUp. It intentionally does not replay
	// Database/Settings: forward-pivot traffic lands on another proxy's
	// auth path, and proxy-generated replay queries do not carry the
	// client's per-query JWS. The peer hello selects the target database;
	// the triggering client query still flows through the normal path.
	//
	// Used by the forward plugin to pivot a session onto a peer's
	// internal-port. peerHello must carry the peer-relay envelope
	// (User=__peer__|<self-addr>, Password=<JWS>) so the receiving proxy's
	// credential plugin marks the session IsPeerTrusted=true.
	//
	// On error, newUp is not closed; the caller retains ownership and must
	// close it. RebindToPeer only takes ownership after all handshake steps
	// succeed (signaled by a nil return).
	RebindToPeer(ctx context.Context, newUp *chproto.Codec, peerHello *chproto.ClientHello) error

	// RebindToLocal is the remote→local counterpart of RebindToPeer: used
	// by forward.Plugin when a USE statement on a previously forwarded
	// session targets a database hosted on THIS proxy. Mirrors RebindToPeer
	// but with three differences:
	//
	//   - hello must carry regular CH credentials (no peer envelope) — the
	//     receiving end is local CH or a normal upstream
	//   - PeerServerHelloRaw / PeerRevision are NOT populated (peer state
	//     is irrelevant on the local-bound leg)
	//   - IsForwarding and RouteTarget are CLEARED on success so the chain
	//     stops treating the session as forwarded — auth/usage/concurrency
	//     resume on the local side, and IsRouted() returns false again
	//   - Database replay is skipped because hello.Database already selected
	//     the physical upstream DB, and the triggering client USE query still
	//     flows through the normal query path after the rebind
	//
	// Same ownership rule as RebindToPeer: newUp is taken on nil-return,
	// retained by the caller on error.
	RebindToLocal(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error

	// SwitchUpstream moves the session to another server for the agent's
	// storage-integrity upstream switch (spec 2026-10-09 §6.4, D19). hello is
	// replayed as given — the caller sets Database — except that a copy of it
	// is sent with ProtocolVersion clamped to SessionState.ClientRevision; the
	// new leg must then negotiate exactly that revision
	// (ErrUpstreamRevisionTooLow) and, when the session recorded a server
	// timezone, report the same one (ErrUpstreamTimezoneMismatch). ctx bounds
	// the handshake: its deadline is applied to the new conn and its
	// cancellation fails the switch. It writes no peer or forward state and
	// replays nothing. It owns newUp in every case: on error newUp is closed,
	// the old upstream stays bound and the session state is untouched.
	// Call it only from OnQuery, where no query is active.
	SwitchUpstream(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error
}

type sessionImpl struct {
	id         int64
	state      *SessionState
	client     *chproto.Codec
	up         atomic.Pointer[chproto.Codec]
	clientConn net.Conn
	closeOnce  sync.Once
	closeErr   error
}

// New wraps clientConn in a Codec and constructs a Session. No network
// traffic occurs; handshake is Relay's responsibility.
func New(id int64, clientConn net.Conn) Session {
	return &sessionImpl{
		id:         id,
		state:      NewSessionState(),
		client:     chproto.NewCodec(clientConn, chproto.DirFromClient),
		clientConn: clientConn,
	}
}

func (s *sessionImpl) ID() int64              { return s.id }
func (s *sessionImpl) State() *SessionState   { return s.state }
func (s *sessionImpl) Client() *chproto.Codec { return s.client }
func (s *sessionImpl) RemoteAddr() net.Addr {
	if s.clientConn == nil {
		return nil
	}
	return s.clientConn.RemoteAddr()
}

func (s *sessionImpl) Upstream() *chproto.Codec { return s.up.Load() }

// BindUpstream sets the upstream codec. Errors if already bound.
func (s *sessionImpl) BindUpstream(_ context.Context, up *chproto.Codec) error {
	if !s.up.CompareAndSwap(nil, up) {
		return fmt.Errorf("%w: upstream already bound", ErrRebindDenied)
	}
	return nil
}

// RebindUpstream atomically swaps the upstream codec. Caller is responsible
// for having performed the new upstream's handshake. When replayState is
// true, SessionState.Replay is invoked after the swap.
//
// MVP has no production caller. See spec §5.4.
func (s *sessionImpl) RebindUpstream(ctx context.Context, newUp *chproto.Codec, replayState bool) error {
	if newUp == nil {
		return fmt.Errorf("%w: nil upstream", ErrRebindDenied)
	}
	old := s.up.Swap(newUp)
	if old != nil {
		if closer, ok := old.Conn().(interface{ Close() error }); ok {
			go func() { _ = closer.Close() }()
		}
	}
	if replayState {
		return s.state.Replay(ctx, newUp)
	}
	return nil
}

// RebindToPeer implements Session.RebindToPeer.
func (s *sessionImpl) RebindToPeer(ctx context.Context, newUp *chproto.Codec, peerHello *chproto.ClientHello) error {
	hs, err := s.handshakeNewUpstream(newUp, peerHello, "rebind-to-peer")
	if err != nil {
		return err
	}
	s.state.SetUpstreamHello(hs.upstreamHello)
	// Store the raw ServerHello bytes and negotiated revision so that
	// relay.handshake can echo them to the client without re-running the
	// upstream hello exchange (RebindToPeer already completed it).
	s.state.mu.Lock()
	s.state.PeerServerHelloRaw = hs.serverHelloRaw
	s.state.PeerRevision = hs.rev
	s.state.mu.Unlock()
	s.swapAndCloseOld(newUp)
	return nil
}

// RebindToLocal implements Session.RebindToLocal.
func (s *sessionImpl) RebindToLocal(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error {
	hs, err := s.handshakeNewUpstream(newUp, hello, "rebind-to-local")
	if err != nil {
		return err
	}
	s.state.SetUpstreamHello(hs.upstreamHello)
	// Clear forward state — the session is back home. Done BEFORE the
	// upstream swap so the chain's filter sees the reset state by the
	// time clientToUpstream re-fetches the upstream and continues with
	// the next packet.
	s.state.mu.Lock()
	if hello.Database != "" {
		s.state.Database = hello.Database
	}
	s.state.IsForwarding = false
	s.state.RouteTarget = ""
	// PeerServerHelloRaw / PeerRevision are intentionally left as-is;
	// they were populated by the original RebindToPeer at handshake
	// time and relay.handshake has already consumed them.
	s.state.mu.Unlock()
	s.swapAndCloseOld(newUp)
	return s.state.ReplaySettings(ctx, newUp)
}

// SwitchUpstream implements Session.SwitchUpstream.
func (s *sessionImpl) SwitchUpstream(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error {
	closeNew := func() {
		if newUp == nil {
			return
		}
		if closer, ok := newUp.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	if hello == nil {
		closeNew()
		return fmt.Errorf("%w: switch-upstream: nil hello", ErrRebindDenied)
	}
	if newUp == nil {
		return fmt.Errorf("%w: switch-upstream: nil upstream", ErrRebindDenied)
	}
	snap := s.state.Snapshot()
	if snap.ClientRevision <= 0 {
		closeNew()
		return fmt.Errorf("%w: switch-upstream: the client leg has no negotiated revision", ErrRebindDenied)
	}
	// Relay forwards upstream packets at the upstream revision to a client
	// parsing at its own, so offer exactly the client leg's revision.
	clamped := *hello
	clamped.ProtocolVersion = snap.ClientRevision

	hs, err := s.handshakeWithContext(ctx, newUp, &clamped)
	if err != nil {
		closeNew()
		return err
	}
	if hs.rev != snap.ClientRevision {
		closeNew()
		return fmt.Errorf("%w: switch-upstream negotiated %d, client leg uses %d", ErrUpstreamRevisionTooLow, hs.rev, snap.ClientRevision)
	}
	if snap.Timezone != "" && hs.serverHello.Timezone != snap.Timezone {
		closeNew()
		return fmt.Errorf("%w: switch-upstream server timezone %q, client received %q", ErrUpstreamTimezoneMismatch, hs.serverHello.Timezone, snap.Timezone)
	}
	s.state.SetUpstreamHello(hs.upstreamHello)
	s.swapAndCloseOld(newUp)
	return nil
}

// handshakeWithContext runs handshakeNewUpstream bounded by ctx: the ctx
// deadline becomes the conn deadline (cleared afterwards) and a ctx
// cancellation closes the conn to unblock the exchange. The caller owns
// newUp and closes it on error.
func (s *sessionImpl) handshakeWithContext(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) (upstreamHandshake, error) {
	if err := ctx.Err(); err != nil {
		return upstreamHandshake{}, fmt.Errorf("switch-upstream: %w", err)
	}
	conn, _ := newUp.Conn().(interface{ SetDeadline(time.Time) error })
	if deadline, ok := ctx.Deadline(); ok && conn != nil {
		_ = conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() {
		if closer, ok := newUp.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	})
	hs, err := s.handshakeNewUpstream(newUp, hello, "switch-upstream")
	if !stop() {
		// ctx ended during the exchange: the conn is closed or being closed.
		if err != nil {
			return upstreamHandshake{}, fmt.Errorf("switch-upstream: %w: %w", ctx.Err(), err)
		}
		return upstreamHandshake{}, fmt.Errorf("switch-upstream: %w", ctx.Err())
	}
	if err != nil {
		if ctxErr := expiredContextErr(ctx); ctxErr != nil {
			return upstreamHandshake{}, fmt.Errorf("switch-upstream: %w: %w", ctxErr, err)
		}
		return upstreamHandshake{}, err
	}
	if conn != nil {
		_ = conn.SetDeadline(time.Time{})
	}
	return hs, nil
}

// expiredContextErr reports why ctx no longer allows work. The conn deadline
// copied from ctx can fire a moment before ctx's own timer does, so a passed
// deadline counts as context.DeadlineExceeded even while ctx.Err() is nil.
func expiredContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// upstreamHandshake is the outcome of handshakeNewUpstream.
type upstreamHandshake struct {
	rev            int                  // negotiated revision, already set on the codec
	serverHello    *chproto.ServerHello // decoded ServerHello
	serverHelloRaw []byte               // raw ServerHello packet bytes
	upstreamHello  *chproto.ClientHello // the hello actually sent (revision-capped)
}

// handshakeNewUpstream runs the standard ClientHello → ServerHello →
// addendum exchange on a fresh upstream codec without touching session
// state. Shared between RebindToPeer, RebindToLocal and SwitchUpstream so
// the three paths agree on protocol-revision negotiation and addendum
// semantics. The result carries the hello it actually sent (revision-capped
// by ClientHelloForUpstream); each caller stores it with SetUpstreamHello
// only once it commits to the switch. errPrefix is used in error messages so
// callers stay distinguishable in logs.
func (s *sessionImpl) handshakeNewUpstream(newUp *chproto.Codec, hello *chproto.ClientHello, errPrefix string) (upstreamHandshake, error) {
	if newUp == nil {
		return upstreamHandshake{}, fmt.Errorf("%w: nil upstream", ErrRebindDenied)
	}
	upstreamHello := chproto.ClientHelloForUpstream(hello)
	if err := newUp.WriteClientHello(upstreamHello); err != nil {
		return upstreamHandshake{}, fmt.Errorf("%s write hello: %w", errPrefix, err)
	}
	newUp.SetServerHelloRevisionHint(int(upstreamHello.ProtocolVersion))
	srvPkt, err := newUp.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return upstreamHandshake{}, fmt.Errorf("%s read server-hello: %w", errPrefix, err)
	}
	if exc, ok := srvPkt.Decoded.(*chproto.Exception); ok {
		return upstreamHandshake{}, fmt.Errorf("%s: upstream rejected handshake: code=%d %s: %s", errPrefix, exc.Code, exc.Name, exc.Message)
	}
	srv, ok := srvPkt.Decoded.(*chproto.ServerHello)
	if !ok {
		return upstreamHandshake{}, fmt.Errorf("%s: unexpected packet type=%d (want ServerHello=%d): %w",
			errPrefix, srvPkt.Type, chproto.ServerHelloCode, chproto.ErrDecode)
	}
	rev := int(upstreamHello.ProtocolVersion)
	if int(srv.Revision) < rev {
		rev = int(srv.Revision)
	}
	newUp.SetRevision(rev)
	if chproto.SupportsAddendum(rev) {
		res := newUp.ResolveUpstreamAddendum(chproto.AddendumResult{}, chproto.AddendumOpts{
			ProposedRecv: "chunked_optional",
			ProposedSend: "chunked_optional",
		})
		if err := newUp.SendAddendum(res); err != nil {
			return upstreamHandshake{}, fmt.Errorf("%s send addendum: %w", errPrefix, err)
		}
	}
	return upstreamHandshake{rev: rev, serverHello: srv, serverHelloRaw: srvPkt.Raw, upstreamHello: upstreamHello}, nil
}

// swapAndCloseOld atomically replaces the bound upstream with newUp
// and asynchronously closes the previous upstream's underlying conn.
// Async close avoids holding the relay's clientToUpstream goroutine
// while the kernel finishes the FIN exchange.
func (s *sessionImpl) swapAndCloseOld(newUp *chproto.Codec) {
	old := s.up.Swap(newUp)
	if old != nil {
		if closer, ok := old.Conn().(interface{ Close() error }); ok {
			go func() { _ = closer.Close() }()
		}
	}
}

// Close tears down the session. Idempotent via sync.Once.
func (s *sessionImpl) Close() error {
	s.closeOnce.Do(func() {
		if s.clientConn != nil {
			_ = s.clientConn.Close()
		}
		if up := s.up.Load(); up != nil {
			if closer, ok := up.Conn().(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
	})
	return s.closeErr
}
