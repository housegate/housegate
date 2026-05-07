package chsession

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"housegate/housegate/pkg/chproto"
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
	// swaps the bound upstream to newUp and replays Database+Settings via
	// SessionState.Replay.
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
	rev, srvHelloRaw, err := s.handshakeNewUpstream(newUp, peerHello, "rebind-to-peer")
	if err != nil {
		return err
	}
	// Store the raw ServerHello bytes and negotiated revision so that
	// relay.handshake can echo them to the client without re-running the
	// upstream hello exchange (RebindToPeer already completed it).
	s.state.mu.Lock()
	s.state.PeerServerHelloRaw = srvHelloRaw
	s.state.PeerRevision = rev
	s.state.mu.Unlock()
	s.swapAndCloseOld(newUp)
	return s.state.Replay(ctx, newUp)
}

// RebindToLocal implements Session.RebindToLocal.
func (s *sessionImpl) RebindToLocal(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error {
	if _, _, err := s.handshakeNewUpstream(newUp, hello, "rebind-to-local"); err != nil {
		return err
	}
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

// handshakeNewUpstream runs the standard ClientHello → ServerHello →
// addendum exchange on a fresh upstream codec without touching session
// state. Shared between RebindToPeer and RebindToLocal so the two
// rebind paths agree on protocol-revision negotiation and addendum
// semantics. errPrefix is used in error messages so callers stay
// distinguishable in logs.
func (s *sessionImpl) handshakeNewUpstream(newUp *chproto.Codec, hello *chproto.ClientHello, errPrefix string) (int, []byte, error) {
	if newUp == nil {
		return 0, nil, fmt.Errorf("%w: nil upstream", ErrRebindDenied)
	}
	if err := newUp.WriteClientHello(hello); err != nil {
		return 0, nil, fmt.Errorf("%s write hello: %w", errPrefix, err)
	}
	newUp.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	srvPkt, err := newUp.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return 0, nil, fmt.Errorf("%s read server-hello: %w", errPrefix, err)
	}
	if exc, ok := srvPkt.Decoded.(*chproto.Exception); ok {
		return 0, nil, fmt.Errorf("%s: upstream rejected handshake: code=%d %s: %s", errPrefix, exc.Code, exc.Name, exc.Message)
	}
	srv, ok := srvPkt.Decoded.(*chproto.ServerHello)
	if !ok {
		return 0, nil, fmt.Errorf("%s: unexpected packet type=%d (want ServerHello=%d): %w",
			errPrefix, srvPkt.Type, chproto.ServerHelloCode, chproto.ErrDecode)
	}
	rev := int(hello.ProtocolVersion)
	if int(srv.Revision) < rev {
		rev = int(srv.Revision)
	}
	newUp.SetRevision(rev)
	if chproto.SupportsAddendum(rev) {
		if err := newUp.SendAddendum(chproto.AddendumResult{
			NegotiatedSend: "notchunked",
			NegotiatedRecv: "notchunked",
		}); err != nil {
			return 0, nil, fmt.Errorf("%s send addendum: %w", errPrefix, err)
		}
	}
	return rev, srvPkt.Raw, nil
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
