package proxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
)

// Server accepts TCP connections, constructs a Session per connection, and
// hands it to Relay.
//
// UpstreamDialer is mode-specific (server / sidecar) and supplied at
// construction time. Hooks is the composed plugin chain.
//
// Design: docs/superpowers/specs/2026-04-21-clickhouse-tcp-conn-interface-design.md
type Server struct {
	Hooks          plugin.Hooks
	UpstreamDialer func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error)

	// Observer receives per-packet and per-byte wire-level metrics from
	// Relay. Optional — may be nil in tests. Kept separate from Hooks
	// because these fire at 10^4-10^6 Hz and should not traverse a
	// variadic plugin dispatch.
	Observer PacketObserver

	// PreflagSession, if non-nil, is invoked on the freshly-created
	// SessionState immediately after the connection is accepted, before
	// OnConnect/OnHello run. Intended for listener-level state injection
	// (e.g. internal-port pre-flagging IsPeerTrusted + IsInternalPort).
	PreflagSession func(*chsession.SessionState)

	// ShutdownTimeout bounds how long Serve waits for in-flight sessions to
	// finish after ctx is cancelled. Zero means wait indefinitely (sessions
	// are given unlimited time to drain). Callers typically populate this
	// from config.ShutdownTimeout (default 30s).
	ShutdownTimeout time.Duration

	nextID atomic.Int64
	wg     sync.WaitGroup
}

// NewServer constructs a Server with the given hooks and upstream-dialer.
// The dialer is called once per client connection to produce the session's
// upstream codec. If hooks is nil, plugin.NoopHooks is used.
func NewServer(hooks plugin.Hooks, dial func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error)) *Server {
	if hooks == nil {
		hooks = plugin.NoopHooks{}
	}
	return &Server{Hooks: hooks, UpstreamDialer: dial}
}

// NewServerWithObserver is a convenience wrapper that also attaches a
// wire-level PacketObserver. Callers may achieve the same effect by
// setting s.Observer on a zero-initialised Server.
func NewServerWithObserver(hooks plugin.Hooks, dial func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error), obs PacketObserver) *Server {
	s := NewServer(hooks, dial)
	s.Observer = obs
	return s
}

// Serve accepts connections from ln and handles each in its own goroutine.
// When ctx is cancelled, the listener is closed so Accept unblocks, and Serve
// waits for all in-flight sessions to finish (bounded by ShutdownTimeout) before
// returning ctx.Err().
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	// Close the listener when ctx is cancelled so Accept returns immediately.
	acceptDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-acceptDone:
		}
	}()
	defer close(acceptDone)

	for {
		c, err := ln.Accept()
		if err != nil {
			// After ctx.Done() the listener is closed; Accept returns an error.
			// Wait for in-flight sessions (bounded by ShutdownTimeout) then return.
			if ctx.Err() != nil {
				s.waitForDrain()
				return ctx.Err()
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, c)
		}()
	}
}

// waitForDrain blocks until all in-flight sessions finish or ShutdownTimeout
// elapses. Zero ShutdownTimeout means wait indefinitely (same as WaitGroup
// with no timeout).
func (s *Server) waitForDrain() {
	if s.ShutdownTimeout == 0 {
		s.wg.Wait()
		return
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(s.ShutdownTimeout):
		log.Warnw("server: shutdown timeout reached; abandoning in-flight sessions",
			"timeout", s.ShutdownTimeout,
		)
	}
}

// handle processes one client TCP connection: build a Session, enrich ctx
// with a session-scoped logger, dial upstream, bind, run Relay, close.
//
// The enriched ctx flows into the UpstreamDialer, BindUpstream, and
// Relay.Run — anything downstream that calls log.FromContext(ctx) gets a
// logger pre-populated with session and remote_addr fields, so per-session
// logs are trivially filterable in downstream tooling.
func (s *Server) handle(ctx context.Context, c net.Conn) {
	id := s.nextID.Add(1)
	sess := chsession.New(id, c)
	defer func() { _ = sess.Close() }()

	if s.PreflagSession != nil {
		s.PreflagSession(sess.State())
	}

	ctx, logger := log.FromContext(ctx, "session", id, "remote_addr", c.RemoteAddr().String())

	// OnConnect / OnDisconnect frame the entire TCP-connection lifetime and
	// fire regardless of whether the upstream dial or handshake succeeds.
	// That's what keeps connection-lifecycle metrics (active gauge,
	// error counters on dial failure) balanced — the alternative of
	// relying on OnClose would leak Inc()'s for every failed dial, since
	// OnClose is only called by Relay.Run after BindUpstream succeeds.
	if err := s.Hooks.OnConnect(ctx, sess); err != nil {
		logger.Warne(err, "OnConnect rejected connection")
		s.Hooks.OnDisconnect(sess)
		return
	}
	defer s.Hooks.OnDisconnect(sess)

	logger.Debugw("session started")
	// Dial happens inside Relay.handshake AFTER OnHello so routing
	// plugins can populate SessionState.RouteTarget before the dialer
	// reads it. Wrapping it as the closure passed in keeps the
	// per-mode dialer signature stable (server / sidecar / forwarding
	// each plug their own).
	r := NewRelay(sess, s.Hooks, s.Observer, UpstreamDialer(s.UpstreamDialer))
	if err := r.Run(ctx); err != nil {
		logger.Warne(err, "relay run ended")
	}
}
