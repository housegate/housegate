package keeper

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/log"
)

// Strategy selects which live keeper a new client connection is steered to.
type Strategy int

const (
	// AnyVoter steers to a random live member. Keeper followers
	// transparently forward writes to the leader over Raft, so any live
	// voter is a correct target.
	AnyVoter Strategy = iota
	// LeaderPref steers to the leader when one is known, else any live
	// member.
	LeaderPref
)

// ParseStrategy maps a config string to a Strategy (default AnyVoter).
func ParseStrategy(s string) Strategy {
	if s == "leader_pref" {
		return LeaderPref
	}
	return AnyVoter
}

// ServerConfig configures the keeper-facing proxy listener.
type ServerConfig struct {
	Tracker     *Tracker
	Strategy    Strategy
	DialTimeout time.Duration
	// ReconcileInterval controls how often live connections are checked
	// against the live member set; a connection whose upstream left the
	// quorum is dropped so the client reconnects and is re-steered.
	// Defaults to the tracker's probe interval.
	ReconcileInterval time.Duration
}

// Server is the CH-facing keeper proxy. It forwards each accepted keeper
// connection to a live quorum member and re-steers on membership change.
type Server struct {
	tracker     *Tracker
	strategy    Strategy
	dialTimeout time.Duration
	reconcile   time.Duration

	mu     sync.Mutex
	conns  map[int]*relayConn
	nextID int

	served  atomic.Int64
	dropped atomic.Int64
}

type relayConn struct {
	id       int
	client   net.Conn
	up       net.Conn
	upstream string
}

// NewServer builds a keeper proxy Server. Tracker is required.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Tracker == nil {
		return nil, errors.New("keeper.NewServer: Tracker is required")
	}
	dt := cfg.DialTimeout
	if dt <= 0 {
		dt = 3 * time.Second
	}
	rc := cfg.ReconcileInterval
	if rc <= 0 {
		rc = cfg.Tracker.probeInterval
	}
	return &Server{
		tracker:     cfg.Tracker,
		strategy:    cfg.Strategy,
		dialTimeout: dt,
		reconcile:   rc,
		conns:       map[int]*relayConn{},
	}, nil
}

// Serve runs the accept loop until ctx is cancelled, also starting the
// Tracker probe loop and the reconcile loop. The signature matches the
// housegate listener contract so proxyImpl can host it alongside the CH
// listeners.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go s.tracker.Run(ctx)
	go s.reconcileLoop(ctx)
	// Unblock Accept on shutdown.
	go func() { <-ctx.Done(); _ = ln.Close() }()

	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.dropAll()
				return nil
			default:
				return err
			}
		}
		go s.handle(ctx, c)
	}
}

func (s *Server) pickUpstream() string {
	live := s.tracker.Live()
	if len(live) == 0 {
		return ""
	}
	if s.strategy == LeaderPref {
		if l := s.tracker.Leader(); l != "" {
			return l
		}
	}
	return live[rand.Intn(len(live))]
}

func (s *Server) handle(ctx context.Context, client net.Conn) {
	upAddr := s.pickUpstream()
	if upAddr == "" {
		_ = client.Close()
		log.Warnw("keeper proxy: no live quorum member to steer to; dropping client")
		return
	}
	d := net.Dialer{Timeout: s.dialTimeout}
	up, err := d.DialContext(ctx, "tcp", upAddr)
	if err != nil {
		_ = client.Close()
		log.Warnw("keeper proxy: dial upstream failed", "upstream", upAddr, "err", err)
		return
	}
	rc := s.register(client, up, upAddr)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = up.Close()
	s.unregister(rc.id)
}

func (s *Server) register(client, up net.Conn, upstream string) *relayConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	rc := &relayConn{id: s.nextID, client: client, up: up, upstream: upstream}
	s.nextID++
	s.conns[rc.id] = rc
	s.served.Add(1)
	return rc
}

func (s *Server) unregister(id int) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

func (s *Server) reconcileLoop(ctx context.Context) {
	tk := time.NewTicker(s.reconcile)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			s.dropStale()
		}
	}
}

// dropStale closes connections whose upstream is no longer a live quorum
// member, forcing the client to reconnect and be re-steered onto a live
// member. Returns the number of connections dropped.
func (s *Server) dropStale() int {
	live := map[string]bool{}
	for _, a := range s.tracker.Live() {
		live[a] = true
	}
	s.mu.Lock()
	var victims []*relayConn
	for _, c := range s.conns {
		if !live[c.upstream] {
			victims = append(victims, c)
			delete(s.conns, c.id)
		}
	}
	s.mu.Unlock()
	for _, c := range victims {
		_ = c.client.Close()
		_ = c.up.Close()
	}
	s.dropped.Add(int64(len(victims)))
	return len(victims)
}

// dropAll closes every live connection. Used on shutdown.
func (s *Server) dropAll() int {
	s.mu.Lock()
	victims := make([]*relayConn, 0, len(s.conns))
	for _, c := range s.conns {
		victims = append(victims, c)
	}
	s.conns = map[int]*relayConn{}
	s.mu.Unlock()
	for _, c := range victims {
		_ = c.client.Close()
		_ = c.up.Close()
	}
	s.dropped.Add(int64(len(victims)))
	return len(victims)
}

// LiveConns returns the number of currently-relayed connections.
func (s *Server) LiveConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Served returns the total number of connections accepted since start.
func (s *Server) Served() int64 { return s.served.Load() }

// Dropped returns the total number of connections force-closed (stale
// re-steer + shutdown).
func (s *Server) Dropped() int64 { return s.dropped.Load() }
