// Package interserver implements housegate's CH-facing interserver proxy
// (link B of the keeper-pool design): the node-local ingress gateway for
// ClickHouse's part-replication port (9009).
//
// Each ClickHouse advertises this gateway's address as its
// interserver_http_host, so remote replicas fetch parts *through* the
// gateway while CH's real interserver port stays bound to loopback. The
// gateway is therefore the single externally-reachable interserver surface
// on the node and can be IP-allowlisted to peer subnets.
//
// It is L4 (protocol-unaware): interserver is HTTP part transfer, but the
// gateway only needs to relay bytes between the remote replica and the
// local CH. Note this is an INGRESS gateway, not a two-hop signing overlay:
// ClickHouse resolves each interserver target per-source from keeper, so
// there is no point at which a fetching replica's traffic can be routed
// through a co-located signing egress. Authenticated housegate↔housegate
// transport (mTLS between gateways) is a follow-up; IP allowlisting is the
// MVP edge control.
package interserver

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/log"
)

// Target returns the co-located ClickHouse's real interserver "host:port".
// A func (rather than a static string) so the address can be resolved after
// the gateway listener is already bound — the integration harness relies on
// this to break the CH⇄gateway startup ordering cycle.
type Target func() string

// ServerConfig configures the interserver ingress gateway.
type ServerConfig struct {
	// Target resolves the local CH interserver address. Required.
	Target Target
	// DialTimeout bounds the dial to the local CH (default 10s).
	DialTimeout time.Duration
	// AllowCIDRs, when non-empty, restricts which source IPs may use the
	// gateway (typically peer subnets). Empty allows all.
	AllowCIDRs []*net.IPNet
}

// Server is the interserver ingress gateway.
type Server struct {
	target      Target
	dialTimeout time.Duration
	allow       []*net.IPNet

	served    atomic.Int64
	rejected  atomic.Int64
	bytesUp   atomic.Int64 // client → CH
	bytesDown atomic.Int64 // CH → client

	mu     sync.Mutex
	conns  map[int]net.Conn
	nextID int
}

// NewServer builds an interserver gateway. Target is required.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Target == nil {
		return nil, errors.New("interserver.NewServer: Target is required")
	}
	dt := cfg.DialTimeout
	if dt <= 0 {
		dt = 10 * time.Second
	}
	return &Server{
		target:      cfg.Target,
		dialTimeout: dt,
		allow:       cfg.AllowCIDRs,
		conns:       map[int]net.Conn{},
	}, nil
}

// Serve runs the accept loop until ctx is cancelled. The signature matches
// the housegate listener contract so proxyImpl can host it alongside the CH
// and keeper listeners.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() { <-ctx.Done(); _ = ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.closeAll()
				return nil
			default:
				return err
			}
		}
		go s.handle(ctx, c)
	}
}

// allowed reports whether the source IP may use the gateway.
func (s *Server) allowed(remote net.Addr) bool {
	if len(s.allow) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range s.allow {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *Server) handle(ctx context.Context, client net.Conn) {
	if !s.allowed(client.RemoteAddr()) {
		s.rejected.Add(1)
		log.Warnw("interserver gateway: source not allowed", "remote", client.RemoteAddr().String())
		_ = client.Close()
		return
	}
	target := s.target()
	if target == "" {
		_ = client.Close()
		log.Warnw("interserver gateway: no local CH interserver target configured")
		return
	}
	d := net.Dialer{Timeout: s.dialTimeout}
	up, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		_ = client.Close()
		log.Warnw("interserver gateway: dial local CH interserver failed", "target", target, "err", err)
		return
	}
	id := s.register(client)
	s.served.Add(1)

	done := make(chan struct{}, 2)
	go func() { copyCounting(up, client, &s.bytesUp); done <- struct{}{} }()
	go func() { copyCounting(client, up, &s.bytesDown); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = up.Close()
	s.unregister(id)
}

// copyCounting streams src→dst, adding to counter as each chunk is read so
// the metric reflects in-flight bytes (not just bytes at connection close —
// interserver connections are HTTP keep-alive and stay open between fetches).
func copyCounting(dst io.Writer, src io.Reader, counter *atomic.Int64) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			counter.Add(int64(n))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

func (s *Server) register(c net.Conn) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	s.conns[id] = c
	return id
}

func (s *Server) unregister(id int) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

func (s *Server) closeAll() {
	s.mu.Lock()
	victims := make([]net.Conn, 0, len(s.conns))
	for _, c := range s.conns {
		victims = append(victims, c)
	}
	s.conns = map[int]net.Conn{}
	s.mu.Unlock()
	for _, c := range victims {
		_ = c.Close()
	}
}

// Served returns the total number of connections accepted and forwarded.
func (s *Server) Served() int64 { return s.served.Load() }

// Rejected returns the number of connections refused by the allowlist.
func (s *Server) Rejected() int64 { return s.rejected.Load() }

// Bytes returns the cumulative bytes relayed (client→CH, CH→client).
func (s *Server) Bytes() (up, down int64) {
	return s.bytesUp.Load(), s.bytesDown.Load()
}
