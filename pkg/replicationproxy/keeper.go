package replicationproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/log"
)

var ErrNoKeeperUpstreams = errors.New("replicationproxy: no keeper upstreams")

// KeeperOptions configures a raw Keeper/ZooKeeper TCP passthrough listener.
type KeeperOptions struct {
	Upstreams   []string
	DialTimeout time.Duration
}

// KeeperServer forwards each accepted client connection to one Keeper upstream.
type KeeperServer struct {
	upstreams   *keeperUpstreams
	dialTimeout time.Duration
}

// NewKeeperServer validates options and returns a protocol-agnostic TCP proxy.
func NewKeeperServer(options KeeperOptions) (*KeeperServer, error) {
	if len(options.Upstreams) == 0 {
		return nil, ErrNoKeeperUpstreams
	}
	if options.DialTimeout < 0 {
		return nil, fmt.Errorf("dial timeout %s: %w", options.DialTimeout, ErrInvalidKeeperOptions)
	}
	upstreams, err := newKeeperUpstreams(options.Upstreams)
	if err != nil {
		return nil, err
	}
	dialTimeout := options.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}
	return &KeeperServer{
		upstreams:   upstreams,
		dialTimeout: dialTimeout,
	}, nil
}

// Serve accepts Keeper/ZooKeeper TCP clients until ctx is canceled or ln fails.
func (s *KeeperServer) Serve(ctx context.Context, ln net.Listener) error {
	ctx, logger := log.FromContext(ctx, "component", "replicationproxy_keeper", "listen", ln.Addr().String())
	active := newKeeperConnSet()
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
			active.Close()
		case <-done:
		}
	}()
	defer func() {
		close(done)
		wg.Wait()
	}()

	for {
		client, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept keeper client: %w", err)
		}
		active.Add(client)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer active.Remove(client)
			s.handle(ctx, client)
		}()
		logger.Debugw("accepted keeper client", "remote", client.RemoteAddr().String())
	}
}

func (s *KeeperServer) handle(ctx context.Context, client net.Conn) {
	ctx, logger := log.FromContext(ctx, "client", client.RemoteAddr().String())
	upstream, upstreamAddr, err := s.dialUpstream(ctx)
	if err != nil {
		logger.Warnw("keeper upstream unavailable", "error", err)
		_ = client.Close()
		return
	}
	defer upstream.Close()
	defer client.Close()

	ctx, logger = log.FromContext(ctx, "upstream", upstreamAddr)
	logger.Debugw("keeper upstream connected")
	copyBothDirections(ctx, client, upstream, logger)
}

func (s *KeeperServer) dialUpstream(ctx context.Context) (net.Conn, string, error) {
	var lastErr error
	for _, addr := range s.upstreams.Candidates() {
		dialCtx := ctx
		cancel := func() {}
		if s.dialTimeout > 0 {
			dialCtx, cancel = context.WithTimeout(ctx, s.dialTimeout)
		}
		dialer := net.Dialer{}
		conn, err := dialer.DialContext(dialCtx, "tcp", addr)
		cancel()
		if err == nil {
			s.upstreams.Mark(addr, true)
			return conn, addr, nil
		}
		s.upstreams.Mark(addr, false)
		lastErr = fmt.Errorf("dial %s: %w", addr, err)
	}
	if lastErr != nil {
		return nil, "", fmt.Errorf("all keeper upstreams unavailable: %w", lastErr)
	}
	return nil, "", ErrNoKeeperUpstreams
}

func copyBothDirections(ctx context.Context, client, upstream net.Conn, logger *log.Logger) {
	var closeOnce sync.Once
	closeConns := func() {
		_ = client.Close()
		_ = upstream.Close()
	}
	errs := make(chan error, 2)
	copyConn := func(dst, src net.Conn, direction string) {
		_, err := io.Copy(dst, src)
		if err != nil && !isExpectedClose(err) {
			logger.Debugw("keeper copy ended with error", "direction", direction, "error", err)
		}
		closeOnce.Do(closeConns)
		errs <- err
	}
	go copyConn(upstream, client, "client_to_upstream")
	go copyConn(client, upstream, "upstream_to_client")

	for completed := 0; completed < 2; completed++ {
		select {
		case <-ctx.Done():
			closeOnce.Do(closeConns)
		case <-errs:
		}
	}
}

func isExpectedClose(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Err != nil && errors.Is(opErr.Err, net.ErrClosed)
}

type keeperConnSet struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newKeeperConnSet() *keeperConnSet {
	return &keeperConnSet{conns: make(map[net.Conn]struct{})}
}

func (s *keeperConnSet) Add(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[conn] = struct{}{}
}

func (s *keeperConnSet) Remove(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}

func (s *keeperConnSet) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.conns {
		_ = conn.Close()
	}
}
