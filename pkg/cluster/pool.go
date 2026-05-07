package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "sentioxyz/sentio-core/common/log"
)

// PooledConn wraps a net.Conn with pool metadata.
// When the connection is done being used, call Close() to close the TCP connection
// and decrement the pool's active count.
type PooledConn struct {
	net.Conn
	createdAt time.Time
	lastUsed  time.Time
	replica   ReplicaConfig
	pool      *ReplicaPool
	inUse     atomic.Bool // true while checked out to a caller
	tcpClosed atomic.Bool // true after underlying TCP Close() called
}

// Replica returns the ReplicaConfig this connection belongs to.
func (c *PooledConn) Replica() ReplicaConfig {
	return c.replica
}

// Close closes the underlying TCP connection.
// If the connection is currently in use (checked out), it also decrements
// the pool's active count. Safe to call multiple times.
func (c *PooledConn) Close() error {
	// Decrement active count only if transitioning from in-use to not-in-use
	if c.inUse.CompareAndSwap(true, false) {
		if c.pool != nil {
			c.pool.activeCount.Add(-1)
		}
	}
	// Close the underlying TCP connection (idempotent via atomic guard)
	if c.tcpClosed.CompareAndSwap(false, true) {
		return c.Conn.Close()
	}
	return nil
}

// ReturnToPool returns this connection back to its pool for reuse.
// If the pool is nil or the connection should be discarded, it closes the connection.
func (c *PooledConn) ReturnToPool() {
	if c.pool != nil {
		c.pool.Put(c)
	} else {
		c.Close()
	}
}

// ReplicaPool manages backend connections to a single ClickHouse replica.
type ReplicaPool struct {
	replica ReplicaConfig
	config  PoolConfig

	mu     sync.Mutex
	idle   []*PooledConn
	closed bool

	activeCount atomic.Int32
	totalDials  atomic.Int64
}

// NewReplicaPool creates a new connection pool for a single replica.
func NewReplicaPool(replica ReplicaConfig, config PoolConfig) *ReplicaPool {
	return &ReplicaPool{
		replica: replica,
		config:  config,
		idle:    make([]*PooledConn, 0, config.MaxIdleConns),
	}
}

// Get borrows a connection from the pool, or dials a new one if none are available.
func (p *ReplicaPool) Get(ctx context.Context) (*PooledConn, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool closed for replica %s", p.replica.Addr())
	}

	// Try to get an idle connection
	now := time.Now()
	for len(p.idle) > 0 {
		// Pop from end (LIFO — most recently used connection)
		n := len(p.idle) - 1
		conn := p.idle[n]
		p.idle[n] = nil
		p.idle = p.idle[:n]

		// Check if connection is still valid
		if p.isConnectionExpired(conn, now) {
			conn.Conn.Close() // direct close since idle conn is not "in use"
			continue
		}

		p.mu.Unlock()
		conn.lastUsed = now
		conn.inUse.Store(true)
		p.activeCount.Add(1)
		return conn, nil
	}

	// Check max open connections limit
	if p.config.MaxOpenConns > 0 && int(p.activeCount.Load()) >= p.config.MaxOpenConns {
		p.mu.Unlock()
		return nil, fmt.Errorf("max open connections (%d) reached for replica %s", p.config.MaxOpenConns, p.replica.Addr())
	}

	p.mu.Unlock()

	// Dial a new connection
	conn, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}

	conn.inUse.Store(true)
	p.activeCount.Add(1)
	p.totalDials.Add(1)
	return conn, nil
}

// Put returns a connection to the pool. If the pool is full or the connection
// is expired, the connection is closed instead.
func (p *ReplicaPool) Put(conn *PooledConn) {
	if conn == nil {
		return
	}

	// Transition from in-use to idle: decrement active count
	if conn.inUse.CompareAndSwap(true, false) {
		p.activeCount.Add(-1)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || conn.tcpClosed.Load() {
		conn.Conn.Close()
		return
	}

	now := time.Now()

	// Don't return expired connections
	if p.isConnectionExpired(conn, now) {
		conn.Conn.Close()
		return
	}

	// Don't exceed max idle
	if p.config.MaxIdleConns > 0 && len(p.idle) >= p.config.MaxIdleConns {
		conn.Conn.Close()
		return
	}

	conn.lastUsed = now
	p.idle = append(p.idle, conn)
}

// Discard closes the connection without returning it to the pool.
// Use this when the connection encountered an error.
func (p *ReplicaPool) Discard(conn *PooledConn) {
	if conn == nil {
		return
	}
	conn.Close() // Close handles active count decrement via closeOnce
}

// ActiveCount returns the number of connections currently in use.
func (p *ReplicaPool) ActiveCount() int32 {
	return p.activeCount.Load()
}

// IdleCount returns the number of idle connections in the pool.
func (p *ReplicaPool) IdleCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

// Close closes all idle connections and marks the pool as closed.
func (p *ReplicaPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	for _, conn := range p.idle {
		conn.Conn.Close()
	}
	p.idle = nil
}

// RunEviction periodically evicts expired idle connections.
func (p *ReplicaPool) RunEviction(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.evictExpired()
		}
	}
}

func (p *ReplicaPool) evictExpired() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	n := 0
	for _, conn := range p.idle {
		if p.isConnectionExpired(conn, now) {
			conn.Conn.Close()
		} else {
			p.idle[n] = conn
			n++
		}
	}
	evicted := len(p.idle) - n

	// Clear references to allow GC
	for i := n; i < len(p.idle); i++ {
		p.idle[i] = nil
	}
	p.idle = p.idle[:n]

	if evicted > 0 {
		log.Infow("evicted expired idle connections", "addr", p.replica.Addr(), "evicted", evicted, "remaining", n)
	}
}

func (p *ReplicaPool) isConnectionExpired(conn *PooledConn, now time.Time) bool {
	// Check max lifetime
	if p.config.ConnMaxLifetime.Duration > 0 && now.Sub(conn.createdAt) > p.config.ConnMaxLifetime.Duration {
		return true
	}
	// Check max idle time
	if p.config.ConnMaxIdleTime.Duration > 0 && now.Sub(conn.lastUsed) > p.config.ConnMaxIdleTime.Duration {
		return true
	}
	return false
}

func (p *ReplicaPool) dial(ctx context.Context) (*PooledConn, error) {
	timeout := p.config.DialTimeout.Duration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}

	addr := p.replica.Addr()
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial replica %s: %w", addr, err)
	}

	// Enable TCP optimizations
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
		tc.SetNoDelay(true)
	}

	now := time.Now()
	return &PooledConn{
		Conn:      conn,
		createdAt: now,
		lastUsed:  now,
		replica:   p.replica,
		pool:      p,
	}, nil
}
