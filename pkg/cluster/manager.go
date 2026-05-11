package cluster

import (
	"context"
	"fmt"
	"net"
	"strings"

	"housegate/housegate/pkg/log"
)

// Manager is the top-level cluster manager for a single shard.
// It integrates connection pools, health checking, and routing.
type Manager struct {
	shard       ShardConfig
	pools       map[string]*ReplicaPool // key: "host:port"
	healthCheck *HealthChecker
	router      *Router
	poolConfig  PoolConfig
}

// NewManager creates a cluster manager for a shard with its replicas.
// It sets up per-replica connection pools, health checker, and router.
func NewManager(shard ShardConfig, hcConfig *HealthCheckConfig, poolCfg *PoolConfig, routingCfg *RoutingConfig) (*Manager, error) {
	if len(shard.Replicas) == 0 {
		return nil, fmt.Errorf("shard %q has no replicas configured", shard.Name)
	}

	// Apply defaults
	var hc HealthCheckConfig
	if hcConfig != nil {
		hc = *hcConfig
	} else {
		hc = DefaultHealthCheckConfig()
	}

	var pc PoolConfig
	if poolCfg != nil {
		pc = *poolCfg
	} else {
		pc = DefaultPoolConfig()
	}

	var rc RoutingConfig
	if routingCfg != nil {
		rc = *routingCfg
	} else {
		rc = DefaultRoutingConfig()
	}

	// Create per-replica pools
	pools := make(map[string]*ReplicaPool, len(shard.Replicas))
	for _, replica := range shard.Replicas {
		addr := replica.Addr()
		pools[addr] = NewReplicaPool(replica, pc)
	}

	// Create health checker
	healthChecker := NewHealthChecker(shard.Replicas, hc)

	// Create router
	strategy := ParseRoutingStrategy(rc.Strategy)
	router := NewRouter(strategy, healthChecker, pools)

	log.Infow("manager created",
		"shard", shard.Name,
		"replicas", len(shard.Replicas),
		"strategy", rc.Strategy)
	for _, r := range shard.Replicas {
		log.Infow("replica registered", "addr", r.Addr(), "weight", r.Weight, "backup", r.IsBackup)
	}

	return &Manager{
		shard:       shard,
		pools:       pools,
		healthCheck: healthChecker,
		router:      router,
		poolConfig:  pc,
	}, nil
}

// Start begins health checking and pool eviction goroutines.
func (m *Manager) Start(ctx context.Context) {
	// Start health checker
	go m.healthCheck.Start(ctx)

	// Start pool eviction for each replica
	for _, pool := range m.pools {
		go pool.RunEviction(ctx)
	}

	log.Infow("shard started: health checking and pool eviction running", "shard", m.shard.Name)
}

// GetConnection selects a healthy replica and returns a pooled connection.
// The caller must call ReturnConnection or DiscardConnection when done.
func (m *Manager) GetConnection(ctx context.Context) (*PooledConn, error) {
	replica, err := m.router.SelectReplica()
	if err != nil {
		return nil, fmt.Errorf("shard %s: %w", m.shard.Name, err)
	}

	pool, ok := m.pools[replica.Addr()]
	if !ok {
		return nil, fmt.Errorf("no pool for replica %s", replica.Addr())
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		// Record failure for health checker (passive detection)
		m.healthCheck.RecordQueryError(replica)
		return nil, fmt.Errorf("get connection from replica %s: %w", replica.Addr(), err)
	}

	return conn, nil
}

// GetConnectionForReplica gets a connection to a specific replica (used for direct routing).
func (m *Manager) GetConnectionForReplica(ctx context.Context, addr string) (*PooledConn, error) {
	pool, ok := m.pools[addr]
	if !ok {
		return nil, fmt.Errorf("no pool for replica %s", addr)
	}
	return pool.Get(ctx)
}

// ReturnConnection returns a connection to its pool.
func (m *Manager) ReturnConnection(conn *PooledConn) {
	if conn == nil {
		return
	}
	conn.ReturnToPool()
}

// DiscardConnection closes a connection without returning it to the pool.
// Use when the connection encountered an error.
func (m *Manager) DiscardConnection(conn *PooledConn) {
	if conn == nil {
		return
	}
	if conn.pool != nil {
		conn.pool.Discard(conn)
	} else {
		conn.Conn.Close()
	}
}

// RecordQuerySuccess records a successful query for passive health detection.
func (m *Manager) RecordQuerySuccess(replica ReplicaConfig) {
	m.healthCheck.RecordQuerySuccess(replica)
}

// RecordQueryError records a failed query for passive health detection.
func (m *Manager) RecordQueryError(replica ReplicaConfig) {
	m.healthCheck.RecordQueryError(replica)
}

// Shard returns the shard configuration.
func (m *Manager) Shard() ShardConfig {
	return m.shard
}

// ReplicaAddrs returns all replica addresses for this shard.
func (m *Manager) ReplicaAddrs() []string {
	addrs := make([]string, len(m.shard.Replicas))
	for i, r := range m.shard.Replicas {
		addrs[i] = r.Addr()
	}
	return addrs
}

// HasReplica checks if the given address matches any replica in this shard.
// addr can be "host:port" format; host comparison is case-insensitive.
func (m *Manager) HasReplica(addr string) bool {
	// Normalize the input address
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// Try as just host
		host = addr
		port = ""
	}

	for _, replica := range m.shard.Replicas {
		rHost := replica.Host
		rPort := fmt.Sprintf("%d", replica.Port)

		if strings.EqualFold(host, rHost) {
			if port == "" || port == rPort {
				return true
			}
		}
	}
	return false
}

// HealthChecker returns the underlying health checker (for metrics).
func (m *Manager) HealthChecker() *HealthChecker {
	return m.healthCheck
}

// Close shuts down all pools and stops health checking.
func (m *Manager) Close() {
	for addr, pool := range m.pools {
		pool.Close()
		log.Infow("pool for replica closed", "addr", addr)
	}
}

// NewSingleReplicaManager creates a manager with a single replica,
// used for backward compatibility when only "upstream" is configured.
func NewSingleReplicaManager(upstream string) (*Manager, error) {
	host, portStr, err := net.SplitHostPort(upstream)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream address %q: %w", upstream, err)
	}
	port := 9000 // default
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return nil, fmt.Errorf("invalid port in upstream address %q: %w", upstream, err)
	}

	shard := ShardConfig{
		Name: "default",
		Replicas: []ReplicaConfig{
			{Host: host, Port: port, Weight: 1},
		},
	}

	return NewManager(shard, nil, nil, nil)
}
