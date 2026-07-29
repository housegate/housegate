package cluster

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housegate/housegate/pkg/log"
)

// ReplicaHealth tracks the health state of a single replica.
type ReplicaHealth struct {
	replica          ReplicaConfig
	healthy          atomic.Bool
	consecutiveFails atomic.Int32
	consecutiveOKs   atomic.Int32
	lastCheckTime    atomic.Value // stores time.Time
	lastLatency      atomic.Value // stores time.Duration
}

// NewReplicaHealth creates a health tracker for a replica.
// The replica starts as healthy (optimistic assumption).
func NewReplicaHealth(replica ReplicaConfig) *ReplicaHealth {
	rh := &ReplicaHealth{
		replica: replica,
	}
	rh.healthy.Store(true)
	rh.lastCheckTime.Store(time.Now())
	rh.lastLatency.Store(time.Duration(0))
	return rh
}

// IsHealthy returns the current health status.
func (rh *ReplicaHealth) IsHealthy() bool {
	return rh.healthy.Load()
}

// Replica returns the config.
func (rh *ReplicaHealth) Replica() ReplicaConfig {
	return rh.replica
}

// HealthChecker manages periodic health probes for all replicas in a shard.
type HealthChecker struct {
	replicas []*ReplicaHealth
	config   HealthCheckConfig
	mu       sync.RWMutex
}

// NewHealthChecker creates a health checker for the given replicas.
func NewHealthChecker(replicas []ReplicaConfig, config HealthCheckConfig) *HealthChecker {
	replicaHealths := make([]*ReplicaHealth, len(replicas))
	for i, r := range replicas {
		replicaHealths[i] = NewReplicaHealth(r)
	}
	return &HealthChecker{
		replicas: replicaHealths,
		config:   config,
	}
}

// Start begins periodic health checking for all replicas.
func (hc *HealthChecker) Start(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("HealthChecker.Start panic recovered", "panic", r)
		}
	}()

	interval := hc.config.Interval.Duration
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// Initial check
	hc.checkAll(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hc.checkAll(ctx)
		}
	}
}

// checkAll probes every replica concurrently.
func (hc *HealthChecker) checkAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, rh := range hc.replicas {
		wg.Add(1)
		go func(rh *ReplicaHealth) {
			defer wg.Done()
			hc.probeSingle(ctx, rh)
		}(rh)
	}
	wg.Wait()
}

// probeSingle performs a TCP dial probe against one replica.
// V1: simple TCP dial. V2 will upgrade to native protocol SELECT 1.
func (hc *HealthChecker) probeSingle(ctx context.Context, rh *ReplicaHealth) {
	timeout := hc.config.Timeout.Duration
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	start := time.Now()
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", rh.replica.Addr())
	latency := time.Since(start)
	rh.lastCheckTime.Store(time.Now())
	rh.lastLatency.Store(latency)

	if err != nil {
		hc.recordFailure(rh)
		return
	}
	conn.Close()
	hc.recordSuccess(rh)
}

// recordFailure increments consecutive failure count and may mark replica unhealthy.
func (hc *HealthChecker) recordFailure(rh *ReplicaHealth) {
	rh.consecutiveOKs.Store(0)
	fails := rh.consecutiveFails.Add(1)

	threshold := hc.config.FailureThreshold
	if threshold <= 0 {
		threshold = 3
	}

	if int(fails) >= threshold && rh.healthy.Load() {
		rh.healthy.Store(false)
		log.Warnw("replica marked UNHEALTHY", "addr", rh.replica.Addr(), "consecutive_failures", fails)
	}
}

// recordSuccess increments consecutive success count and may mark replica healthy.
func (hc *HealthChecker) recordSuccess(rh *ReplicaHealth) {
	rh.consecutiveFails.Store(0)
	oks := rh.consecutiveOKs.Add(1)

	threshold := hc.config.RecoveryThreshold
	if threshold <= 0 {
		threshold = 2
	}

	if int(oks) >= threshold && !rh.healthy.Load() {
		rh.healthy.Store(true)
		log.Infow("replica marked HEALTHY", "addr", rh.replica.Addr(), "consecutive_successes", oks)
	}
}

// RecordQueryError is called by the router/proxy when a query to this replica fails.
// This implements passive health detection.
func (hc *HealthChecker) RecordQueryError(replica ReplicaConfig) {
	for _, rh := range hc.replicas {
		if rh.replica.Addr() == replica.Addr() {
			hc.recordFailure(rh)
			return
		}
	}
}

// RecordQuerySuccess is called by the router/proxy when a query to this replica succeeds.
func (hc *HealthChecker) RecordQuerySuccess(replica ReplicaConfig) {
	for _, rh := range hc.replicas {
		if rh.replica.Addr() == replica.Addr() {
			hc.recordSuccess(rh)
			return
		}
	}
}

// IsHealthy returns whether a specific replica is currently healthy.
func (hc *HealthChecker) IsHealthy(replica ReplicaConfig) bool {
	for _, rh := range hc.replicas {
		if rh.replica.Addr() == replica.Addr() {
			return rh.IsHealthy()
		}
	}
	return false
}

// HealthyReplicas returns all currently healthy replicas.
func (hc *HealthChecker) HealthyReplicas() []ReplicaConfig {
	var result []ReplicaConfig
	for _, rh := range hc.replicas {
		if rh.IsHealthy() {
			result = append(result, rh.replica)
		}
	}
	return result
}

// AllReplicas returns all replica health states (for metrics/debugging).
func (hc *HealthChecker) AllReplicas() []*ReplicaHealth {
	return hc.replicas
}
