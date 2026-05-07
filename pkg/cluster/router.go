package cluster

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	log "sentioxyz/sentio-core/common/log"
)

// RoutingStrategy defines how to select a replica within a shard.
type RoutingStrategy int

const (
	StrategyRoundRobin RoutingStrategy = iota
	StrategyRandom
	StrategyLeastConn
	StrategyWeighted
)

// ParseRoutingStrategy parses a strategy name string.
func ParseRoutingStrategy(name string) RoutingStrategy {
	switch name {
	case "round-robin", "roundrobin", "":
		return StrategyRoundRobin
	case "random":
		return StrategyRandom
	case "least-conn", "leastconn":
		return StrategyLeastConn
	case "weighted":
		return StrategyWeighted
	default:
		log.Warnw("unknown strategy, defaulting to round-robin", "strategy", name)
		return StrategyRoundRobin
	}
}

// Router selects healthy replicas based on the configured strategy.
type Router struct {
	strategy    RoutingStrategy
	healthCheck *HealthChecker
	pools       map[string]*ReplicaPool // key: "host:port" → pool (for least-conn)
	mu          sync.Mutex
	rrIndex     atomic.Uint64 // for round-robin
}

// NewRouter creates a router with the given strategy.
func NewRouter(strategy RoutingStrategy, healthCheck *HealthChecker, pools map[string]*ReplicaPool) *Router {
	return &Router{
		strategy:    strategy,
		healthCheck: healthCheck,
		pools:       pools,
	}
}

// SelectReplica picks a healthy replica based on the configured strategy.
// Returns an error if no healthy replicas are available.
func (r *Router) SelectReplica() (ReplicaConfig, error) {
	healthy := r.healthCheck.HealthyReplicas()

	// Filter out backup replicas if there are non-backup healthy replicas
	primary := filterNonBackup(healthy)
	if len(primary) > 0 {
		healthy = primary
	}
	// If only backup replicas are healthy, use them

	if len(healthy) == 0 {
		return ReplicaConfig{}, fmt.Errorf("no healthy replicas available")
	}

	switch r.strategy {
	case StrategyRoundRobin:
		return r.selectRoundRobin(healthy), nil
	case StrategyRandom:
		return r.selectRandom(healthy), nil
	case StrategyLeastConn:
		return r.selectLeastConn(healthy), nil
	case StrategyWeighted:
		return r.selectWeighted(healthy), nil
	default:
		return r.selectRoundRobin(healthy), nil
	}
}

func (r *Router) selectRoundRobin(replicas []ReplicaConfig) ReplicaConfig {
	idx := r.rrIndex.Add(1)
	return replicas[int(idx-1)%len(replicas)]
}

func (r *Router) selectRandom(replicas []ReplicaConfig) ReplicaConfig {
	return replicas[rand.Intn(len(replicas))]
}

func (r *Router) selectLeastConn(replicas []ReplicaConfig) ReplicaConfig {
	var best ReplicaConfig
	bestCount := int32(1<<31 - 1) // max int32

	for _, replica := range replicas {
		pool, ok := r.pools[replica.Addr()]
		if !ok {
			// No pool means no active connections — best candidate
			return replica
		}
		count := pool.ActiveCount()
		if count < bestCount {
			bestCount = count
			best = replica
		}
	}
	return best
}

func (r *Router) selectWeighted(replicas []ReplicaConfig) ReplicaConfig {
	totalWeight := 0
	for _, replica := range replicas {
		w := replica.Weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w
	}

	pick := rand.Intn(totalWeight)
	cumulative := 0
	for _, replica := range replicas {
		w := replica.Weight
		if w <= 0 {
			w = 1
		}
		cumulative += w
		if pick < cumulative {
			return replica
		}
	}

	// Fallback (should not reach here)
	return replicas[0]
}

// filterNonBackup filters out backup replicas.
func filterNonBackup(replicas []ReplicaConfig) []ReplicaConfig {
	var result []ReplicaConfig
	for _, r := range replicas {
		if !r.IsBackup {
			result = append(result, r)
		}
	}
	return result
}
