// Package registry is housegate's read-only view of the indexer network:
// where each indexer is reachable, what logical databases exist, and who
// has access. It defines the contracts (Topology, Databases, Access)
// housegate's proxy chain consumes; implementations are supplied by the
// embedder.
//
// The package deliberately avoids external producer types — its
// vocabulary is the narrow slice of fields housegate actually reads.
// Producers that have richer data should adapt at the boundary.
package registry

// ProxyAddress is the dialing target for a peer housegate instance
// fronting an indexer's ClickHouse. From housegate's perspective the
// remote endpoint is another housegate (which itself proxies CH), hence
// HousegatePort rather than the producer-side "ClickhouseProxyPort".
type ProxyAddress struct {
	Url           string
	HousegatePort uint16
}

// Registry is the union of Topology, Databases, and Access — the
// full read-only network view housegate consumes as a single
// dependency. Callers that need only a subset should depend on the
// narrower interface; this composite exists for wiring sites
// (proxy.Options, multi-method plugins) where it's natural to pass
// one value.
type Registry interface {
	Topology
	Databases
	Access
}

// KeeperPool is an OPTIONAL capability a Registry may implement to expose
// the current keeper quorum membership to housegate's keeper proxy
// (pkg/keeper). It is deliberately NOT part of Registry: deployments that
// don't host a keeper pool simply omit it, and the keeper proxy then runs
// on its statically configured members. Implementations that DO carry the
// pool membership (e.g. a chain-backed Registry observing the on-chain
// keeper-pool-changed event) let the proxy track live membership and
// re-steer when the quorum is reconfigured. build.go probes for it via a
// type assertion.
type KeeperPool interface {
	// KeeperPoolMembers returns the keeper-client endpoints (host:port) of
	// the current quorum, or nil if unknown. Implementations must return a
	// fresh slice each call. An empty/nil result is treated as "no update"
	// by the proxy (it keeps its current members rather than going dark).
	KeeperPoolMembers() []string
}

// Topology resolves indexer ids to dialing targets.
type Topology interface {
	// ProxyByIndexerId returns the dialing target for indexerId. The
	// (zero, false) return signals an unknown indexer; callers commonly
	// probe for existence and treat miss as a non-error.
	ProxyByIndexerId(indexerId uint64) (ProxyAddress, bool)

	// AllIndexers returns a snapshot of every known indexer keyed by id.
	// The returned map is owned by the caller and safe to retain or
	// mutate; implementations must return a fresh copy.
	AllIndexers() map[uint64]ProxyAddress
}
