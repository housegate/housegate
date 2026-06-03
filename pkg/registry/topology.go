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
// the keeper quorum membership of each named shard to housegate's keeper
// proxy (pkg/keeper). A deployment with one global pool registers a single
// shard (conventionally "default"); a deployment that runs §6 multi-shard
// keeper pools registers one shard per pool (e.g. "default", "shard_2").
// Build.go probes for this capability via a type assertion; deployments
// that don't carry the membership simply omit it and the keeper proxies
// run on their statically configured members.
type KeeperPool interface {
	// KeeperPoolMembers returns the keeper-client endpoints (host:port) of
	// the given shard's current quorum, or nil if unknown. Implementations
	// must return a fresh slice each call. An empty/nil result is treated
	// as "no update" by the proxy (it keeps its current members rather
	// than going dark).
	KeeperPoolMembers(shard string) []string

	// KeeperShardsList returns the names of every shard the Registry
	// knows about. Used so build.go can enumerate which shards to feed
	// live membership for without hard-coding shard names.
	KeeperShardsList() []string
}

// MeshTopology is an OPTIONAL Registry capability exposing the
// interserver-mesh Ingress address for a peer replica (e.g. "ch-1" →
// "ch-1.mesh.example.com:19009"). Used by pkg/interserver's Egress to
// route a part fetch — the source replica is parsed from the HTTP
// endpoint URL, then resolved to its peer Ingress via this lookup.
// Implementations that don't run the interserver-mesh sidecar simply
// omit it.
type MeshTopology interface {
	// MeshIngressFor returns the Ingress address (host:port) for the
	// given replica name, or ("", false) if unknown.
	MeshIngressFor(replica string) (string, bool)
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
