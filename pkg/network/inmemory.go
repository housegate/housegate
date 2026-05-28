package network

import (
	"fmt"
	"sort"
	"sync"

	"housegate/housegate/pkg/registry"
)

// InMemoryNetworkState is the in-memory implementation of
// registry.Registry used by YAML fixtures and tests. The
// commitgate observers in this package mutate the exported fields
// directly under InMemoryNetworkState.mu when DDL/DCL events fire;
// the public read methods translate the producer-shaped fat
// records (IndexerInfo, DatabaseInfo, ...) into the slim
// registry.Registry vocabulary at the boundary.
type InMemoryNetworkState struct {
	mu                   sync.RWMutex
	ProcessorAllocations map[string][]ProcessorAllocation
	IndexerInfos         map[uint64]IndexerInfo
	ProcessorInfos       map[string]ProcessorInfo
	DatabaseInfos        map[Database]DatabaseInfo
	DatabasePermissions  map[AccountAddress]DatabasePermissions
	Operators            map[AccountAddress]map[AccountAddress]bool

	// KeeperShards maps a shard name to the keeper-client endpoints
	// (host:port) of that shard's current quorum (architecture.md §6).
	// Satisfies the optional registry.KeeperPool capability so the
	// keeper proxy can track live membership in tests and local/yaml
	// deployments. Empty / missing shard means "unknown" (no update).
	KeeperShards map[string][]string

	// MeshIngressByReplica maps a CH replica name (as it appears in
	// /clickhouse/tables/.../replicas/<replica>/) to its peer Ingress
	// address (host:port). Satisfies the optional registry.MeshTopology
	// capability so the interserver-mesh Egress can route a part fetch to
	// the right peer's mTLS Ingress.
	MeshIngressByReplica map[string]string
}

// NewInMemoryNetworkState returns an empty InMemoryNetworkState with
// all maps initialised.
func NewInMemoryNetworkState() *InMemoryNetworkState {
	return &InMemoryNetworkState{
		ProcessorAllocations: make(map[string][]ProcessorAllocation),
		IndexerInfos:         make(map[uint64]IndexerInfo),
		ProcessorInfos:       make(map[string]ProcessorInfo),
		DatabaseInfos:        make(map[Database]DatabaseInfo),
		DatabasePermissions:  make(map[AccountAddress]DatabasePermissions),
		Operators:            make(map[AccountAddress]map[AccountAddress]bool),
		KeeperShards:         make(map[string][]string),
	}
}

// --- registry.KeeperPool (optional)

// KeeperPoolMembers returns a copy of the named shard's keeper quorum
// endpoints, or nil when unknown. Satisfies registry.KeeperPool.
func (s *InMemoryNetworkState) KeeperPoolMembers(shard string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.KeeperShards[shard]
	if len(m) == 0 {
		return nil
	}
	return append([]string(nil), m...)
}

// KeeperShardsList returns the names of every registered shard. Satisfies
// registry.KeeperPool.KeeperShards. (Named *List to avoid a clash with
// the exported KeeperShards map field.)
func (s *InMemoryNetworkState) KeeperShardsList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.KeeperShards))
	for name := range s.KeeperShards {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// SetKeeperPool replaces the named shard's keeper quorum membership.
// Test/local helper that mirrors how a chain-backed Registry would update
// on the on-chain keeper-pool-changed event.
func (s *InMemoryNetworkState) SetKeeperPool(shard string, members ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.KeeperShards == nil {
		s.KeeperShards = make(map[string][]string)
	}
	s.KeeperShards[shard] = append([]string(nil), members...)
}

// --- registry.MeshTopology (optional)

// MeshIngressFor returns the interserver-mesh Ingress address for a peer
// replica, or ("", false) when unknown. Satisfies registry.MeshTopology.
func (s *InMemoryNetworkState) MeshIngressFor(replica string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addr, ok := s.MeshIngressByReplica[replica]
	return addr, ok
}

// SetMeshIngress registers (or updates) a peer replica's mesh Ingress
// address. Test/local helper.
func (s *InMemoryNetworkState) SetMeshIngress(replica, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MeshIngressByReplica == nil {
		s.MeshIngressByReplica = map[string]string{}
	}
	s.MeshIngressByReplica[replica] = addr
}

// --- registry.Topology

func (s *InMemoryNetworkState) ProxyByIndexerId(indexerId uint64) (registry.ProxyAddress, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.IndexerInfos[indexerId]
	if !ok {
		return registry.ProxyAddress{}, false
	}
	return registry.ProxyAddress{
		Url:           info.IndexerUrl,
		HousegatePort: info.ClickhouseProxyPort,
	}, true
}

func (s *InMemoryNetworkState) AllIndexers() map[uint64]registry.ProxyAddress {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[uint64]registry.ProxyAddress, len(s.IndexerInfos))
	for id, info := range s.IndexerInfos {
		out[id] = registry.ProxyAddress{
			Url:           info.IndexerUrl,
			HousegatePort: info.ClickhouseProxyPort,
		}
	}
	return out
}

// --- registry.Databases

func (s *InMemoryNetworkState) Get(id string) (registry.Database, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.DatabaseInfos[Database(id)]
	if !ok {
		return registry.Database{}, false
	}
	return convertDatabase(info), true
}

func (s *InMemoryNetworkState) All() map[string]registry.Database {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]registry.Database, len(s.DatabaseInfos))
	for db, info := range s.DatabaseInfos {
		out[string(db)] = convertDatabase(info)
	}
	return out
}

// --- registry.Access

// PermissionsFor returns the per-database permission bitmap for an
// account. The map merges the account's explicit grants with any
// wildcard grants held by WildcardAddress; callers should treat the
// returned map as opaque and not modify it.
func (s *InMemoryNetworkState) PermissionsFor(account string) (map[string]registry.DbAuth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addr := AccountAddress(account)
	perms, ok := s.DatabasePermissions[addr]
	wildcard, wildcardOk := s.DatabasePermissions[WildcardAddress]
	if !ok && !wildcardOk {
		return nil, false
	}
	out := make(map[string]registry.DbAuth, len(perms)+len(wildcard))
	if ok {
		for db, auth := range perms {
			out[string(db)] = auth
		}
	}
	if wildcardOk && addr != WildcardAddress {
		for db, auth := range wildcard {
			out[string(db)] |= auth
		}
	}
	return out, true
}

func (s *InMemoryNetworkState) HasPermission(account, database string, action registry.Action) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.DatabaseInfos[Database(database)]; !ok {
		return false, fmt.Errorf("database not found: %s", database)
	}
	auth := registry.DbAuth(0)
	if perms, ok := s.DatabasePermissions[AccountAddress(account)]; ok {
		auth |= perms[Database(database)]
	}
	if AccountAddress(account) != WildcardAddress {
		if perms, ok := s.DatabasePermissions[WildcardAddress]; ok {
			auth |= perms[Database(database)]
		}
	}
	return promotePermissionBits(auth)&registry.DbAuth(action) != 0, nil
}

func (s *InMemoryNetworkState) IsOperator(owner, signer string) bool {
	if owner == "" || signer == "" {
		return false
	}
	if owner == signer {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ops, ok := s.Operators[AccountAddress(owner)]
	if !ok {
		return false
	}
	return ops[AccountAddress(signer)]
}

// SetOperator is a test/dev helper: mark signer as an authorised
// operator for owner (allowed=true) or revoke (allowed=false).
func (s *InMemoryNetworkState) SetOperator(owner, signer AccountAddress, allowed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !allowed {
		if ops, ok := s.Operators[owner]; ok {
			delete(ops, signer)
			if len(ops) == 0 {
				delete(s.Operators, owner)
			}
		}
		return
	}
	ops, ok := s.Operators[owner]
	if !ok {
		ops = map[AccountAddress]bool{}
		s.Operators[owner] = ops
	}
	ops[signer] = true
}

// convertDatabase translates the producer-shaped DatabaseInfo into
// the slim registry.Database that the proxy chain consumes.
func convertDatabase(info DatabaseInfo) registry.Database {
	var tables []registry.Table
	if len(info.Tables) > 0 {
		tables = make([]registry.Table, len(info.Tables))
		for i, t := range info.Tables {
			tables[i] = registry.Table{Id: t.TableId}
		}
	}
	return registry.Database{
		IndexerId:     info.IndexerId,
		PendingDelete: info.PendingDelete,
		Tables:        tables,
	}
}

// Compile-time check that InMemoryNetworkState satisfies the
// consumer-side contract housegate's proxy chain depends on.
var _ registry.Registry = (*InMemoryNetworkState)(nil)
