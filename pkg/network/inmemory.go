package network

import (
	"fmt"
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
	}
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
