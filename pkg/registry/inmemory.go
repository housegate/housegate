package registry

import (
	"fmt"
	"sync"
)

// InMemory is a test/dev implementation of Topology, Databases, and
// Access backed by plain maps. Exported fields let tests assemble state
// directly; the type guards them with an RWMutex so concurrent reads
// are safe.
type InMemory struct {
	mu sync.RWMutex

	Indexers      map[uint64]ProxyAddress
	DatabaseInfos map[string]Database
	Permissions   map[string]map[string]DbAuth // account → database → bitmap
	Operators     map[string]map[string]bool   // owner → signer → granted
}

// NewInMemory returns an InMemory with all maps initialised.
func NewInMemory() *InMemory {
	return &InMemory{
		Indexers:      make(map[uint64]ProxyAddress),
		DatabaseInfos: make(map[string]Database),
		Permissions:   make(map[string]map[string]DbAuth),
		Operators:     make(map[string]map[string]bool),
	}
}

// --- Topology

func (m *InMemory) ProxyByIndexerId(indexerId uint64) (ProxyAddress, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.Indexers[indexerId]
	return p, ok
}

func (m *InMemory) AllIndexers() map[uint64]ProxyAddress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[uint64]ProxyAddress, len(m.Indexers))
	for k, v := range m.Indexers {
		out[k] = v
	}
	return out
}

// --- Databases

func (m *InMemory) Get(id string) (Database, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.DatabaseInfos[id]
	return d, ok
}

func (m *InMemory) All() map[string]Database {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]Database, len(m.DatabaseInfos))
	for k, v := range m.DatabaseInfos {
		out[k] = v
	}
	return out
}

// --- Access

func (m *InMemory) PermissionsFor(account string) (map[string]DbAuth, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	perms, ok := m.Permissions[account]
	if !ok {
		return nil, false
	}
	out := make(map[string]DbAuth, len(perms))
	for k, v := range perms {
		out[k] = v
	}
	return out, true
}

func (m *InMemory) HasPermission(account, database string, action Action) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.DatabaseInfos[database]; !ok {
		return false, fmt.Errorf("database not found: %s", database)
	}
	auth := DbAuth(0)
	if perms, ok := m.Permissions[account]; ok {
		auth |= perms[database]
	}
	return expandAuth(auth)&DbAuth(action) != 0, nil
}

func (m *InMemory) IsOperator(owner, signer string) bool {
	if owner == "" || signer == "" {
		return false
	}
	if owner == signer {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	ops, ok := m.Operators[owner]
	if !ok {
		return false
	}
	return ops[signer]
}

// expandAuth applies the standard hierarchy: Owner ⇒ Admin|Write|Read,
// Write ⇒ Read. Admin alone does NOT imply Write or Read.
func expandAuth(auth DbAuth) DbAuth {
	if auth&DbAuthOwner != 0 {
		auth |= DbAuthAdmin | DbAuthWrite | DbAuthRead
	}
	if auth&DbAuthWrite != 0 {
		auth |= DbAuthRead
	}
	return auth
}

// Compile-time checks that InMemory satisfies all three interfaces.
var (
	_ Topology  = (*InMemory)(nil)
	_ Databases = (*InMemory)(nil)
	_ Access    = (*InMemory)(nil)
)
