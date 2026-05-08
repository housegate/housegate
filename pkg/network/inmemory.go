package network

import (
	"fmt"
	"sync"

	"sentioxyz/sentio-core/network/registry"
)

// InMemoryNetworkState is a test-friendly State backed by plain
// maps. Exported fields let tests assemble state directly; the type
// still honours the interface's read methods.
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
// non-nil maps.
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

func (s *InMemoryNetworkState) RetrieveProcessorAllocation(processorId ProcessorId) ([]ProcessorAllocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocs, ok := s.ProcessorAllocations[processorId]
	return allocs, ok
}

func (s *InMemoryNetworkState) RetrieveIndexerInfo(indexerId IndexId) (IndexerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.IndexerInfos[indexerId]
	return info, ok
}

func (s *InMemoryNetworkState) RetrieveProcessorInfo(processorId ProcessorId) (ProcessorInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.ProcessorInfos[processorId]
	return info, ok
}

func (s *InMemoryNetworkState) RetrieveAllIndexerInfos() map[uint64]IndexerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[uint64]IndexerInfo, len(s.IndexerInfos))
	for k, v := range s.IndexerInfos {
		cp[k] = v
	}
	return cp
}

func (s *InMemoryNetworkState) RetrieveDatabaseInfo(database Database) (DatabaseInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.DatabaseInfos[database]
	return info, ok
}

func (s *InMemoryNetworkState) RetrieveAllDatabaseInfos() map[Database]DatabaseInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[Database]DatabaseInfo, len(s.DatabaseInfos))
	for k, v := range s.DatabaseInfos {
		cp[k] = v
	}
	return cp
}

func (s *InMemoryNetworkState) RetrieveDatabasePermissions(account AccountAddress) (DatabasePermissions, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	perms, ok := s.DatabasePermissions[account]
	if !ok {
		return nil, false
	}
	cp := make(DatabasePermissions, len(perms))
	for k, v := range perms {
		cp[k] = v
	}
	return cp, true
}

// AccountHasPermissionForDatabase checks whether `account` has the
// permission bit `action` on `database`. The Owner bit is expected to
// be stored explicitly in DatabasePermissions — no implicit promotion
// from DatabaseInfo.Owner. An unknown database is an error.
func (s *InMemoryNetworkState) AccountHasPermissionForDatabase(account AccountAddress, database Database, action registry.Action) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.DatabaseInfos[database]
	if !ok {
		return false, fmt.Errorf("database not found: %s", database)
	}

	auth := registry.DbAuth(0)
	if perms, ok := s.DatabasePermissions[account]; ok {
		auth |= perms[database]
	}
	return auth&registry.DbAuth(action) != 0, nil
}

func (s *InMemoryNetworkState) IsOperator(owner, signer AccountAddress) bool {
	if owner == "" || signer == "" {
		return false
	}
	if owner == signer {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ops, ok := s.Operators[owner]
	if !ok {
		return false
	}
	return ops[signer]
}

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

func (s *InMemoryNetworkState) Type() StateType {
	return StateTypeInMemory
}

// Compile-time check the test fake satisfies the production interface.
var _ State = (*InMemoryNetworkState)(nil)
