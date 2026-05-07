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
	// DatabaseInfos is keyed by Database (a string alias from
	// sentio-core's registry package).
	DatabaseInfos map[Database]DatabaseInfo
	// DatabasePermissions maps an account address to the permission
	// bitmap it holds against each database.
	DatabasePermissions map[AccountAddress]DatabasePermissions
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

func (s *InMemoryNetworkState) Type() StateType {
	return StateTypeInMemory
}

// Compile-time check the test fake satisfies the production interface.
var _ State = (*InMemoryNetworkState)(nil)
