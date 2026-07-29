package housegate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var errStorageIntegrityMergeGuardNotAsserted = errors.New("merge guard not asserted")

type StorageIntegrityMergeHealth interface {
	CheckMergeHealth() error
}

// StorageIntegrityMergeSupervisor continuously reasserts the idempotent merge
// guard and exposes a fail-closed health latch to ingress.
type StorageIntegrityMergeSupervisor struct {
	guard    StorageIntegrityMergeGuard
	interval time.Duration

	assertMu sync.Mutex
	healthMu sync.RWMutex
	health   error
}

func NewStorageIntegrityMergeSupervisor(guard StorageIntegrityMergeGuard, interval time.Duration) *StorageIntegrityMergeSupervisor {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &StorageIntegrityMergeSupervisor{
		guard:    guard,
		interval: interval,
		health:   errStorageIntegrityMergeGuardNotAsserted,
	}
}

func (s *StorageIntegrityMergeSupervisor) AssertStopMerges(ctx context.Context) error {
	if s == nil || s.guard == nil {
		return errors.New("storage_integrity: merge guard is required")
	}
	s.assertMu.Lock()
	err := s.guard.AssertStopMerges(ctx)
	s.assertMu.Unlock()

	s.healthMu.Lock()
	s.health = err
	s.healthMu.Unlock()
	return err
}

func (s *StorageIntegrityMergeSupervisor) CheckMergeHealth() error {
	if s == nil {
		return errors.New("storage_integrity: merge supervisor is required")
	}
	s.healthMu.RLock()
	err := s.health
	s.healthMu.RUnlock()
	if err != nil {
		return fmt.Errorf("storage_integrity: merge guard unhealthy: %w", err)
	}
	return nil
}

func (s *StorageIntegrityMergeSupervisor) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = s.AssertStopMerges(ctx)
		case <-ctx.Done():
			return
		}
	}
}
