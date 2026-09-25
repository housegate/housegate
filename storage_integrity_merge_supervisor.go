package housegate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/sitable"
)

var errStorageIntegrityMergeGuardNotAsserted = errors.New("merge guard not asserted")

// StorageIntegrityMergeHealth answers the merge-guard health of one logical
// table (spec 2026-09-24 §9.4); ingress admission checks only its target.
type StorageIntegrityMergeHealth interface {
	CheckMergeHealth(tableID string) error
}

// StorageIntegrityMergeSupervisor reasserts the idempotent merge guard over
// the Active tables of the current table-state snapshot, on an interval and
// whenever the table state changes, and keeps one fail-closed health latch
// per table.
type StorageIntegrityMergeSupervisor struct {
	guard    StorageIntegrityMergeGuard
	state    sitable.TableState
	interval time.Duration

	assertMu sync.Mutex
	healthMu sync.RWMutex
	health   map[string]error
	version  uint64 // snapshot version of the last pass
	asserted bool
}

func NewStorageIntegrityMergeSupervisor(guard StorageIntegrityMergeGuard, state sitable.TableState, interval time.Duration) *StorageIntegrityMergeSupervisor {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &StorageIntegrityMergeSupervisor{
		guard:    guard,
		state:    state,
		interval: interval,
		health:   map[string]error{},
	}
}

// Assert runs one pass over the current snapshot's Active tables, replaces
// every latch, and returns the joined failures of the Active tables (nil when
// all are healthy).
func (s *StorageIntegrityMergeSupervisor) Assert(ctx context.Context) error {
	if s == nil || s.guard == nil || s.state == nil {
		return errors.New("storage_integrity: merge guard and table state are required")
	}
	s.assertMu.Lock()
	defer s.assertMu.Unlock()
	snap := s.state.Current()
	active := snap.Active()
	ids := make([]string, 0, len(active))
	for _, table := range active {
		ids = append(ids, table.ID)
	}
	report, err := s.guard.AssertTables(ctx, ids)
	health := make(map[string]error, len(ids))
	var failures []error
	for _, id := range ids {
		tableErr := err
		if tableErr == nil {
			var ok bool
			if tableErr, ok = report.Tables[id]; !ok {
				tableErr = errStorageIntegrityMergeGuardNotAsserted
			}
		}
		health[id] = tableErr
		if tableErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", id, tableErr))
		}
	}
	for table, tableErr := range report.Other {
		if tableErr != nil {
			log.Warnw("storage_integrity: merge guard found an unhealthy non-Active table", "database", table.Database, "table", table.Table, "error", tableErr)
		}
	}
	s.healthMu.Lock()
	s.health = health
	s.version = snap.Version()
	s.asserted = true
	s.healthMu.Unlock()
	return errors.Join(failures...)
}

// CheckMergeHealth fails closed for a table no pass has asserted yet, such as
// a table that became Active after the last pass.
func (s *StorageIntegrityMergeSupervisor) CheckMergeHealth(tableID string) error {
	if s == nil {
		return errors.New("storage_integrity: merge supervisor is required")
	}
	s.healthMu.RLock()
	err, ok := s.health[tableID]
	s.healthMu.RUnlock()
	if !ok {
		err = errStorageIntegrityMergeGuardNotAsserted
	}
	if err != nil {
		return fmt.Errorf("storage_integrity: merge guard unhealthy for %s: %w", tableID, err)
	}
	return nil
}

// Unhealthy returns the sorted ids whose latch is closed (diagnostics).
func (s *StorageIntegrityMergeSupervisor) Unhealthy() []string {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	var out []string
	for id, err := range s.health {
		if err != nil {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Run reasserts every interval and immediately after every table-state
// change, so a newly Active table is asserted without waiting for the tick.
// It subscribes before comparing versions, so a change that lands between a
// pass and the next subscription is never missed. It returns when ctx is
// done, including before a pending pass: reassert skips a canceled pass, so
// the version check alone would otherwise loop without waiting.
func (s *StorageIntegrityMergeSupervisor) Run(ctx context.Context) {
	if s == nil || s.guard == nil || s.state == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		changed := s.state.Changed()
		if !s.assertedVersion(s.state.Current().Version()) {
			s.reassert(ctx)
			continue
		}
		select {
		case <-ticker.C:
		case <-changed:
		case <-ctx.Done():
			return
		}
		s.reassert(ctx)
	}
}

func (s *StorageIntegrityMergeSupervisor) assertedVersion(version uint64) bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.asserted && s.version == version
}

func (s *StorageIntegrityMergeSupervisor) reassert(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if err := s.Assert(ctx); err != nil && ctx.Err() == nil {
		log.Warnw("storage_integrity: merge guard reassert found unhealthy tables", "error", err)
	}
}

var _ StorageIntegrityMergeHealth = (*StorageIntegrityMergeSupervisor)(nil)
