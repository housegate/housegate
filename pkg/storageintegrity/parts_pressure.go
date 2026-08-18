package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSoftPartsPerPartition = 2400
	DefaultHardPartsPerPartition = 2950
	DefaultPartsRefreshTimeout   = 2 * time.Second
	DefaultPartsSnapshotTTL      = 6 * time.Second
)

// ErrBackpressure is the sentinel every part-pressure refusal unwraps to.
var ErrBackpressure = errors.New("storage_integrity: back-pressure")

// BackpressureError describes the refused physical table and partition.
type BackpressureError struct {
	Database  string
	Table     string
	Partition string
	Parts     int
	Limit     int
	Kind      string
}

func (e *BackpressureError) Error() string {
	if e.Kind == "unavailable" {
		return fmt.Sprintf("storage_integrity: back-pressure: part inventory unavailable for %s.%s; retry later", e.Database, e.Table)
	}
	return fmt.Sprintf("storage_integrity: back-pressure: %s.%s partition %s has %d active parts (%s limit %d); retry later",
		e.Database, e.Table, e.Partition, e.Parts, e.Kind, e.Limit)
}

func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// LogicalPartitionID maps system.parts.partition text to the row-side
// p_<value> / all convention. Whether the table is unpartitioned comes from
// system.tables.partition_key; the partition text alone is ambiguous because
// a partitioned String column may contain the literal value "tuple()".
func LogicalPartitionID(partitionText string, unpartitioned bool) string {
	if unpartitioned {
		return "all"
	}
	return "p_" + partitionText
}

type PartsKey struct {
	Database  string
	Table     string
	Partition string
}

type PartsSnapshot map[PartsKey]int

type PartsPressureConfig struct {
	UnsafeDatabase        string
	SafeDatabase          string
	SoftPartsPerPartition int
	HardPartsPerPartition int
	RefreshTimeout        time.Duration
	SnapshotTTL           time.Duration
}

// PartsReservation holds one prospective part per touched partition. Callers
// must finish it exactly once: Release before a source write, or Commit once a
// source write may have happened. Committed counts remain admission-visible
// until a later successful snapshot absorbs them.
type PartsReservation interface {
	Commit()
	Release()
}

// PartsPressureGuard caches active-part counts and answers ingress admission
// from the last successful snapshot.
type PartsPressureGuard struct {
	conn MergeConn
	cfg  PartsPressureConfig

	refreshMu   sync.Mutex
	admissionMu sync.Mutex
	commitMu    sync.Mutex
	mu          sync.RWMutex
	snapshot    PartsSnapshot
	reserved    PartsSnapshot
	committed   PartsSnapshot
	// committedBaselines records the last active-part count against which each
	// committed reservation was made. A successful metadata query may absorb a
	// committed slot only after its count growth covers that baseline; query
	// success alone is not visibility proof.
	committedBaselines map[PartsKey][]int
	takenAt            time.Time
	haveSnap           bool
	now                func() time.Time

	invalidated chan struct{}
}

func NewPartsPressureGuard(conn MergeConn, cfg PartsPressureConfig) *PartsPressureGuard {
	if cfg.SoftPartsPerPartition <= 0 {
		cfg.SoftPartsPerPartition = DefaultSoftPartsPerPartition
	}
	if cfg.HardPartsPerPartition <= 0 {
		cfg.HardPartsPerPartition = DefaultHardPartsPerPartition
	}
	if cfg.RefreshTimeout <= 0 {
		cfg.RefreshTimeout = DefaultPartsRefreshTimeout
	}
	if cfg.SnapshotTTL <= 0 {
		cfg.SnapshotTTL = DefaultPartsSnapshotTTL
	}
	return &PartsPressureGuard{
		conn:               conn,
		cfg:                cfg,
		reserved:           PartsSnapshot{},
		committed:          PartsSnapshot{},
		committedBaselines: map[PartsKey][]int{},
		now:                time.Now,
		invalidated:        make(chan struct{}, 1),
	}
}

// BuildSnapshotQuery groups active parts by partition text, not partition_id,
// and joins the table's partition key so an unpartitioned table can be
// distinguished from a partitioned String value whose bytes are "tuple()".
func (g *PartsPressureGuard) BuildSnapshotQuery() string {
	databases := []string{quoteMergeString(g.cfg.UnsafeDatabase)}
	if g.cfg.SafeDatabase != "" {
		databases = append(databases, quoteMergeString(g.cfg.SafeDatabase))
	}
	return "SELECT parts.database, parts.table, parts.partition, tables.partition_key, count() " +
		"FROM system.parts AS parts INNER JOIN system.tables AS tables " +
		"ON parts.database = tables.database AND parts.table = tables.name " +
		"WHERE parts.database IN (" + strings.Join(databases, ", ") + ") AND parts.active " +
		"GROUP BY parts.database, parts.table, parts.partition, tables.partition_key"
}

// Refresh replaces the cached snapshot only after a complete successful read.
func (g *PartsPressureGuard) Refresh(ctx context.Context) (PartsSnapshot, error) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	// Freeze Commit while the query is in flight. A commit completed after this
	// snapshot will use the new snapshot count as its baseline and cannot consume
	// visibility growth already credited here.
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	refreshCtx, cancel := context.WithTimeout(ctx, g.cfg.RefreshTimeout)
	defer cancel()
	rows, err := g.conn.Query(refreshCtx, g.BuildSnapshotQuery())
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: parts snapshot query failed: %w", err)
	}
	defer rows.Close()
	snapshot := PartsSnapshot{}
	for rows.Next() {
		var database, table, partition, partitionKey string
		var number uint64
		if err := rows.Scan(&database, &table, &partition, &partitionKey, &number); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts snapshot: %w", err)
		}
		snapshot[PartsKey{Database: database, Table: table, Partition: LogicalPartitionID(partition, partitionKey == "")}] = int(number)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts snapshot: %w", err)
	}
	g.mu.Lock()
	g.snapshot = snapshot
	g.takenAt = g.now()
	g.haveSnap = true
	for key, baselines := range g.committedBaselines {
		remaining := committedAfterSnapshot(baselines, snapshot[key])
		if len(remaining) == 0 {
			delete(g.committed, key)
			delete(g.committedBaselines, key)
		} else {
			g.committed[key] = len(remaining)
			g.committedBaselines[key] = remaining
		}
	}
	g.mu.Unlock()
	return g.copySnapshot(), nil
}

func (g *PartsPressureGuard) Snapshot() (PartsSnapshot, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return nil, false
	}
	return copyPartsSnapshot(g.snapshot), true
}

func (g *PartsPressureGuard) copySnapshot() PartsSnapshot {
	snapshot, _ := g.Snapshot()
	return snapshot
}

// Allow is a point-in-time check for diagnostics. Admission paths must use
// Reserve so concurrent sessions cannot all pass against the same snapshot.
func (g *PartsPressureGuard) Allow(table, partitionID string) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.checkLocked(table, partitionID)
}

// Reserve synchronously refreshes the source inventory, then atomically checks
// and reserves one prospective active part in every touched partition. The
// refresh and reservation are serialized across callers so concurrent sessions
// cannot all pass against the same soft-limit slot. Either all partitions are
// reserved or none are.
func (g *PartsPressureGuard) Reserve(ctx context.Context, table string, partitionIDs []string) (PartsReservation, error) {
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	if _, err := g.Refresh(ctx); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		unavailable := &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Kind: "unavailable"}
		return nil, fmt.Errorf("%w: refresh parts snapshot: %w", unavailable, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	keys := make([]PartsKey, 0, len(partitionIDs))
	baselines := make([]int, 0, len(partitionIDs))
	seen := make(map[string]bool, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		if seen[partitionID] {
			continue
		}
		seen[partitionID] = true
		if err := g.checkLocked(table, partitionID); err != nil {
			return nil, err
		}
		key := PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}
		keys = append(keys, key)
		baselines = append(baselines, g.snapshot[key])
	}
	for _, key := range keys {
		g.reserved[key]++
	}
	return &partsReservation{guard: g, keys: keys, baselines: baselines}, nil
}

func (g *PartsPressureGuard) checkLocked(table, partitionID string) error {
	if !g.haveSnap || g.now().Sub(g.takenAt) > g.cfg.SnapshotTTL {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Kind: "unavailable"}
	}
	key := PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}
	number := g.snapshot[key] + g.reserved[key] + g.committed[key]
	switch {
	case number >= g.cfg.HardPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: number, Limit: g.cfg.HardPartsPerPartition, Kind: "hard"}
	case number >= g.cfg.SoftPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: number, Limit: g.cfg.SoftPartsPerPartition, Kind: "soft"}
	default:
		return nil
	}
}

type partsReservation struct {
	guard     *PartsPressureGuard
	keys      []PartsKey
	baselines []int
	once      sync.Once
}

func (r *partsReservation) Commit() {
	if r == nil || r.guard == nil {
		return
	}
	r.once.Do(func() {
		r.guard.commitMu.Lock()
		defer r.guard.commitMu.Unlock()
		r.guard.mu.Lock()
		defer r.guard.mu.Unlock()
		for idx, key := range r.keys {
			decrementPartCount(r.guard.reserved, key)
			baseline := r.baselines[idx]
			// A refresh may have completed while the source write was in flight.
			// Do not let this late Commit reuse growth already represented by that
			// snapshot; keeping the slot is conservative until later visibility.
			if current := r.guard.snapshot[key]; current > baseline {
				baseline = current
			}
			r.guard.committedBaselines[key] = append(r.guard.committedBaselines[key], baseline)
			r.guard.committed[key]++
		}
	})
}

func (r *partsReservation) Release() {
	if r == nil || r.guard == nil {
		return
	}
	r.once.Do(func() {
		r.guard.mu.Lock()
		defer r.guard.mu.Unlock()
		for _, key := range r.keys {
			decrementPartCount(r.guard.reserved, key)
		}
	})
}

func decrementPartCount(counts PartsSnapshot, key PartsKey) {
	if left := counts[key] - 1; left > 0 {
		counts[key] = left
	} else {
		delete(counts, key)
	}
}

// committedAfterSnapshot removes only reservations whose distinct count growth
// is visible in observed. Remaining baselines are advanced past growth already
// credited during this refresh so a later identical snapshot cannot absorb the
// same increment twice.
func committedAfterSnapshot(baselines []int, observed int) []int {
	ordered := append([]int(nil), baselines...)
	sort.Ints(ordered)
	covered := 0
	nextVisible := 0
	for idx, baseline := range ordered {
		if idx == 0 || baseline+1 > nextVisible {
			nextVisible = baseline + 1
		}
		if nextVisible > observed {
			break
		}
		covered++
		nextVisible++
	}
	remaining := append([]int(nil), ordered[covered:]...)
	for idx, baseline := range remaining {
		if baseline < observed {
			remaining[idx] = observed
		}
	}
	return remaining
}

func copyPartsSnapshot(in PartsSnapshot) PartsSnapshot {
	out := make(PartsSnapshot, len(in))
	for key, number := range in {
		out[key] = number
	}
	return out
}

func (g *PartsPressureGuard) Invalidate() {
	select {
	case g.invalidated <- struct{}{}:
	default:
	}
}

func (g *PartsPressureGuard) Invalidated() <-chan struct{} { return g.invalidated }
