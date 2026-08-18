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
// Commit once a source write may have happened. Release cancels the reservation
// before or after Commit when a later source lookup / exact cleanup proves no
// part remains. Finalize makes a committed reservation non-cancelable after
// ACK2; its capacity remains charged until a snapshot covers the whole cohort.
type PartsReservation interface {
	Commit()
	Release()
	Finalize()
}

type committedPartReservation struct {
	reservationID      uint64
	baseline           int
	reservedGeneration uint64
	final              bool
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
	// committedReservations retain reservation identity across visibility so a
	// later no-write lookup or exact cleanup can cancel the right slot. Snapshot
	// generations and original counts prove only aggregate cohort coverage; they
	// never attribute an ambiguous count increase to a particular statement.
	committedReservations map[PartsKey][]committedPartReservation
	nextReservationID     uint64
	snapshotGeneration    uint64
	takenAt               time.Time
	haveSnap              bool
	now                   func() time.Time

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
		conn:                  conn,
		cfg:                   cfg,
		reserved:              PartsSnapshot{},
		committed:             PartsSnapshot{},
		committedReservations: map[PartsKey][]committedPartReservation{},
		now:                   time.Now,
		invalidated:           make(chan struct{}, 1),
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
	g.snapshotGeneration++
	g.takenAt = g.now()
	g.haveSnap = true
	for key := range g.committedReservations {
		g.reconcileCommittedKeyLocked(key)
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
	generation := g.snapshotGeneration
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
	g.nextReservationID++
	return &partsReservation{
		guard:              g,
		id:                 g.nextReservationID,
		keys:               keys,
		baselines:          baselines,
		reservedGeneration: generation,
		state:              reservationReserved,
	}, nil
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
	guard              *PartsPressureGuard
	id                 uint64
	keys               []PartsKey
	baselines          []int
	reservedGeneration uint64
	state              reservationState // protected by guard.mu
}

type reservationState uint8

const (
	reservationReserved reservationState = iota + 1
	reservationCommitted
	reservationFinalized
	reservationReleased
)

func (r *partsReservation) Commit() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	r.commitLocked()
}

func (r *partsReservation) Release() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	switch r.state {
	case reservationReserved:
		for _, key := range r.keys {
			decrementPartCount(r.guard.reserved, key)
		}
	case reservationCommitted:
		for _, key := range r.keys {
			r.guard.removeCommittedReservationLocked(key, r.id)
			r.guard.reconcileCommittedKeyLocked(key)
		}
	case reservationFinalized, reservationReleased:
		return
	}
	r.state = reservationReleased
}

func (r *partsReservation) Finalize() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	if r.state == reservationReserved {
		r.commitLocked()
	}
	if r.state != reservationCommitted {
		return
	}
	for _, key := range r.keys {
		entries := r.guard.committedReservations[key]
		for idx := range entries {
			if entries[idx].reservationID == r.id {
				entries[idx].final = true
			}
		}
		r.guard.committedReservations[key] = entries
		r.guard.reconcileCommittedKeyLocked(key)
	}
	r.state = reservationFinalized
}

func (r *partsReservation) commitLocked() {
	if r.state != reservationReserved {
		return
	}
	for idx, key := range r.keys {
		decrementPartCount(r.guard.reserved, key)
		r.guard.committedReservations[key] = append(r.guard.committedReservations[key], committedPartReservation{
			reservationID:      r.id,
			baseline:           r.baselines[idx],
			reservedGeneration: r.reservedGeneration,
		})
		r.guard.reconcileCommittedKeyLocked(key)
	}
	r.state = reservationCommitted
}

func decrementPartCount(counts PartsSnapshot, key PartsKey) {
	if left := counts[key] - 1; left > 0 {
		counts[key] = left
	} else {
		delete(counts, key)
	}
}

func (g *PartsPressureGuard) removeCommittedReservationLocked(key PartsKey, reservationID uint64) {
	entries := g.committedReservations[key]
	kept := entries[:0]
	for _, entry := range entries {
		if entry.reservationID != reservationID {
			kept = append(kept, entry)
		}
	}
	if len(kept) == 0 {
		delete(g.committedReservations, key)
		delete(g.committed, key)
		return
	}
	g.committedReservations[key] = kept
}

// reconcileCommittedKeyLocked computes aggregate visibility debt for the
// reservation cohort without claiming that any count increase belongs to a
// particular identity. Original reserve baselines survive a refresh that races
// ahead of Commit; removing a canceled identity recomputes the cohort proof.
func (g *PartsPressureGuard) reconcileCommittedKeyLocked(key PartsKey) {
	entries := g.committedReservations[key]
	if len(entries) == 0 {
		delete(g.committed, key)
		return
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].baseline != entries[j].baseline {
			return entries[i].baseline < entries[j].baseline
		}
		if entries[i].reservedGeneration != entries[j].reservedGeneration {
			return entries[i].reservedGeneration < entries[j].reservedGeneration
		}
		return entries[i].reservationID < entries[j].reservationID
	})
	observed := g.snapshot[key]
	nextVisible := 0
	covered := 0
	for idx := range entries {
		if idx == 0 || entries[idx].baseline+1 > nextVisible {
			nextVisible = entries[idx].baseline + 1
		}
		if nextVisible <= observed {
			covered++
			nextVisible++
		}
	}

	// Final identities become non-cancelable at ACK2. Compact them only when the
	// snapshot covers the entire cohort, so no ambiguous aggregate growth is ever
	// assigned to a specific statement merely to free its handle.
	if covered == len(entries) {
		kept := entries[:0]
		for _, entry := range entries {
			if !entry.final {
				kept = append(kept, entry)
			}
		}
		entries = kept
		if len(entries) == 0 {
			delete(g.committedReservations, key)
			delete(g.committed, key)
			return
		}
		g.committedReservations[key] = entries
		// Every retained (cancelable) identity was already covered as part of the
		// larger cohort; removing finalized members cannot invalidate that proof.
		covered = len(entries)
	}
	pending := len(entries) - covered
	if pending == 0 {
		delete(g.committed, key)
	} else {
		g.committed[key] = pending
	}
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
