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

// ErrCleanupProofPending means the exact pre/post cleanup inventory proof has
// not completed. The statement reservation and source frontier must remain
// fenced until the same-ID proof retry succeeds.
var ErrCleanupProofPending = errors.New("storage_integrity: cleanup inventory proof pending")

// ErrIntakeJournalMigrationRequired means a legacy finalized candidate lacks
// the durable exact-observation fact needed to distinguish delayed visibility
// from historical cleanup. Startup must stop for an explicit operator/source
// proof instead of silently dropping or permanently charging the candidate.
var ErrIntakeJournalMigrationRequired = errors.New("storage_integrity: intake journal migration required")

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

// partsInventory is the exact active part-name set behind a count snapshot.
// Counts remain the admission input, while names provide proof that an exact
// cleanup removed a part rather than merely coinciding with unrelated growth.
type partsInventory map[PartsKey]map[string]struct{}

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
// before or after Commit when a later source lookup proves no write occurred.
// CommitIndeterminate is the no-result form: it keeps candidate-less debt from
// being absorbed by unrelated aggregate growth until lookup or exact binding.
// PrepareCleanupProof freezes exact candidates that are active immediately
// before source cleanup. ReleaseCleaned then proves those names absent and
// cancels the reservation. Finalize makes a committed reservation non-cancelable
// after ACK2; its capacity remains charged until a snapshot covers the cohort.
type PartsReservation interface {
	Commit(...CandidatePart) error
	CommitIndeterminate()
	Release()
	PrepareCleanupProof(context.Context, []CandidatePart) error
	ReleaseCleaned(context.Context) error
	Finalize()
}

// PartsRestoreRecord is one durable intake projected into the pressure
// lifecycle. ObservedCandidates are exact names that a previous successful
// local inventory proved active; if such a name is now absent, it is historical
// cleanup rather than delayed visibility and needs neither ownership nor debt.
type PartsRestoreRecord struct {
	StatementID        string
	Table              string
	PartitionIDs       []string
	Candidates         []CandidatePart
	ObservedCandidates []CandidatePart
	Finalized          bool
	LegacyObservation  bool
}

type reservationSlot struct {
	reservation *partsReservation
	keyIndex    int
}

type candidateClaim struct {
	reservationID uint64
	statementID   string
	candidate     CandidatePart
	// observedActive distinguishes an exact part that has existed in a
	// successful inventory from one whose source write is still subject to
	// ClickHouse visibility delay. A finalized claim is released only after an
	// inventory first observes the part and a later inventory proves it absent.
	observedActive    bool
	observedPersisted bool
}

type candidateObservation struct {
	statementID string
	candidate   CandidatePart
}

// PartsPressureGuard caches active-part counts plus exact names and answers
// ingress admission from the last successful snapshot.
type PartsPressureGuard struct {
	conn MergeConn
	cfg  PartsPressureConfig

	refreshMu   sync.Mutex
	admissionMu sync.Mutex
	commitMu    sync.Mutex
	mu          sync.RWMutex
	snapshot    PartsSnapshot
	activeParts partsInventory
	reserved    PartsSnapshot
	committed   PartsSnapshot
	// committedReservations retain reservation identity across visibility so a
	// later no-write lookup can cancel the right slot. Snapshot
	// generations and original counts prove only aggregate cohort coverage; they
	// never attribute an ambiguous count increase to a particular statement.
	committedReservations map[PartsKey][]reservationSlot
	// candidateClaims makes an exact ClickHouse part name proof single-owner.
	// Otherwise one malformed repeated candidate could cover multiple slots.
	candidateClaims map[PartsKey]map[string]candidateClaim
	pendingObserved map[string]candidateObservation
	observedHook    func(context.Context, string, CandidatePart) error
	// liveReservations retains cancelable and not-yet-visible finalized handles
	// so a serialized post-cleanup snapshot can rebase every later reservation.
	liveReservations   map[uint64]*partsReservation
	nextReservationID  uint64
	snapshotGeneration uint64
	takenAt            time.Time
	haveSnap           bool
	// restoreBlocked latches any incomplete durable-journal projection. Normal
	// polling may refresh inventory but cannot reopen admission; only a complete
	// successful RestoreBatch proves all durable reservations/claims installed.
	restoreBlocked bool
	now            func() time.Time

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
		activeParts:           partsInventory{},
		reserved:              PartsSnapshot{},
		committed:             PartsSnapshot{},
		committedReservations: map[PartsKey][]reservationSlot{},
		candidateClaims:       map[PartsKey]map[string]candidateClaim{},
		pendingObserved:       map[string]candidateObservation{},
		liveReservations:      map[uint64]*partsReservation{},
		now:                   time.Now,
		invalidated:           make(chan struct{}, 1),
	}
}

// SetCandidateObservedHook installs the durable acknowledgement used before a
// locally observed exact candidate may later be retired as historical cleanup.
// A hook failure makes the current inventory unavailable and is retried on the
// next Refresh.
func (g *PartsPressureGuard) SetCandidateObservedHook(hook func(context.Context, string, CandidatePart) error) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.observedHook = hook
	g.mu.Unlock()
}

// BuildSnapshotQuery reads every active part name together with partition text,
// not partition_id, and joins the table's partition key so an unpartitioned
// table can be distinguished from a partitioned String value whose bytes are
// "tuple()". Aggregation happens in Go because exact names are required for
// cleanup proof.
func (g *PartsPressureGuard) BuildSnapshotQuery() string {
	databases := []string{quoteMergeString(g.cfg.UnsafeDatabase)}
	if g.cfg.SafeDatabase != "" {
		databases = append(databases, quoteMergeString(g.cfg.SafeDatabase))
	}
	return "SELECT parts.database, parts.table, parts.partition, tables.partition_key, parts.name " +
		"FROM system.parts AS parts INNER JOIN system.tables AS tables " +
		"ON parts.database = tables.database AND parts.table = tables.name " +
		"WHERE parts.database IN (" + strings.Join(databases, ", ") + ") AND parts.active " +
		"ORDER BY parts.database, parts.table, parts.partition, parts.name"
}

// Refresh replaces the cached snapshot only after a complete successful read.
func (g *PartsPressureGuard) Refresh(ctx context.Context) (PartsSnapshot, error) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	// Freeze reservation lifecycle changes while the query is in flight so the
	// snapshot generation and every visibility barrier move atomically.
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
	inventory := partsInventory{}
	for rows.Next() {
		var database, table, partition, partitionKey, partName string
		if err := rows.Scan(&database, &table, &partition, &partitionKey, &partName); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts snapshot: %w", err)
		}
		key := PartsKey{Database: database, Table: table, Partition: LogicalPartitionID(partition, partitionKey == "")}
		snapshot[key]++
		if inventory[key] == nil {
			inventory[key] = map[string]struct{}{}
		}
		inventory[key][partName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts snapshot: %w", err)
	}
	g.mu.Lock()
	g.snapshot = snapshot
	g.activeParts = inventory
	g.snapshotGeneration++
	g.reconcileCandidateClaimsLocked()
	g.takenAt = g.now()
	g.haveSnap = !g.restoreBlocked && !g.hasPendingCleanupProofLocked() && len(g.pendingObserved) == 0
	for key := range g.committedReservations {
		g.reconcileCommittedKeyLocked(key)
	}
	g.mu.Unlock()
	if err := g.flushCandidateObservations(ctx); err != nil {
		g.mu.Lock()
		g.haveSnap = false
		g.mu.Unlock()
		return nil, fmt.Errorf("storage_integrity: persist observed candidate: %w", err)
	}
	g.mu.Lock()
	// A prior inventory may have observed a candidate whose durable marker was
	// only just persisted. Reconcile again so an already-absent finalized claim
	// can retire without waiting for another poll.
	g.reconcileCandidateClaimsLocked()
	g.haveSnap = !g.restoreBlocked && !g.hasPendingCleanupProofLocked() && len(g.pendingObserved) == 0
	for key := range g.committedReservations {
		g.reconcileCommittedKeyLocked(key)
	}
	result := copyPartsSnapshot(g.snapshot)
	g.mu.Unlock()
	return result, nil
}

func (g *PartsPressureGuard) Snapshot() (PartsSnapshot, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return nil, false
	}
	return copyPartsSnapshot(g.snapshot), true
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
	return g.reserve(ctx, "", table, partitionIDs)
}

// ReserveStatement is the journal-addressable admission form. Runtime callers
// that install a candidate-observation hook must use it so an exact name first
// seen after Commit can be persisted against the owning statement before the
// claim is ever retired.
func (g *PartsPressureGuard) ReserveStatement(ctx context.Context, statementID, table string, partitionIDs []string) (PartsReservation, error) {
	if statementID == "" {
		return nil, errors.New("storage_integrity: pressure reservation requires a statement id")
	}
	return g.reserve(ctx, statementID, table, partitionIDs)
}

func (g *PartsPressureGuard) reserve(ctx context.Context, statementID, table string, partitionIDs []string) (PartsReservation, error) {
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
	reservation, err := g.newReservationLocked(table, partitionIDs, true)
	if err != nil {
		return nil, err
	}
	reservation.statementID = statementID
	return reservation, nil
}

// RestoreBatch reconstructs a complete durable-journal projection from one
// successful exact inventory. It is transactional: any malformed coordinate,
// ownership conflict, or observation-persistence failure rolls back every
// provisional slot/claim in the batch. Finalized candidates that were observed
// by an earlier local inventory and are now absent are historical cleanup; an
// unseen absent candidate retains finalized capacity debt until it appears.
func (g *PartsPressureGuard) RestoreBatch(ctx context.Context, records []PartsRestoreRecord) (map[string]PartsReservation, error) {
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	if _, err := g.Refresh(ctx); err != nil {
		g.mu.Lock()
		g.restoreBlocked = true
		g.haveSnap = false
		g.mu.Unlock()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("storage_integrity: restore parts snapshot: %w", err)
	}

	g.commitMu.Lock()
	g.mu.Lock()
	// RestoreBatch owns admissionMu, so it can provisionally clear an earlier
	// restore latch while rebuilding. Any error below re-latches before unlock.
	g.restoreBlocked = false
	reservations := make(map[string]PartsReservation, len(records))
	type appliedRestore struct {
		reservation *partsReservation
		finalized   bool
	}
	applied := make([]appliedRestore, 0, len(records))
	seenStatements := make(map[string]struct{}, len(records))
	rollback := func() {
		for idx := len(applied) - 1; idx >= 0; idx-- {
			applied[idx].reservation.releaseLocked()
		}
	}
	fail := func(err error) (map[string]PartsReservation, error) {
		rollback()
		g.restoreBlocked = true
		g.haveSnap = false
		g.mu.Unlock()
		g.commitMu.Unlock()
		return nil, err
	}
	for _, record := range records {
		if record.StatementID == "" {
			return fail(errors.New("storage_integrity: restored pressure record has no statement id"))
		}
		if _, duplicate := seenStatements[record.StatementID]; duplicate {
			return fail(fmt.Errorf("storage_integrity: duplicate restored pressure statement %s", record.StatementID))
		}
		seenStatements[record.StatementID] = struct{}{}
		for _, observed := range record.ObservedCandidates {
			if !candidatePartIn(observed, record.Candidates) {
				return fail(fmt.Errorf("storage_integrity: restored observation %q is not a candidate for statement %s", observed.PartName, record.StatementID))
			}
		}
		if !record.Finalized && record.PartitionIDs == nil {
			return fail(fmt.Errorf("storage_integrity: restored statement %s has no known partition set", record.StatementID))
		}
		if record.Finalized && len(record.Candidates) == 0 {
			continue
		}
		observedCandidates := make(map[string]bool, len(record.ObservedCandidates))
		for _, candidate := range record.ObservedCandidates {
			observedCandidates[candidateCoordinateKey(candidate)] = true
		}
		partitionIDs := record.PartitionIDs
		candidates := record.Candidates
		if record.Finalized {
			allowedPartitions := make(map[string]struct{}, len(record.PartitionIDs))
			for _, partitionID := range record.PartitionIDs {
				allowedPartitions[partitionID] = struct{}{}
			}
			effective := make([]CandidatePart, 0, len(record.Candidates))
			for _, candidate := range record.Candidates {
				if candidate.PartName == "" {
					return fail(errors.New("storage_integrity: finalized recovery candidate has empty part name"))
				}
				if PhysicalTableName(candidate.TableID) != record.Table {
					return fail(fmt.Errorf("storage_integrity: finalized recovery candidate %q maps outside table %s", candidate.PartName, record.Table))
				}
				if _, ok := allowedPartitions[candidate.PartitionID]; !ok {
					return fail(fmt.Errorf("storage_integrity: finalized recovery candidate %q maps outside restored partition %s", candidate.PartName, candidate.PartitionID))
				}
				key := PartsKey{Database: g.cfg.UnsafeDatabase, Table: record.Table, Partition: candidate.PartitionID}
				_, active := g.activeParts[key][candidate.PartName]
				if record.LegacyObservation && !observedCandidates[candidateCoordinateKey(candidate)] && !active {
					return fail(fmt.Errorf(
						"%w: terminal statement %s candidate %s is absent without durable observation proof",
						ErrIntakeJournalMigrationRequired, record.StatementID, candidate.PartName,
					))
				}
				if observedCandidates[candidateCoordinateKey(candidate)] && !active {
					continue
				}
				effective = append(effective, candidate)
			}
			if len(effective) == 0 {
				continue
			}
			candidates = effective
			partitionIDs = restoreCandidatePartitions(effective)
		}
		reservation, err := g.newReservationLocked(record.Table, partitionIDs, false)
		if err != nil {
			return fail(err)
		}
		reservation.statementID = record.StatementID
		reservation.recovered = true
		reservation.observedCandidates = observedCandidates

		bindCandidates := candidates
		if record.Finalized {
			bindCandidates = make([]CandidatePart, 0, len(candidates))
			unseenAbsent := make([]bool, len(reservation.keys))
			for _, candidate := range candidates {
				keyIndex, candidateErr := reservation.keyIndexForCandidate(candidate)
				if candidateErr != nil {
					reservation.releaseLocked()
					return fail(candidateErr)
				}
				key := reservation.keys[keyIndex]
				_, active := g.activeParts[key][candidate.PartName]
				observed := reservation.candidateWasObserved(candidate)
				if observed && !active {
					continue
				}
				bindCandidates = append(bindCandidates, candidate)
				if !observed && !active {
					unseenAbsent[keyIndex] = true
				}
			}
			for keyIndex := range reservation.keys {
				if !unseenAbsent[keyIndex] {
					reservation.candidateCovered[keyIndex] = true
				}
			}
		}
		if err := reservation.bindCandidatePartsLocked(bindCandidates); err != nil {
			reservation.releaseLocked()
			return fail(err)
		}
		reservation.commitLocked()
		applied = append(applied, appliedRestore{reservation: reservation, finalized: record.Finalized})
		if !record.Finalized {
			reservations[record.StatementID] = reservation
		}
	}
	g.mu.Unlock()

	if err := g.flushCandidateObservations(ctx); err != nil {
		g.mu.Lock()
		return fail(fmt.Errorf("storage_integrity: persist restored candidate observation: %w", err))
	}

	g.mu.Lock()
	for _, restored := range applied {
		if restored.finalized {
			restored.reservation.finalizeLocked()
		}
	}
	g.restoreBlocked = false
	g.haveSnap = !g.hasPendingCleanupProofLocked() && len(g.pendingObserved) == 0
	g.mu.Unlock()
	g.commitMu.Unlock()
	return reservations, nil
}

func restoreCandidatePartitions(candidates []CandidatePart) []string {
	seen := make(map[string]struct{}, len(candidates))
	partitions := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := seen[candidate.PartitionID]; ok {
			continue
		}
		seen[candidate.PartitionID] = struct{}{}
		partitions = append(partitions, candidate.PartitionID)
	}
	sort.Strings(partitions)
	return partitions
}

// newReservationLocked captures the current exact inventory into a live
// reservation. Callers hold mu; lifecycle callers also serialize through
// admissionMu, and Restore additionally holds commitMu because it commits the
// new handle before returning.
func (g *PartsPressureGuard) newReservationLocked(table string, partitionIDs []string, enforceLimits bool) (*partsReservation, error) {
	if enforceLimits {
		if err := g.checkAvailableLocked(table, ""); err != nil {
			return nil, err
		}
	}
	keys := make([]PartsKey, 0, len(partitionIDs))
	baselines := make([]int, 0, len(partitionIDs))
	baselineParts := make([]map[string]struct{}, 0, len(partitionIDs))
	initialParts := make([]map[string]struct{}, 0, len(partitionIDs))
	visibleAfterGenerations := make([]uint64, 0, len(partitionIDs))
	generation := g.snapshotGeneration
	seen := make(map[string]bool, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		if seen[partitionID] {
			continue
		}
		seen[partitionID] = true
		if enforceLimits {
			if err := g.checkCapacityLocked(table, partitionID); err != nil {
				return nil, err
			}
		}
		key := PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}
		keys = append(keys, key)
		baselines = append(baselines, g.snapshot[key])
		captured := copyPartNames(g.activeParts[key])
		baselineParts = append(baselineParts, captured)
		initialParts = append(initialParts, copyPartNames(captured))
		visibleAfterGenerations = append(visibleAfterGenerations, generation)
	}
	for _, key := range keys {
		g.reserved[key]++
	}
	g.nextReservationID++
	reservation := &partsReservation{
		guard:            g,
		id:               g.nextReservationID,
		keys:             keys,
		baselines:        baselines,
		baselineParts:    baselineParts,
		initialParts:     initialParts,
		candidateParts:   make([]map[string]struct{}, len(keys)),
		candidateCovered: make([]bool, len(keys)),
		cleanupObserved:  make([]map[string]struct{}, len(keys)),
		visibleAfter:     visibleAfterGenerations,
		state:            reservationReserved,
	}
	g.liveReservations[reservation.id] = reservation
	return reservation, nil
}

func (g *PartsPressureGuard) checkLocked(table, partitionID string) error {
	if err := g.checkAvailableLocked(table, partitionID); err != nil {
		return err
	}
	return g.checkCapacityLocked(table, partitionID)
}

func (g *PartsPressureGuard) checkAvailableLocked(table, partitionID string) error {
	if g.restoreBlocked || !g.haveSnap || g.now().Sub(g.takenAt) > g.cfg.SnapshotTTL {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Kind: "unavailable"}
	}
	return nil
}

func (g *PartsPressureGuard) checkCapacityLocked(table, partitionID string) error {
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
	guard       *PartsPressureGuard
	id          uint64
	statementID string
	keys        []PartsKey
	baselines   []int
	// baselineParts is the exact inventory represented by baselines. It can be
	// advanced to a proven post-cleanup snapshot. initialParts is immutable and
	// prevents a pre-admission candidate name from covering new-write debt.
	baselineParts    []map[string]struct{}
	initialParts     []map[string]struct{}
	candidateParts   []map[string]struct{}
	candidateCovered []bool
	// visibleAfter is an exclusive snapshot-generation barrier. A reservation
	// can only be absorbed by a successful refresh after this generation. Exact
	// cleanup advances it so a cached pre-cleanup snapshot cannot be reused.
	visibleAfter        []uint64
	cleanupObserved     []map[string]struct{}
	cleanupProofArmed   bool
	cleanupProofPending bool
	recovered           bool
	observedCandidates  map[string]bool
	unboundRetained     bool
	state               reservationState // protected by guard.mu
}

type reservationState uint8

const (
	reservationReserved reservationState = iota + 1
	reservationCommitted
	reservationFinalized
	reservationReleased
)

func (r *partsReservation) Commit(candidates ...CandidatePart) error {
	if r == nil || r.guard == nil {
		return nil
	}
	// Keep lifecycle changes outside Reserve's refresh-to-baseline critical
	// section; otherwise cleanup proof could occur after the refresh but before
	// the new reservation captures its baseline.
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	bindErr := r.bindCandidatePartsLocked(candidates)
	if bindErr != nil {
		r.unboundRetained = true
	} else if len(candidates) > 0 {
		r.unboundRetained = false
	}
	r.commitLocked()
	for _, key := range r.keys {
		r.guard.reconcileCommittedKeyLocked(key)
	}
	return bindErr
}

func (r *partsReservation) CommitIndeterminate() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	r.unboundRetained = true
	r.commitLocked()
	for _, key := range r.keys {
		r.guard.reconcileCommittedKeyLocked(key)
	}
}

func (r *partsReservation) Release() {
	r.release()
}

// PrepareCleanupProof captures only frozen candidate names that are active in a
// successful inventory immediately before AbortPreparedStatement. It fences
// all new admission until the matching post-cleanup proof completes. A retry
// never overwrites an already-armed pre-cleanup snapshot: abort may have
// succeeded before the prior post-cleanup refresh failed.
func (r *partsReservation) PrepareCleanupProof(ctx context.Context, candidates []CandidatePart) error {
	if r == nil || r.guard == nil {
		return nil
	}
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.mu.RLock()
	done := r.state == reservationFinalized || r.state == reservationReleased
	armed := r.cleanupProofArmed
	r.guard.mu.RUnlock()
	if done || armed {
		return nil
	}
	_, refreshErr := r.guard.Refresh(ctx)
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	if refreshErr != nil {
		r.guard.haveSnap = false
		r.cleanupProofPending = true
		return cleanupProofError(refreshErr)
	}
	// Bind the frozen candidate identity before any source abort can run. Commit
	// also binds after a normal prepare, but terminal rejection reaches this
	// hook from inside Orchestrate before ingress can call Commit. Delaying the
	// check would let one statement ask the source to DROP another statement's
	// already-claimed active part.
	if err := r.bindCandidatePartsLocked(candidates); err != nil {
		r.guard.haveSnap = false
		r.cleanupProofPending = true
		return cleanupProofError(err)
	}
	for _, candidate := range candidates {
		if candidate.PartName == "" {
			err := errors.New("exact cleanup candidate has empty part name")
			r.guard.haveSnap = false
			r.cleanupProofPending = true
			return cleanupProofError(err)
		}
		keyIndex, candidateErr := r.keyIndexForCandidate(candidate)
		if candidateErr != nil {
			r.guard.haveSnap = false
			r.cleanupProofPending = true
			return cleanupProofError(candidateErr)
		}
		key := r.keys[keyIndex]
		if _, active := r.guard.activeParts[key][candidate.PartName]; !active {
			continue
		}
		if r.cleanupObserved[keyIndex] == nil {
			r.cleanupObserved[keyIndex] = map[string]struct{}{}
		}
		r.cleanupObserved[keyIndex][candidate.PartName] = struct{}{}
	}
	r.cleanupProofArmed = true
	r.cleanupProofPending = true
	r.guard.haveSnap = false
	return nil
}

// ReleaseCleaned cancels after exact candidate cleanup. It requires a preceding
// PrepareCleanupProof and proves every candidate that existed before cleanup is
// absent afterward. Only reservations whose captured baseline contained a
// proven-removed name advance to the complete post-cleanup inventory.
func (r *partsReservation) ReleaseCleaned(ctx context.Context) error {
	if r == nil || r.guard == nil {
		return nil
	}
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.mu.RLock()
	done := r.state == reservationFinalized || r.state == reservationReleased
	r.guard.mu.RUnlock()
	if done {
		return nil
	}
	r.guard.mu.RLock()
	armed := r.cleanupProofArmed
	r.guard.mu.RUnlock()
	if !armed {
		r.guard.commitMu.Lock()
		r.guard.mu.Lock()
		r.guard.haveSnap = false
		r.cleanupProofPending = true
		r.guard.mu.Unlock()
		r.guard.commitMu.Unlock()
		return cleanupProofError(errors.New("pre-cleanup candidate inventory was not prepared"))
	}
	_, refreshErr := r.guard.Refresh(ctx)
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	if refreshErr != nil {
		// Cleanup is already complete, but without a post-cleanup inventory proof
		// no descendant baseline may move. Make the cache unavailable immediately.
		r.guard.haveSnap = false
		r.cleanupProofPending = true
	} else if remaining := r.remainingObservedCandidatesLocked(); remaining != "" {
		r.guard.haveSnap = false
		r.cleanupProofPending = true
		refreshErr = fmt.Errorf("exact cleanup candidate %s remains active", remaining)
	} else {
		r.guard.rebaseLiveReservationsAfterCleanupLocked(r.id, r.cleanupObserved)
		r.cleanupProofPending = false
		r.releaseLocked()
		r.guard.haveSnap = !r.guard.restoreBlocked && !r.guard.hasPendingCleanupProofLocked()
	}
	if refreshErr != nil {
		return cleanupProofError(refreshErr)
	}
	return nil
}

func cleanupProofError(cause error) error {
	return fmt.Errorf("%w: %w: %w", ErrCleanupProofPending, ErrBackpressure, cause)
}

func (r *partsReservation) keyIndexForCandidate(candidate CandidatePart) (int, error) {
	if candidate.TableID == "" {
		return -1, fmt.Errorf("exact candidate %q has empty table_id", candidate.PartName)
	}
	physicalTable := PhysicalTableName(candidate.TableID)
	for idx, key := range r.keys {
		if key.Table == physicalTable && key.Partition == candidate.PartitionID {
			return idx, nil
		}
	}
	return -1, fmt.Errorf(
		"exact candidate %q maps to %s/%s outside reserved table/partitions",
		candidate.PartName, physicalTable, candidate.PartitionID,
	)
}

func (r *partsReservation) remainingObservedCandidatesLocked() string {
	for keyIndex, observed := range r.cleanupObserved {
		active := r.guard.activeParts[r.keys[keyIndex]]
		for partName := range observed {
			if _, ok := active[partName]; ok {
				return partName
			}
		}
	}
	return ""
}

func candidateCoordinateKey(candidate CandidatePart) string {
	return fmt.Sprintf("%d:%s/%d:%s/%d:%s",
		len(candidate.TableID), candidate.TableID,
		len(candidate.PartitionID), candidate.PartitionID,
		len(candidate.PartName), candidate.PartName,
	)
}

func observationKey(statementID string, candidate CandidatePart) string {
	return fmt.Sprintf("%d:%s/%s", len(statementID), statementID, candidateCoordinateKey(candidate))
}

func (r *partsReservation) candidateWasObserved(candidate CandidatePart) bool {
	if r == nil || r.observedCandidates == nil {
		return false
	}
	return r.observedCandidates[candidateCoordinateKey(candidate)]
}

func (r *partsReservation) bindCandidatePartsLocked(candidates []CandidatePart) error {
	if r.state == reservationReleased {
		return nil
	}
	if len(candidates) > 0 && r.guard.observedHook != nil && r.statementID == "" {
		return errors.New("storage_integrity: exact candidate reservation is not bound to a statement id")
	}
	keyIndexes := make([]int, len(candidates))
	for idx, candidate := range candidates {
		if candidate.PartName == "" {
			return errors.New("exact candidate has empty part name")
		}
		keyIndex, err := r.keyIndexForCandidate(candidate)
		if err != nil {
			return err
		}
		key := r.keys[keyIndex]
		claim := r.guard.candidateClaims[key][candidate.PartName]
		if claim.reservationID != 0 && claim.reservationID != r.id {
			return fmt.Errorf("exact candidate %q for %s/%s is already claimed by another reservation", candidate.PartName, key.Table, key.Partition)
		}
		keyIndexes[idx] = keyIndex
	}
	for idx, candidate := range candidates {
		keyIndex := keyIndexes[idx]
		key := r.keys[keyIndex]
		if r.guard.candidateClaims[key] == nil {
			r.guard.candidateClaims[key] = map[string]candidateClaim{}
		}
		claim := r.guard.candidateClaims[key][candidate.PartName]
		claim.reservationID = r.id
		claim.statementID = r.statementID
		claim.candidate = candidate
		if r.candidateWasObserved(candidate) {
			claim.observedActive = true
			claim.observedPersisted = true
		}
		if _, active := r.guard.activeParts[key][candidate.PartName]; active {
			claim.observedActive = true
			if !claim.observedPersisted {
				r.guard.pendingObserved[observationKey(r.statementID, candidate)] = candidateObservation{
					statementID: r.statementID,
					candidate:   candidate,
				}
			}
			if r.recovered {
				r.candidateCovered[keyIndex] = true
			}
		}
		r.guard.candidateClaims[key][candidate.PartName] = claim
		if r.candidateParts[keyIndex] == nil {
			r.candidateParts[keyIndex] = map[string]struct{}{}
		}
		r.candidateParts[keyIndex][candidate.PartName] = struct{}{}
	}
	return nil
}

func (g *PartsPressureGuard) releaseCandidateClaimsLocked(reservation *partsReservation) {
	for keyIndex, candidates := range reservation.candidateParts {
		key := reservation.keys[keyIndex]
		for partName := range candidates {
			claim := g.candidateClaims[key][partName]
			if claim.reservationID == reservation.id {
				delete(g.pendingObserved, observationKey(claim.statementID, claim.candidate))
				delete(g.candidateClaims[key], partName)
			}
		}
		if len(g.candidateClaims[key]) == 0 {
			delete(g.candidateClaims, key)
		}
	}
}

// reconcileCandidateClaimsLocked keeps finalized exact names single-owner for
// their full active lifetime. A source prepare result can precede system.parts
// visibility, so absence alone is not enough to retire an orphaned finalized
// claim: the name must first be observed active and only then observed absent.
// Cancelled and exactly-cleaned reservations remove their own claims directly.
func (g *PartsPressureGuard) reconcileCandidateClaimsLocked() {
	for key, claims := range g.candidateClaims {
		active := g.activeParts[key]
		for partName, claim := range claims {
			if _, ok := active[partName]; ok {
				claim.observedActive = true
				if !claim.observedPersisted {
					g.pendingObserved[observationKey(claim.statementID, claim.candidate)] = candidateObservation{
						statementID: claim.statementID,
						candidate:   claim.candidate,
					}
				}
				claims[partName] = claim
				continue
			}
			if claim.observedActive && claim.observedPersisted && g.liveReservations[claim.reservationID] == nil {
				delete(g.pendingObserved, observationKey(claim.statementID, claim.candidate))
				delete(claims, partName)
			}
		}
		if len(claims) == 0 {
			delete(g.candidateClaims, key)
		}
	}
}

func (g *PartsPressureGuard) flushCandidateObservations(ctx context.Context) error {
	for {
		g.mu.Lock()
		var (
			key         string
			observation candidateObservation
		)
		for pendingKey, pending := range g.pendingObserved {
			key, observation = pendingKey, pending
			break
		}
		hook := g.observedHook
		g.mu.Unlock()
		if key == "" {
			return nil
		}
		if hook != nil {
			if err := hook(ctx, observation.statementID, observation.candidate); err != nil {
				return err
			}
		}
		g.mu.Lock()
		claimKey := PartsKey{
			Database:  g.cfg.UnsafeDatabase,
			Table:     PhysicalTableName(observation.candidate.TableID),
			Partition: observation.candidate.PartitionID,
		}
		claims := g.candidateClaims[claimKey]
		claim, claimed := claims[observation.candidate.PartName]
		if claimed && claim.statementID == observation.statementID {
			claim.observedActive = true
			claim.observedPersisted = true
			claims[observation.candidate.PartName] = claim
		}
		delete(g.pendingObserved, key)
		g.mu.Unlock()
	}
}

func (g *PartsPressureGuard) hasPendingCleanupProofLocked() bool {
	for _, reservation := range g.liveReservations {
		if reservation.cleanupProofPending {
			return true
		}
	}
	return false
}

func (r *partsReservation) release() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	if r.state == reservationFinalized || r.state == reservationReleased {
		return
	}
	r.releaseLocked()
}

func (r *partsReservation) releaseLocked() {
	affected := make(map[PartsKey]struct{}, len(r.keys))
	switch r.state {
	case reservationReserved:
		for _, key := range r.keys {
			decrementPartCount(r.guard.reserved, key)
			affected[key] = struct{}{}
		}
	case reservationCommitted:
		for _, key := range r.keys {
			r.guard.removeCommittedReservationLocked(key, r.id)
			affected[key] = struct{}{}
		}
	}
	r.state = reservationReleased
	r.guard.releaseCandidateClaimsLocked(r)
	delete(r.guard.liveReservations, r.id)
	for key := range affected {
		r.guard.reconcileCommittedKeyLocked(key)
	}
}

func (r *partsReservation) Finalize() {
	if r == nil || r.guard == nil {
		return
	}
	r.guard.admissionMu.Lock()
	defer r.guard.admissionMu.Unlock()
	r.guard.commitMu.Lock()
	defer r.guard.commitMu.Unlock()
	r.guard.mu.Lock()
	defer r.guard.mu.Unlock()
	r.finalizeLocked()
}

func (r *partsReservation) finalizeLocked() {
	if r.state == reservationReserved {
		r.commitLocked()
	}
	if r.state != reservationCommitted {
		return
	}
	r.state = reservationFinalized
	if len(r.keys) == 0 {
		delete(r.guard.liveReservations, r.id)
		return
	}
	for _, key := range r.keys {
		r.guard.reconcileCommittedKeyLocked(key)
	}
}

func (r *partsReservation) commitLocked() {
	if r.state != reservationReserved {
		return
	}
	r.state = reservationCommitted
	for idx, key := range r.keys {
		decrementPartCount(r.guard.reserved, key)
		r.guard.committedReservations[key] = append(r.guard.committedReservations[key], reservationSlot{
			reservation: r,
			keyIndex:    idx,
		})
		r.guard.reconcileCommittedKeyLocked(key)
	}
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
		if entry.reservation.id != reservationID {
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

func (g *PartsPressureGuard) pruneFinalizedReservationLocked(reservation *partsReservation) {
	if reservation == nil || reservation.state != reservationFinalized {
		return
	}
	for _, entries := range g.committedReservations {
		for _, entry := range entries {
			if entry.reservation == reservation {
				return
			}
		}
	}
	// Exact names outlive a finalized reservation handle while their parts are
	// active. Refresh retires each claim only after active -> absent evidence, so
	// an already-queued statement cannot reuse the name and abort the ACK2 part.
	delete(g.liveReservations, reservation.id)
}

// coveredReservationSlotsLocked returns aggregate visible capacity without
// assigning any observed increase to a concrete reservation identity.
func (g *PartsPressureGuard) coveredReservationSlotsLocked(key PartsKey, slots []reservationSlot) int {
	sort.SliceStable(slots, func(i, j int) bool {
		left, right := slots[i], slots[j]
		leftBaseline := left.reservation.baselines[left.keyIndex]
		rightBaseline := right.reservation.baselines[right.keyIndex]
		if leftBaseline != rightBaseline {
			return leftBaseline < rightBaseline
		}
		leftGeneration := left.reservation.visibleAfter[left.keyIndex]
		rightGeneration := right.reservation.visibleAfter[right.keyIndex]
		if leftGeneration != rightGeneration {
			return leftGeneration < rightGeneration
		}
		return left.reservation.id < right.reservation.id
	})
	observed := g.snapshot[key]
	nextVisible := 0
	covered := 0
	for idx, slot := range slots {
		baseline := slot.reservation.baselines[slot.keyIndex]
		if idx == 0 || baseline+1 > nextVisible {
			nextVisible = baseline + 1
		}
		if g.snapshotGeneration > slot.reservation.visibleAfter[slot.keyIndex] && nextVisible <= observed {
			covered++
			nextVisible++
		}
	}
	exactCovered := 0
	unresolvedIdentity := 0
	for _, slot := range slots {
		reservation := slot.reservation
		keyIndex := slot.keyIndex
		if !reservation.candidateCovered[keyIndex] && g.snapshotGeneration > reservation.visibleAfter[keyIndex] {
			for partName := range reservation.candidateParts[keyIndex] {
				if _, preexisting := reservation.initialParts[keyIndex][partName]; preexisting {
					continue
				}
				if _, active := g.activeParts[key][partName]; active {
					reservation.candidateCovered[keyIndex] = true
					break
				}
			}
		}
		if reservation.candidateCovered[keyIndex] {
			exactCovered++
		} else if len(reservation.candidateParts[keyIndex]) > 0 {
			// Once a prepared result names an exact candidate, unrelated aggregate
			// growth cannot prove this slot visible. In particular, a finalized
			// restart candidate may appear after another source's part; releasing
			// its debt at the earlier N+1 would oversubscribe the partition.
			unresolvedIdentity++
		} else if reservation.unboundRetained && reservation.state != reservationFinalized {
			// A cancelable committed slot with no candidate identity represents an
			// indeterminate Prepare response. Aggregate growth cannot prove that
			// statement wrote the visible part: a later source lookup may prove it
			// wrote nothing and reuse the same frontier-owned slot for retry.
			unresolvedIdentity++
		}
	}
	// Aggregate growth cannot identify an owner, while an exact candidate name
	// can prove additional coverage even when a cleanup made the count net-zero.
	// Taking the maximum (not the sum) prevents the same visible part from
	// covering both its exact reservation and another aggregate slot.
	if exactCovered > covered {
		covered = exactCovered
	}
	if maximum := len(slots) - unresolvedIdentity; covered > maximum {
		covered = maximum
	}
	return covered
}

// rebaseLiveReservationsAfterCleanupLocked advances a reservation only when its
// captured baseline contained an exact candidate observed before cleanup and
// absent afterward. The new baseline is the complete post-cleanup inventory,
// not baseline-1: this includes any offsetting concurrent write and prevents it
// from being misattributed to a queued reservation. Reserve order is irrelevant.
func (g *PartsPressureGuard) rebaseLiveReservationsAfterCleanupLocked(cleanedID uint64, removed []map[string]struct{}) {
	affected := map[PartsKey]struct{}{}
	for id, reservation := range g.liveReservations {
		if id == cleanedID || reservation.state == reservationReleased {
			continue
		}
		for keyIndex, key := range reservation.keys {
			cleanerKeyIndex := -1
			cleaner := g.liveReservations[cleanedID]
			if cleaner != nil {
				for idx, cleanerKey := range cleaner.keys {
					if cleanerKey == key {
						cleanerKeyIndex = idx
						break
					}
				}
			}
			if cleanerKeyIndex < 0 {
				continue
			}
			containsRemoved := false
			for partName := range removed[cleanerKeyIndex] {
				if _, captured := reservation.baselineParts[keyIndex][partName]; captured {
					containsRemoved = true
					break
				}
			}
			if !containsRemoved {
				continue
			}
			reservation.baselines[keyIndex] = g.snapshot[key]
			reservation.baselineParts[keyIndex] = copyPartNames(g.activeParts[key])
			reservation.visibleAfter[keyIndex] = g.snapshotGeneration
			affected[key] = struct{}{}
		}
	}
	for key := range affected {
		g.reconcileCommittedKeyLocked(key)
	}
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
	covered := g.coveredReservationSlotsLocked(key, entries)

	// Final identities become non-cancelable at ACK2. Compact them only when the
	// snapshot covers the entire cohort, so no ambiguous aggregate growth is ever
	// assigned to a specific statement merely to free its handle.
	if covered == len(entries) {
		kept := entries[:0]
		finalized := make([]*partsReservation, 0)
		for _, entry := range entries {
			if entry.reservation.state != reservationFinalized {
				kept = append(kept, entry)
			} else {
				finalized = append(finalized, entry.reservation)
			}
		}
		entries = kept
		if len(entries) == 0 {
			delete(g.committedReservations, key)
			delete(g.committed, key)
			for _, reservation := range finalized {
				g.pruneFinalizedReservationLocked(reservation)
			}
			return
		}
		g.committedReservations[key] = entries
		for _, reservation := range finalized {
			g.pruneFinalizedReservationLocked(reservation)
		}
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

func copyPartNames(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for partName := range in {
		out[partName] = struct{}{}
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
