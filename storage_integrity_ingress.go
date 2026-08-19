package housegate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityTableSchemaResolver resolves the immutable table schema used
// to derive payload partitions. This host-level seam deliberately stays out of
// pkg/storageintegrity: signed-envelope v2 removed schema resolution from the
// core protocol and production ingress owns the authoritative schema set.
type StorageIntegrityTableSchemaResolver interface {
	StorageIntegrityTableSchema(tableID string) (payloadexec.TableSchema, bool)
}

// StorageIntegrityTableSchemaResolverFunc adapts a function into a resolver.
type StorageIntegrityTableSchemaResolverFunc func(tableID string) (payloadexec.TableSchema, bool)

func (fn StorageIntegrityTableSchemaResolverFunc) StorageIntegrityTableSchema(tableID string) (payloadexec.TableSchema, bool) {
	return fn(tableID)
}

// StorageIntegrityIngress is the P1e runtime shell that connects the ingress
// admission plugin to the staged-intake orchestrator. It implements the
// plugin's AdmissionConsumer: a completed, signed admission is mapped into a
// core AdmissionRecord and driven through the orchestrator, which runs the
// staged prepare / submit / RC-late-binding path to ACK2.
//
// It owns only the orchestrator, the merge guard, the selected replay
// materializer kind, and the optional DA/PayloadStore writer that produces the
// opaque payload_ref before staged intake starts. It deliberately constructs NO
// HouseGate-owned Verifier or Promoter: verifier selection, quorum, and
// manifest publication are Arbiter / SNode responsibilities the orchestrator
// only drives through ports (design sections 3.6 and 4.1). HouseGate provides
// arbiter-proto SubmitStatement, GetStatementStatus, and PayloadStore put
// adapters. The embedding host supplies the selected-SNode SourcePreparer plus
// PreparedStatementLookup adapter.
type StorageIntegrityIngress struct {
	orch           *sicore.Orchestrator
	guard          StorageIntegrityMergeGuard
	matKind        sicore.MaterializerKind
	payloadWriter  sicore.PayloadWriter
	leaseManager   sicore.PayloadLeaseManager
	mergeRunner    *StorageIntegrityMergeSupervisor
	pressure       StorageIntegrityPartsPressure
	schemas        StorageIntegrityTableSchemaResolver
	pressureRunner StorageIntegrityPartsPressureLifecycle
	statementLocks [64]sync.Mutex
	pressureMu     sync.Mutex
	// pressureReservations keeps indeterminate/unsafe-written reservations
	// addressable by statement until ACK2 finalizes them or a required source
	// lookup / exact cleanup proves that no candidate part remains.
	pressureReservations map[string]trackedPartsReservation

	cleanupProofTimeout time.Duration

	backgroundMu     sync.Mutex
	backgroundCancel context.CancelFunc
	backgroundWG     sync.WaitGroup
	backgroundClosed bool
}

type trackedPartsReservation struct {
	reservation            sicore.PartsReservation
	tableID                string
	partitions             []string
	reuseWhileFrontierHeld bool
}

// NewStorageIntegrityIngress constructs the ingress runtime over an orchestrator
// and the selected materializer kind. The merge guard is optional (nil when no
// ClickHouse connection is wired). A nil orchestrator is a wiring error.
func NewStorageIntegrityIngress(orch *sicore.Orchestrator, guard StorageIntegrityMergeGuard, matKind sicore.MaterializerKind) (*StorageIntegrityIngress, error) {
	return NewStorageIntegrityIngressWithPayloadWriter(orch, guard, matKind, nil)
}

// NewStorageIntegrityIngressWithPayloadWriter constructs the ingress runtime
// with a DA/PayloadStore writer. When writer is nil, the runtime preserves the
// previous local content-addressed payload_ref fallback; production P1 wiring
// should pass a real PayloadWriter so SubmitStatement carries an opaque
// PayloadStore ref.
func NewStorageIntegrityIngressWithPayloadWriter(orch *sicore.Orchestrator, guard StorageIntegrityMergeGuard, matKind sicore.MaterializerKind, writer sicore.PayloadWriter) (*StorageIntegrityIngress, error) {
	if orch == nil {
		return nil, fmt.Errorf("storage_integrity ingress: orchestrator is required")
	}
	switch matKind {
	case sicore.MaterializerNative, sicore.MaterializerCSV:
	default:
		return nil, fmt.Errorf("storage_integrity ingress: valid materializer kind is required")
	}
	return &StorageIntegrityIngress{
		orch:                 orch,
		guard:                guard,
		matKind:              matKind,
		payloadWriter:        writer,
		pressureReservations: map[string]trackedPartsReservation{},
		cleanupProofTimeout:  sicore.DefaultPartsRefreshTimeout,
	}, nil
}

// RecoverPending drains durable non-terminal intake records before listeners
// admit new source writes.
func (i *StorageIntegrityIngress) RecoverPending(ctx context.Context) error {
	if i == nil || i.orch == nil {
		return fmt.Errorf("storage_integrity ingress: orchestrator is required")
	}
	if err := i.orch.RecoverPending(ctx); err != nil {
		return err
	}
	// Every recovered non-terminal record is now either exactly cleaned (whose
	// hook removed its reservation) or ACK2. Finalize the remaining restored
	// handles so their visible debt/claims follow normal post-ACK2 semantics.
	i.finalizeRecoveredReservations()
	return nil
}

func (i *StorageIntegrityIngress) StartBackground(ctx context.Context) {
	if i == nil || (i.leaseManager == nil && i.mergeRunner == nil && i.pressureRunner == nil) {
		return
	}
	i.backgroundMu.Lock()
	if i.backgroundCancel != nil || i.backgroundClosed {
		i.backgroundMu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	i.backgroundCancel = cancel
	leaseManager := i.leaseManager
	mergeRunner := i.mergeRunner
	pressureRunner := i.pressureRunner
	if leaseManager != nil {
		i.backgroundWG.Add(1)
		go func() {
			defer i.backgroundWG.Done()
			leaseManager.Run(runCtx)
		}()
	}
	if mergeRunner != nil {
		i.backgroundWG.Add(1)
		go func() {
			defer i.backgroundWG.Done()
			mergeRunner.Run(runCtx)
		}()
	}
	if pressureRunner != nil {
		i.backgroundWG.Add(1)
		go func() {
			defer i.backgroundWG.Done()
			pressureRunner.Run(runCtx)
		}()
	}
	i.backgroundMu.Unlock()
}

func (i *StorageIntegrityIngress) Close() {
	if i == nil {
		return
	}
	i.backgroundMu.Lock()
	cancel := i.backgroundCancel
	i.backgroundCancel = nil
	i.backgroundClosed = true
	i.backgroundMu.Unlock()
	if cancel != nil {
		cancel()
	}
	i.backgroundWG.Wait()
}

// WithPartsPressure enables the ingress back-pressure check. The resolver
// supplies the pinned schema used to derive every payload partition.
func (i *StorageIntegrityIngress) WithPartsPressure(pressure StorageIntegrityPartsPressure, schemas StorageIntegrityTableSchemaResolver) {
	i.pressure = pressure
	i.schemas = schemas
	pressure.SetCandidateObservedHook(i.orch.MarkCandidateObserved)
	i.orch.SetBeforeRecovery(i.restorePressureReservations)
	i.orch.SetBeforeRegisterPreparedClaim(i.bindPreparedCandidates)
	i.orch.SetBeforeExactCleanup(i.beforeExactCleanup)
	i.orch.SetAfterExactCleanup(i.afterExactCleanup)
}

// WithTableSchemas binds the authoritative schema resolver even when parts
// pressure is disabled. Candidate validation still needs the payload-derived
// touched partition set; disabling admission pressure only skips reservation.
func (i *StorageIntegrityIngress) WithTableSchemas(schemas StorageIntegrityTableSchemaResolver) {
	i.schemas = schemas
	i.orch.SetBeforeRecovery(i.restorePressureReservations)
}

func (i *StorageIntegrityIngress) bindPreparedCandidates(_ context.Context, res sicore.IntakeResult) error {
	tracked, ok := i.trackedPressureReservation(res.StatementID)
	if !ok {
		return nil
	}
	if err := validatePressureCandidateBinding(res.StatementID, tracked.tableID, tracked.partitions, res.Prepared.CandidateParts); err != nil {
		return err
	}
	if err := tracked.reservation.Commit(res.Prepared.CandidateParts...); err != nil {
		return fmt.Errorf("storage_integrity ingress: bind prepared candidates for %s before RC: %w", res.StatementID, err)
	}
	return nil
}

func (i *StorageIntegrityIngress) beforeExactCleanup(ctx context.Context, res sicore.IntakeResult) error {
	tracked, ok := i.trackedPressureReservation(res.StatementID)
	if !ok {
		return nil
	}
	if err := validatePressureCandidateBinding(res.StatementID, tracked.tableID, tracked.partitions, res.Prepared.CandidateParts); err != nil {
		return err
	}
	proofCtx, cancel := i.cleanupProofContext(ctx)
	defer cancel()
	return tracked.reservation.PrepareCleanupProof(proofCtx, res.Prepared.CandidateParts)
}

func (i *StorageIntegrityIngress) afterExactCleanup(ctx context.Context, res sicore.IntakeResult) error {
	tracked, ok := i.trackedPressureReservation(res.StatementID)
	if !ok {
		return nil
	}
	proofCtx, cancel := i.cleanupProofContext(ctx)
	defer cancel()
	err := tracked.reservation.ReleaseCleaned(proofCtx)
	if err == nil {
		i.deletePressureReservation(res.StatementID)
	}
	return err
}

func validatePressureCandidateBinding(statementID, tableID string, touchedPartitions []string, candidates []sicore.CandidatePart) error {
	expected := make(map[string]struct{}, len(touchedPartitions))
	for _, partitionID := range touchedPartitions {
		if partitionID == "" {
			return fmt.Errorf("storage_integrity ingress: statement %s has an empty touched candidate partition", statementID)
		}
		if _, duplicate := expected[partitionID]; duplicate {
			return fmt.Errorf("storage_integrity ingress: statement %s touched candidate partition %q is duplicated", statementID, partitionID)
		}
		expected[partitionID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.TableID == "" || candidate.TableID != tableID {
			return fmt.Errorf("storage_integrity ingress: statement %s candidate table_id %q does not match admission table_id %q", statementID, candidate.TableID, tableID)
		}
		if candidate.PartitionID == "" || candidate.PartName == "" {
			return fmt.Errorf("storage_integrity ingress: statement %s has incomplete exact candidate %q/%q", statementID, candidate.PartitionID, candidate.PartName)
		}
		if _, duplicate := seen[candidate.PartitionID]; duplicate {
			return fmt.Errorf("storage_integrity ingress: statement %s candidate partition %q is duplicated", statementID, candidate.PartitionID)
		}
		seen[candidate.PartitionID] = struct{}{}
		if _, ok := expected[candidate.PartitionID]; !ok {
			return fmt.Errorf("storage_integrity ingress: statement %s candidate partition %q was not touched by the payload", statementID, candidate.PartitionID)
		}
	}
	for partitionID := range expected {
		if _, ok := seen[partitionID]; !ok {
			return fmt.Errorf("storage_integrity ingress: statement %s candidate partition %q is missing", statementID, partitionID)
		}
	}
	return nil
}

func (i *StorageIntegrityIngress) restorePressureReservations(ctx context.Context, records []sicore.IntakeJournalRecord) error {
	if i.pressure == nil && i.schemas == nil {
		return nil
	}
	restoreRecords := make([]sicore.PartsRestoreRecord, 0, len(records))
	restoredPartitions := make(map[string][]string, len(records))
	for _, record := range records {
		if knownUnwrittenTerminalAbort(record) {
			// The source durably proved that Prepare rejected before writing, and
			// the concurrent Submit result is already an authoritative terminal
			// rejection. Recovery must still re-run its idempotent empty abort, but
			// it must not project capacity debt that no candidate can ever cover.
			continue
		}
		if record.IsTerminal && !record.TerminalResult.Ack2 {
			continue
		}
		if record.IsTerminal && !record.HasPrepared {
			return fmt.Errorf("storage_integrity ingress: terminal ACK2 statement %s has no prepared result", record.StatementID)
		}
		var (
			table                   string
			partitions              []string
			candidates              []sicore.CandidatePart
			legacyMissingPartitions bool
			err                     error
		)
		if record.HasPrepared {
			candidates = record.Prepared.CandidateParts
		}
		if err := validateObservedPressureCandidates(record); err != nil {
			return err
		}
		if record.IsTerminal {
			table = sicore.PhysicalTableName(record.Admission.TableID)
			partitions = clonePartitionIDs(record.Admission.TouchedPartitionIDs)
			legacyMissingPartitions = partitions == nil && record.JournalVersion == 0 && len(candidates) > 0
			if legacyMissingPartitions {
				// The observation-unaware development journal predates the durable
				// payload-derived set. It may only migrate when exact active names
				// provide the proof enforced by RestoreBatch below.
				partitions = candidatePartitionIDs(candidates)
			}
		} else {
			table, partitions, err = i.partsPressureTarget(record.Admission)
			if err != nil {
				return fmt.Errorf("storage_integrity ingress: restore pressure target for %s: %w", record.StatementID, err)
			}
		}
		if record.HasPrepared {
			if err := validatePressureCandidateBinding(record.StatementID, record.Admission.TableID, partitions, candidates); err != nil {
				return err
			}
		}
		if legacyMissingPartitions {
			if err := i.orch.PersistRecoveredTouchedPartitions(ctx, record.StatementID, partitions); err != nil {
				return fmt.Errorf("storage_integrity ingress: migrate recovered touched partitions for %s: %w", record.StatementID, err)
			}
		} else if !record.IsTerminal {
			if err := i.orch.PersistRecoveredTouchedPartitions(ctx, record.StatementID, partitions); err != nil {
				return fmt.Errorf("storage_integrity ingress: bind recovered touched partitions for %s: %w", record.StatementID, err)
			}
		}
		restoreRecords = append(restoreRecords, sicore.PartsRestoreRecord{
			StatementID:        record.StatementID,
			Table:              table,
			PartitionIDs:       partitions,
			Candidates:         candidates,
			ObservedCandidates: record.ObservedCandidateParts,
			Finalized:          record.IsTerminal,
			LegacyObservation:  record.JournalVersion == 0,
		})
		restoredPartitions[record.StatementID] = clonePartitionIDs(partitions)
	}
	if i.pressure == nil {
		return nil
	}
	restored, err := i.pressure.RestoreBatch(ctx, restoreRecords)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: restore pressure reservations: %w", err)
	}
	i.pressureMu.Lock()
	for _, record := range records {
		reservation := restored[record.StatementID]
		if reservation == nil {
			continue
		}
		i.pressureReservations[record.StatementID] = trackedPartsReservation{
			reservation: reservation,
			tableID:     record.Admission.TableID,
			partitions:  clonePartitionIDs(restoredPartitions[record.StatementID]),
		}
	}
	i.pressureMu.Unlock()
	return nil
}

func knownUnwrittenTerminalAbort(record sicore.IntakeJournalRecord) bool {
	return !record.IsTerminal &&
		record.Stage == sicore.LifecycleAbortPending &&
		record.PrepareKnownUnwritten &&
		!record.HasPrepared &&
		record.HasSubmit &&
		record.Submit.Category.RequiresAbort()
}

func validateObservedPressureCandidates(record sicore.IntakeJournalRecord) error {
	for _, candidate := range record.ObservedCandidateParts {
		if !candidatePartIn(candidate, record.Prepared.CandidateParts) {
			return fmt.Errorf("storage_integrity ingress: statement %s observed candidate %q is not in the prepared result", record.StatementID, candidate.PartName)
		}
	}
	return nil
}

func candidatePartIn(candidate sicore.CandidatePart, candidates []sicore.CandidatePart) bool {
	for _, current := range candidates {
		if current.TableID == candidate.TableID && current.PartitionID == candidate.PartitionID && current.PartName == candidate.PartName {
			return true
		}
	}
	return false
}

func candidatePartitionIDs(candidates []sicore.CandidatePart) []string {
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

func clonePartitionIDs(partitionIDs []string) []string {
	if partitionIDs == nil {
		return nil
	}
	return append([]string{}, partitionIDs...)
}

func (i *StorageIntegrityIngress) finalizeRecoveredReservations() {
	i.pressureMu.Lock()
	restored := i.pressureReservations
	i.pressureReservations = map[string]trackedPartsReservation{}
	i.pressureMu.Unlock()
	for _, tracked := range restored {
		tracked.reservation.Finalize()
	}
}

func (i *StorageIntegrityIngress) cleanupProofContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), i.cleanupProofTimeout)
}

func (i *StorageIntegrityIngress) partsPressureTarget(rec sicore.AdmissionRecord) (string, []string, error) {
	if i.schemas == nil && i.pressure == nil {
		return "", nil, nil
	}
	if i.schemas == nil {
		return "", nil, fmt.Errorf("storage_integrity ingress: back-pressure requires a table schema resolver")
	}
	schema, ok := i.schemas.StorageIntegrityTableSchema(rec.TableID)
	if !ok {
		return "", nil, fmt.Errorf("storage_integrity ingress: no pinned schema for table %q", rec.TableID)
	}
	if schema.TableID != rec.TableID {
		return "", nil, fmt.Errorf("storage_integrity ingress: schema table_id %q does not match admission table_id %q", schema.TableID, rec.TableID)
	}
	if rec.EnvelopeVersion >= sicore.EnvelopeVersionV2 {
		expectedSchemaHash := payloadexec.TableSchemaHash(rec.NetworkID, schema)
		if rec.SchemaHash != expectedSchemaHash {
			return "", nil, fmt.Errorf(
				"storage_integrity ingress: schema_hash %q does not match authoritative table schema %q for %s",
				rec.SchemaHash,
				expectedSchemaHash,
				rec.TableID,
			)
		}
	}
	partitions, err := sicore.PayloadPartitionIDs(schema, rec.PayloadEncoding, rec.Revision, rec.Payload)
	if err != nil {
		return "", nil, fmt.Errorf("storage_integrity ingress: %w", err)
	}
	return sicore.PhysicalTableName(rec.TableID), partitions, nil
}

func (i *StorageIntegrityIngress) reservePartsPressure(ctx context.Context, statementID, table string, partitions []string) (sicore.PartsReservation, error) {
	reservation, err := i.pressure.ReserveStatement(ctx, statementID, table, partitions)
	if err == nil {
		return reservation, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !errors.Is(err, sicore.ErrBackpressure) {
		err = fmt.Errorf("%w: pressure refresh unavailable: %w", sicore.ErrBackpressure, err)
	}
	return nil, backpressureClientError(table, err)
}

func (i *StorageIntegrityIngress) lockStatement(statementID string) func() {
	var hash uint32 = 2166136261
	for idx := 0; idx < len(statementID); idx++ {
		hash ^= uint32(statementID[idx])
		hash *= 16777619
	}
	lock := &i.statementLocks[hash%uint32(len(i.statementLocks))]
	lock.Lock()
	return lock.Unlock
}

func backpressureClientError(table string, err error) error {
	storageIntegrityBackpressureTotal.WithLabelValues(table).Inc()
	var backpressure *sicore.BackpressureError
	message := "storage_integrity: back-pressure: retry later"
	if errors.As(err, &backpressure) {
		message = backpressure.Error()
	}
	return &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: message, Err: err}
}

// ConsumeStorageIntegrityAdmission maps a completed plugin admission into a core
// AdmissionRecord and drives the orchestrator. A non-ACK2 outcome is surfaced as
// an error so the plugin reports failure to the client rather than a false
// success; only a bound ACK2 returns nil. This is the production staged-intake
// path.
func (i *StorageIntegrityIngress) ConsumeStorageIntegrityAdmission(ctx context.Context, adm siplugin.Admission) error {
	if health, ok := i.guard.(StorageIntegrityMergeHealth); ok {
		if err := health.CheckMergeHealth(); err != nil {
			return fmt.Errorf("storage_integrity ingress: merge health: %w", err)
		}
	}
	rec := AdmissionRecordFromPlugin(adm)
	actualMaterializer, err := sicore.SelectMaterializerKind(rec.PayloadEncoding)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: %w", err)
	}
	if actualMaterializer != i.matKind {
		return fmt.Errorf(
			"storage_integrity ingress: runtime requires %s materializer, payload encoding %q selects %s",
			storageIntegrityMaterializerName(i.matKind),
			rec.PayloadEncoding,
			storageIntegrityMaterializerName(actualMaterializer),
		)
	}
	unlockStatement := i.lockStatement(rec.StatementID)
	defer unlockStatement()

	table, partitions, err := i.partsPressureTarget(rec)
	if err != nil {
		return err
	}
	if partitions != nil {
		rec.TouchedPartitionIDs = clonePartitionIDs(partitions)
	}
	requiresPrepare, err := i.orch.AdmissionRequiresPrepare(ctx, rec)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: preflight %s: %w", rec.StatementID, err)
	}
	tracked, _ := i.trackedPressureReservation(rec.StatementID)
	trackedReservation := tracked.reservation
	var attemptReservation sicore.PartsReservation
	reusedRetryReservation := false
	if requiresPrepare && trackedReservation != nil {
		if tracked.reuseWhileFrontierHeld {
			// A pre-write Prepare rejection retains both the source frontier and its
			// original uncommitted capacity slot. Reuse that handle: releasing and
			// re-reserving would let a queued statement take the slot while still
			// blocking behind this statement's frontier.
			attemptReservation = trackedReservation
			reusedRetryReservation = true
		} else {
			// For an existing indeterminate prepare, true means the required source
			// lookup proved the previous attempt wrote nothing. Cancel its committed
			// slot before the ordinary fresh admission gate.
			trackedReservation.Release()
			i.deletePressureReservation(rec.StatementID)
			trackedReservation = nil
		}
	}
	if i.pressure != nil && requiresPrepare && attemptReservation == nil {
		attemptReservation, err = i.reservePartsPressure(ctx, rec.StatementID, table, partitions)
		if err != nil {
			return err
		}
		trackedReservation = attemptReservation
		i.setPressureReservation(rec.StatementID, rec.TableID, partitions, attemptReservation)
	}
	if i.payloadWriter != nil {
		put, err := i.payloadWriter.PutPayload(ctx, rec.Payload, rec.PayloadHash, rec.PayloadLength)
		if err != nil {
			if !reusedRetryReservation {
				i.cancelAttemptReservation(rec.StatementID, attemptReservation)
			}
			return fmt.Errorf("storage_integrity ingress: put payload for %s: %w", rec.StatementID, err)
		}
		if put.PayloadRef == "" {
			if !reusedRetryReservation {
				i.cancelAttemptReservation(rec.StatementID, attemptReservation)
			}
			return fmt.Errorf("storage_integrity ingress: payload store returned empty payload_ref for %s", rec.StatementID)
		}
		rec.PayloadRef = put.PayloadRef
	}
	res, err := i.orch.Orchestrate(ctx, rec)
	switch {
	case errors.Is(err, sicore.ErrCleanupProofPending):
		// Exact cleanup proof is pending (before or after the idempotent source
		// abort). Keep the statement-addressed reservation/fence for same-ID retry.
		i.setReuseWhileFrontierHeld(rec.StatementID, false)
	case res.Lifecycle == sicore.LifecycleCleaned && res.IsTerminal():
		// Exact cleanup normally reconciles/releases this reservation from its
		// hook while the source frontier is held. A terminal outcome whose Prepare
		// was durably known unwritten has no exact candidates and deliberately
		// bypasses that hook, so cancel the handle here as well. Release is
		// idempotent after ReleaseCleaned. A retryable pre-write Prepare rejection
		// also reports Cleaned, but its accepted Submit is not terminal: retain its
		// statement reservation with the source frontier until same-ID retry.
		i.cancelTrackedReservation(rec.StatementID, trackedReservation)
	case res.RetainsSourceFrontier() && !res.SourceWriteMayExist():
		// A pre-write failure can report Cleaned, AbortPending, or another local
		// error while deliberately retaining the source frontier. Keep the matching
		// uncommitted capacity reservation for that same interval, including across
		// payload-store retries, so a queued statement cannot invert capacity and
		// source ordering.
		i.setReuseWhileFrontierHeld(rec.StatementID, true)
	case attemptReservation != nil && (errors.Is(err, sicore.ErrBackpressure) || !res.SourceWriteMayExist()):
		// SNode hard pressure and every definite pre-prepare failure occur before a
		// source write. Only an actually attempted/known prepare stays charged.
		i.cancelTrackedReservation(rec.StatementID, attemptReservation)
	case attemptReservation != nil:
		i.setReuseWhileFrontierHeld(rec.StatementID, false)
		if commitErr := attemptReservation.Commit(res.Prepared.CandidateParts...); commitErr != nil {
			err = errors.Join(err, fmt.Errorf("storage_integrity ingress: bind prepared candidates for %s: %w", rec.StatementID, commitErr))
		} else if res.Ack2 {
			attemptReservation.Finalize()
			i.deletePressureReservation(rec.StatementID)
		}
	case trackedReservation != nil && res.SourceWriteMayExist():
		// A resumed indeterminate prepare may reveal its exact candidates only on
		// lookup. Bind them even on a non-terminal retry so exact inventory can
		// account for a net-zero cleanup/write interleaving.
		i.setReuseWhileFrontierHeld(rec.StatementID, false)
		if commitErr := trackedReservation.Commit(res.Prepared.CandidateParts...); commitErr != nil {
			err = errors.Join(err, fmt.Errorf("storage_integrity ingress: bind resumed candidates for %s: %w", rec.StatementID, commitErr))
		} else if res.Ack2 {
			trackedReservation.Finalize()
			i.deletePressureReservation(rec.StatementID)
		}
	}
	if i.pressure != nil {
		// Orchestrate may create or clean candidate parts on success, retryable
		// outcomes, terminal rejection, or indeterminate errors. Refresh every
		// such transition; local preflight refusals return above without mutation.
		i.pressure.Invalidate()
	}
	if err != nil {
		if errors.Is(err, sicore.ErrBackpressure) {
			return backpressureClientError(sicore.PhysicalTableName(rec.TableID), err)
		}
		return fmt.Errorf("storage_integrity ingress: orchestrate %s: %w", rec.StatementID, err)
	}
	if !res.Ack2 {
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
	return nil
}

func (i *StorageIntegrityIngress) pressureReservation(statementID string) sicore.PartsReservation {
	tracked, ok := i.trackedPressureReservation(statementID)
	if !ok {
		return nil
	}
	return tracked.reservation
}

func (i *StorageIntegrityIngress) trackedPressureReservation(statementID string) (trackedPartsReservation, bool) {
	i.pressureMu.Lock()
	defer i.pressureMu.Unlock()
	tracked, ok := i.pressureReservations[statementID]
	return tracked, ok
}

func (i *StorageIntegrityIngress) setPressureReservation(statementID, tableID string, partitions []string, reservation sicore.PartsReservation) {
	if reservation == nil {
		return
	}
	i.pressureMu.Lock()
	i.pressureReservations[statementID] = trackedPartsReservation{
		reservation: reservation,
		tableID:     tableID,
		partitions:  clonePartitionIDs(partitions),
	}
	i.pressureMu.Unlock()
}

func (i *StorageIntegrityIngress) setReuseWhileFrontierHeld(statementID string, reusable bool) {
	i.pressureMu.Lock()
	tracked, ok := i.pressureReservations[statementID]
	if ok {
		tracked.reuseWhileFrontierHeld = reusable
		i.pressureReservations[statementID] = tracked
	}
	i.pressureMu.Unlock()
}

func (i *StorageIntegrityIngress) deletePressureReservation(statementID string) {
	i.pressureMu.Lock()
	delete(i.pressureReservations, statementID)
	i.pressureMu.Unlock()
}

func (i *StorageIntegrityIngress) cancelAttemptReservation(statementID string, reservation sicore.PartsReservation) {
	if reservation == nil {
		return
	}
	reservation.Release()
	i.deletePressureReservation(statementID)
}

func (i *StorageIntegrityIngress) cancelTrackedReservation(statementID string, reservation sicore.PartsReservation) {
	if reservation != nil {
		reservation.Release()
	}
	i.deletePressureReservation(statementID)
}

func storageIntegrityMaterializerName(kind sicore.MaterializerKind) string {
	switch kind {
	case sicore.MaterializerNative:
		return "Native"
	case sicore.MaterializerCSV:
		return "CSV"
	default:
		return "unspecified"
	}
}

// AdmissionRecordFromPlugin is the pure projection of a plugin Admission into
// the core AdmissionRecord the orchestrator consumes. Signed statement fields
// and captured bytes are carried from the plugin; payload_hash is normalized to
// the replay/arbiter digest profile so the sequenced envelope matches
// arbiter-proto and verifier expectations. It is deterministic and performs no
// source-side I/O.
func AdmissionRecordFromPlugin(adm siplugin.Admission) sicore.AdmissionRecord {
	encoding := adm.Payload.Encoding
	if encoding == "" {
		encoding = sicore.PayloadEncodingClickHouseNativeData
	}
	return sicore.AdmissionRecord{
		StatementID:     adm.StatementID,
		Kind:            coreKind(adm.Kind),
		TableID:         adm.TableID,
		SQL:             adm.SQL,
		SQLHash:         adm.SQLHash,
		Signer:          adm.Signer,
		UserJWS:         adm.UserJWS,
		Payload:         adm.Payload.Bytes,
		PayloadLength:   adm.Payload.Length,
		PayloadHash:     replay.DigestBytes(adm.Payload.Bytes),
		PayloadEncoding: encoding,
		Revision:        adm.Payload.Revision,
		EnvelopeVersion: adm.EnvelopeVersion,
		NetworkID:       adm.NetworkID,
		KeeperShardID:   adm.KeeperShardID,
		SettingsHash:    adm.SettingsHash,
		SchemaHash:      adm.SchemaHash,
		RowIDProfileID:  adm.RowIDProfileID,
	}
}

// coreKind maps the plugin statement kind to the core kind. The two packages use
// identical string values on purpose (they cannot import each other), so the
// mapping is a direct string carry.
func coreKind(k siplugin.Kind) sicore.Kind {
	switch k {
	case siplugin.KindInsert:
		return sicore.KindInsert
	default:
		return sicore.Kind(string(k))
	}
}
