package housegate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/housegate/housegate/pkg/chproto"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

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
	schemas        sicore.TableSchemaResolver
	pressureRunner StorageIntegrityPartsPressureLifecycle
	statementLocks [64]sync.Mutex
	pressureMu     sync.Mutex
	// pressureReservations keeps indeterminate/unsafe-written reservations
	// addressable by statement until ACK2 finalizes them or a required source
	// lookup / exact cleanup proves that no candidate part remains.
	pressureReservations map[string]sicore.PartsReservation

	backgroundMu     sync.Mutex
	backgroundCancel context.CancelFunc
	backgroundWG     sync.WaitGroup
	backgroundClosed bool
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
		pressureReservations: map[string]sicore.PartsReservation{},
	}, nil
}

// RecoverPending drains durable non-terminal intake records before listeners
// admit new source writes.
func (i *StorageIntegrityIngress) RecoverPending(ctx context.Context) error {
	if i == nil || i.orch == nil {
		return fmt.Errorf("storage_integrity ingress: orchestrator is required")
	}
	return i.orch.RecoverPending(ctx)
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
func (i *StorageIntegrityIngress) WithPartsPressure(pressure StorageIntegrityPartsPressure, schemas sicore.TableSchemaResolver) {
	i.pressure = pressure
	i.schemas = schemas
}

func (i *StorageIntegrityIngress) partsPressureTarget(rec sicore.AdmissionRecord) (string, []string, error) {
	if i.pressure == nil {
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
	partitions, err := sicore.PayloadPartitionIDs(schema, rec.PayloadEncoding, rec.Revision, rec.Payload)
	if err != nil {
		return "", nil, fmt.Errorf("storage_integrity ingress: %w", err)
	}
	return sicore.PhysicalTableName(rec.TableID), partitions, nil
}

func (i *StorageIntegrityIngress) reservePartsPressure(ctx context.Context, table string, partitions []string) (sicore.PartsReservation, error) {
	reservation, err := i.pressure.Reserve(ctx, table, partitions)
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
	requiresPrepare, err := i.orch.AdmissionRequiresPrepare(ctx, rec)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: preflight %s: %w", rec.StatementID, err)
	}
	trackedReservation := i.pressureReservation(rec.StatementID)
	if requiresPrepare && trackedReservation != nil {
		// For an existing statement, true means the required source lookup proved
		// the previous indeterminate prepare wrote nothing (or no prepare was ever
		// attempted). Cancel that statement's old committed slot before re-gating.
		trackedReservation.Release()
		i.deletePressureReservation(rec.StatementID)
		trackedReservation = nil
	}
	var attemptReservation sicore.PartsReservation
	if i.pressure != nil && requiresPrepare {
		attemptReservation, err = i.reservePartsPressure(ctx, table, partitions)
		if err != nil {
			return err
		}
		trackedReservation = attemptReservation
		i.setPressureReservation(rec.StatementID, attemptReservation)
	}
	if i.payloadWriter != nil {
		put, err := i.payloadWriter.PutPayload(ctx, rec.Payload, rec.PayloadHash, rec.PayloadLength)
		if err != nil {
			i.cancelAttemptReservation(rec.StatementID, attemptReservation)
			return fmt.Errorf("storage_integrity ingress: put payload for %s: %w", rec.StatementID, err)
		}
		if put.PayloadRef == "" {
			i.cancelAttemptReservation(rec.StatementID, attemptReservation)
			return fmt.Errorf("storage_integrity ingress: payload store returned empty payload_ref for %s", rec.StatementID)
		}
		rec.PayloadRef = put.PayloadRef
	}
	res, err := i.orch.Orchestrate(ctx, rec)
	switch {
	case res.Lifecycle == sicore.LifecycleCleaned:
		// Exact cleanup is definitive even when the following terminal-journal
		// save failed. Cancel a reserved, committed, or already-visible identity.
		i.cancelCleanedReservation(rec.StatementID, trackedReservation)
	case attemptReservation != nil && (errors.Is(err, sicore.ErrBackpressure) || !res.SourceWriteMayExist()):
		// SNode hard pressure and every definite pre-prepare failure occur before a
		// source write. Only an actually attempted/known prepare stays charged.
		i.cancelTrackedReservation(rec.StatementID, attemptReservation)
	case attemptReservation != nil:
		attemptReservation.Commit()
		if res.Ack2 {
			attemptReservation.Finalize()
			i.deletePressureReservation(rec.StatementID)
		}
	case trackedReservation != nil && res.Ack2:
		trackedReservation.Finalize()
		i.deletePressureReservation(rec.StatementID)
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
	i.pressureMu.Lock()
	defer i.pressureMu.Unlock()
	return i.pressureReservations[statementID]
}

func (i *StorageIntegrityIngress) setPressureReservation(statementID string, reservation sicore.PartsReservation) {
	if reservation == nil {
		return
	}
	i.pressureMu.Lock()
	i.pressureReservations[statementID] = reservation
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

func (i *StorageIntegrityIngress) cancelCleanedReservation(statementID string, reservation sicore.PartsReservation) {
	if reservation != nil {
		reservation.ReleaseCleaned()
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
