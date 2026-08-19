package housegate

import (
	"context"
	"fmt"
	"sync"

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
	orch          *sicore.Orchestrator
	guard         StorageIntegrityMergeGuard
	matKind       sicore.MaterializerKind
	payloadWriter sicore.PayloadWriter
	leaseManager  sicore.PayloadLeaseManager
	mergeRunner   *StorageIntegrityMergeSupervisor

	backgroundMu     sync.Mutex
	backgroundCancel context.CancelFunc
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
	return &StorageIntegrityIngress{orch: orch, guard: guard, matKind: matKind, payloadWriter: writer}, nil
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
	if i == nil || (i.leaseManager == nil && i.mergeRunner == nil) {
		return
	}
	i.backgroundMu.Lock()
	if i.backgroundCancel != nil {
		i.backgroundMu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	i.backgroundCancel = cancel
	i.backgroundMu.Unlock()
	if i.leaseManager != nil {
		go i.leaseManager.Run(runCtx)
	}
	if i.mergeRunner != nil {
		go i.mergeRunner.Run(runCtx)
	}
}

func (i *StorageIntegrityIngress) Close() {
	if i == nil {
		return
	}
	i.backgroundMu.Lock()
	cancel := i.backgroundCancel
	i.backgroundCancel = nil
	i.backgroundMu.Unlock()
	if cancel != nil {
		cancel()
	}
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
	if i.payloadWriter != nil {
		put, err := i.payloadWriter.PutPayload(ctx, rec.Payload, rec.PayloadHash, rec.PayloadLength)
		if err != nil {
			return fmt.Errorf("storage_integrity ingress: put payload for %s: %w", rec.StatementID, err)
		}
		if put.PayloadRef == "" {
			return fmt.Errorf("storage_integrity ingress: payload store returned empty payload_ref for %s", rec.StatementID)
		}
		rec.PayloadRef = put.PayloadRef
	}
	res, err := i.orch.Orchestrate(ctx, rec)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: orchestrate %s: %w", rec.StatementID, err)
	}
	if !res.Ack2 {
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
	return nil
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
