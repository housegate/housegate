package housegate

import (
	"context"
	"fmt"

	siplugin "housegate/housegate/pkg/plugins/storageintegrity"
	sicore "housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityIngress is the P1e runtime shell that connects the ingress
// admission plugin to the staged-intake orchestrator. It implements the
// plugin's AdmissionConsumer: a completed, signed admission is mapped into a
// core AdmissionRecord and driven through the orchestrator, which runs the
// staged prepare / submit / RC-late-binding path to ACK2.
//
// It owns only the orchestrator, the merge guard, and the selected replay
// materializer kind. It deliberately constructs NO HouseGate-owned Verifier or
// Promoter: verifier selection, quorum, and manifest publication are Arbiter /
// SNode responsibilities the orchestrator only drives through ports (design
// sections 3.6 and 4.1). Until the companion staged-prepare seam lands, the
// orchestrator has no real StatementSubmitter/SourcePreparer/IntakeStatusQuerier
// adapters, so this consumer can be constructed and validated but cannot yet
// close a statement to ACK2 (see CompanionStagedIntakeAvailable).
type StorageIntegrityIngress struct {
	orch    *sicore.Orchestrator
	guard   *sicore.MergeGuard
	matKind sicore.MaterializerKind
}

// NewStorageIntegrityIngress constructs the ingress runtime over an orchestrator
// and the selected materializer kind. The merge guard is optional (nil when no
// ClickHouse connection is wired). A nil orchestrator is a wiring error.
func NewStorageIntegrityIngress(orch *sicore.Orchestrator, guard *sicore.MergeGuard, matKind sicore.MaterializerKind) (*StorageIntegrityIngress, error) {
	if orch == nil {
		return nil, fmt.Errorf("storage_integrity ingress: orchestrator is required")
	}
	return &StorageIntegrityIngress{orch: orch, guard: guard, matKind: matKind}, nil
}

// ConsumeStorageIntegrityAdmission maps a completed plugin admission into a core
// AdmissionRecord and drives the orchestrator. A non-ACK2 outcome is surfaced as
// an error so the plugin reports failure to the client rather than a false
// success; only a bound ACK2 returns nil. This is the C1-gated end-to-end path.
func (i *StorageIntegrityIngress) ConsumeStorageIntegrityAdmission(ctx context.Context, adm siplugin.Admission) error {
	rec := AdmissionRecordFromPlugin(adm)
	res, err := i.orch.Orchestrate(ctx, rec)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: orchestrate %s: %w", rec.StatementID, err)
	}
	if !res.Ack2 {
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
	return nil
}

// AdmissionRecordFromPlugin is the pure projection of a plugin Admission into
// the core AdmissionRecord the orchestrator consumes. It carries every
// signed/captured field verbatim; the payload encoding is threaded so the
// runtime can select the matching materializer. It is deterministic and needs
// no companion seam.
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
		PayloadHash:     adm.Payload.SHA256,
		PayloadEncoding: encoding,
		Revision:        adm.Payload.Revision,
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
