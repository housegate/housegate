package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// RepairSyncer is the gated port that syncs a lagging worker to a target
// manifest from an authoritative source and returns its exact readback of the
// synced parts. No implementation exists: it needs the companion seam that
// exposes the authoritative manifest/canonical-peer sync. See
// CompanionMutationConsensusAvailable.
type RepairSyncer interface {
	SyncToManifest(ctx context.Context, workerID, targetManifestID string, scope []AffectedPartition) (source RepairSource, readback []CandidatePart, err error)
}

// ServingReentryAuditor is the gated C3 SafeAudit port that reports whether a
// repaired worker has re-passed the serving audit for a target manifest. No
// implementation exists: it needs the companion C3 SafeAudit seam. See
// CompanionMutationConsensusAvailable.
type ServingReentryAuditor interface {
	ServingAuditPassed(ctx context.Context, workerID, targetManifestID string) (bool, error)
}

// RepairWorker drives one worker's repair → exact-verify → audit → read-set
// re-entry (design section 5.3). It holds the gated syncer and auditor ports; it
// never implements the Arbiter read-set cut. Until the companion seam lands the
// ports have no real implementation and Recover fails closed.
type RepairWorker struct {
	workerID string
	syncer   RepairSyncer
	auditor  ServingReentryAuditor
}

// NewRepairWorker constructs the driver over its gated ports. Both ports and a
// non-blank worker id are required.
func NewRepairWorker(workerID string, syncer RepairSyncer, auditor ServingReentryAuditor) (*RepairWorker, error) {
	if workerID == "" {
		return nil, fmt.Errorf("repair worker: blank worker id")
	}
	if syncer == nil || auditor == nil {
		return nil, fmt.Errorf("repair worker: syncer and auditor ports are required")
	}
	return &RepairWorker{workerID: workerID, syncer: syncer, auditor: auditor}, nil
}

// Recover would sync the worker to the target manifest, verify its exact
// readback, wait for the serving audit to pass, and only then report the worker
// eligible to re-enter the read set — all via the pure DecideReadSetReentry
// invariant. It fails closed while the companion seam is absent: no real syncer /
// auditor exists, so the worker cannot recover end to end and must not fabricate
// the authoritative-sync or SafeAudit protocol.
func (w *RepairWorker) Recover(ctx context.Context, targetManifestID string, manifestActiveParts []replay.PartManifestEntry, stillQuarantined bool) (ReadSetReentryDecision, error) {
	if !CompanionMutationConsensusAvailable {
		return ReadSetReentryDecision{}, fmt.Errorf("repair worker: companion repair/SafeAudit seam absent; cannot recover a worker into the read set end-to-end")
	}
	if targetManifestID == "" || len(manifestActiveParts) == 0 {
		return ReadSetReentryDecision{}, fmt.Errorf("repair worker: blank target manifest or empty active parts")
	}
	scope := affectedFromManifest(manifestActiveParts)
	source, readback, err := w.syncer.SyncToManifest(ctx, w.workerID, targetManifestID, scope)
	if err != nil {
		return ReadSetReentryDecision{}, err
	}
	auditPassed, err := w.auditor.ServingAuditPassed(ctx, w.workerID, targetManifestID)
	if err != nil {
		return ReadSetReentryDecision{}, err
	}
	return DecideReadSetReentry(ReadSetReentryInput{
		WorkerID:            w.workerID,
		TargetManifestID:    targetManifestID,
		ManifestActiveParts: manifestActiveParts,
		WorkerReadbackParts: readback,
		RepairSource:        source,
		StillQuarantined:    stillQuarantined,
		ServingAuditPassed:  auditPassed,
	}), nil
}

// affectedFromManifest projects the distinct (table, partition) pairs of a
// manifest scope, deduplicated.
func affectedFromManifest(parts []replay.PartManifestEntry) []AffectedPartition {
	seen := map[string]bool{}
	var out []AffectedPartition
	for _, p := range parts {
		k := p.TableID + "/" + p.PartitionID
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, AffectedPartition{TableID: p.TableID, PartitionID: p.PartitionID})
	}
	return out
}
