package housegate

import (
	"context"
	"fmt"

	sicore "housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityMutation is the P2 mutation runtime shell, analogous to
// StorageIntegrityIngress for P1e INSERT. It holds only the mutation ports, the
// safe-read gate, the fixed 3-worker set, and the versioned serving floor — no
// Verifier/Promoter and no Arbiter FSM/quorum/manifest state. HouseGate drives
// the P2 flow through the ports; it does not decide the quorum, publish the
// manifest, or commit the safe cut. Until the companion C2 mutation-consensus
// seam lands, the ports have no real implementation and RunMutation fails closed.
type StorageIntegrityMutation struct {
	submitter sicore.MutationSubmitter
	claims    sicore.MutationClaimSubmitter
	acker     sicore.MutationPublicationAcker
	safeCut   sicore.MutationSafeCutPublisher
	publisher sicore.MutationPublicationDriver
	readGate  *sicore.SafeReadGate
	workerIDs []string
	floor     int
}

// NewStorageIntegrityMutation constructs the mutation runtime over the four
// mutation ports, the publication driver, and the safe-read gate. It requires
// every port and the gate non-nil and exactly three distinct worker ids (the
// fixed P2 v1 profile, consistent with MutationServingAvailabilityFloor==2). The
// worker ids are defensively copied.
func NewStorageIntegrityMutation(
	submitter sicore.MutationSubmitter,
	claims sicore.MutationClaimSubmitter,
	acker sicore.MutationPublicationAcker,
	safeCut sicore.MutationSafeCutPublisher,
	publisher sicore.MutationPublicationDriver,
	readGate *sicore.SafeReadGate,
	workerIDs []string,
) (*StorageIntegrityMutation, error) {
	if submitter == nil || claims == nil || acker == nil || safeCut == nil || publisher == nil {
		return nil, fmt.Errorf("storage_integrity mutation: all mutation ports are required")
	}
	if readGate == nil {
		return nil, fmt.Errorf("storage_integrity mutation: safe-read gate is required")
	}
	if len(workerIDs) != 3 {
		return nil, fmt.Errorf("storage_integrity mutation: P2 v1 requires exactly 3 serving workers, got %d", len(workerIDs))
	}
	seen := map[string]bool{}
	for _, w := range workerIDs {
		if w == "" {
			return nil, fmt.Errorf("storage_integrity mutation: blank worker id")
		}
		if seen[w] {
			return nil, fmt.Errorf("storage_integrity mutation: duplicate worker id %q", w)
		}
		seen[w] = true
	}
	return &StorageIntegrityMutation{
		submitter: submitter,
		claims:    claims,
		acker:     acker,
		safeCut:   safeCut,
		publisher: publisher,
		readGate:  readGate,
		workerIDs: append([]string(nil), workerIDs...),
		floor:     sicore.MutationServingAvailabilityFloor,
	}, nil
}

// WorkerIDs returns a defensive copy of the configured worker ids.
func (m *StorageIntegrityMutation) WorkerIDs() []string {
	return append([]string(nil), m.workerIDs...)
}

// ServingAvailabilityFloor returns the versioned P2 profile floor (2). It proves
// HouseGate reads the profile constant rather than inventing a runtime-mutable
// floor.
func (m *StorageIntegrityMutation) ServingAvailabilityFloor() int {
	return m.floor
}

// validatePublicationInputs runs the runtime's canonical-artifact preflight
// through the shared pure builder (rather than re-deriving it), so the runtime
// cannot silently accept a malformed artifact.
func (m *StorageIntegrityMutation) validatePublicationInputs(art sicore.CanonicalArtifact) error {
	if _, err := sicore.BuildCanonicalPublicationSet(art); err != nil {
		return fmt.Errorf("storage_integrity mutation: publication preflight: %w", err)
	}
	return nil
}

// RunMutation would drive the full P2 flow (SubmitMutation → 3 tasks → 2/3
// claim quorum → canonical publication → per-worker ack → atomic safe cut). It
// fails closed while the companion C2 mutation-consensus seam is absent: no real
// port implementation exists, so the runtime cannot execute end to end and must
// not fabricate the Arbiter protocol.
func (m *StorageIntegrityMutation) RunMutation(_ context.Context, _ sicore.MutationStatementEnvelope) error {
	if !sicore.CompanionMutationConsensusAvailable {
		return fmt.Errorf("storage_integrity mutation: companion C2 mutation-consensus seam absent; runtime cannot execute end-to-end")
	}
	return fmt.Errorf("storage_integrity mutation: end-to-end orchestration not implemented")
}
