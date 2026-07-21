package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// ControlledCompactionDriver is the gated C4 port that builds the output
// compacted parts in the hg_compact shadow (design section 6 step 2). No
// implementation exists: it needs the companion controlled-compaction seam. See
// CompanionMutationConsensusAvailable.
type ControlledCompactionDriver interface {
	ExecuteControlledCompaction(ctx context.Context, workerID string, plan CompactionPlan) (CompactionOutput, error)
}

// CompactionPublicationDriver is the gated C4 port that publishes the compacted
// parts via signed base-CAS REPLACE and returns the per-worker ack (design
// section 6 step 4/5). No implementation exists; see
// CompanionMutationConsensusAvailable.
type CompactionPublicationDriver interface {
	PublishCompaction(ctx context.Context, workerID string, plan ReplacePartitionPlan, baseRoots []replay.PartitionCommitment) (PublicationAck, error)
}

// CompactionWorker drives one controlled compaction: build the shadow output,
// verify the LtHash equation, publish via signed base-CAS REPLACE, then derive
// the new content-addressed manifest id. It holds only the two gated C4 ports; it
// never selects the input parts (the Arbiter does) nor commits the manifest.
// Until the companion C4 seam lands the ports have no real implementation and
// RunCompaction fails closed.
type CompactionWorker struct {
	compactor ControlledCompactionDriver
	publisher CompactionPublicationDriver
}

// NewCompactionWorker constructs the worker over its two gated ports.
func NewCompactionWorker(compactor ControlledCompactionDriver, publisher CompactionPublicationDriver) (*CompactionWorker, error) {
	if compactor == nil || publisher == nil {
		return nil, fmt.Errorf("compaction worker: compactor and publisher ports are required")
	}
	return &CompactionWorker{compactor: compactor, publisher: publisher}, nil
}

// RunCompaction would build the shadow output, verify the equation, publish via
// REPLACE, and derive the new manifest id. It fails closed while the companion C4
// seam is absent: no real compaction/publication driver exists, so the worker
// cannot change the safe part layout and must not fabricate the controlled-
// compaction protocol.
func (w *CompactionWorker) RunCompaction(ctx context.Context, workerID string, plan CompactionPlan) (PublicationAck, string, error) {
	if !CompanionMutationConsensusAvailable {
		return PublicationAck{}, "", fmt.Errorf("compaction worker: companion C4 controlled-compaction seam absent; cannot change the safe part layout end-to-end")
	}
	if err := plan.Valid(); err != nil {
		return PublicationAck{}, "", err
	}
	output, err := w.compactor.ExecuteControlledCompaction(ctx, workerID, plan)
	if err != nil {
		return PublicationAck{}, "", err
	}
	replacePlan, err := BuildCompactionReplacePlan(plan, output)
	if err != nil {
		return PublicationAck{}, "", err
	}
	ack, err := w.publisher.PublishCompaction(ctx, workerID, replacePlan, plan.BasePartitionRoots)
	if err != nil {
		return PublicationAck{}, "", err
	}
	manifestID, err := BuildCompactionManifestID(plan, output)
	if err != nil {
		return PublicationAck{}, "", err
	}
	return ack, manifestID, nil
}
