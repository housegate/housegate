package storageintegrity

import (
	"context"
	"fmt"
)

// SafeAuditReadbackPort is the gated port that reads a worker's local hg_safe
// active set and recomputes per-part row-hashes for a frozen AuditTask, so the
// worker can run VerifyLocalActiveSet against real storage. No implementation
// exists: it needs the companion C3 SafeAudit ServingEvidence seam (a real
// post-ACK3 snapshot readback). See CompanionMutationConsensusAvailable.
type SafeAuditReadbackPort interface {
	ReadLocalActiveSet(ctx context.Context, task AuditTask) (LocalAuditEvidence, error)
}

// SafeAuditVoteSubmitter is the gated port that submits a signed vote to the
// Arbiter FSM (which records signed votes and deterministically derives the
// decision/quarantine, reading no row data — design section 5.1). No
// implementation exists: it needs the companion C3 SubmitAuditVote RPC, which is
// absent from arbiter/arbiter-proto. See CompanionMutationConsensusAvailable.
type SafeAuditVoteSubmitter interface {
	SubmitAuditVote(ctx context.Context, vote SafeAuditVote) error
}

// SafeAuditWorker drives one worker's serving audit: read the local active set,
// build a fail-closed signed vote, and submit it. It holds only the two gated
// ports and the worker's signer — no Arbiter FSM/decision state (the FSM derives
// the decision from signed votes). Until the companion C3 seam lands, the ports
// have no real implementation and RunAudit fails closed.
type SafeAuditWorker struct {
	readback  SafeAuditReadbackPort
	submitter SafeAuditVoteSubmitter
	signer    *Ed25519ClaimSigner
}

// NewSafeAuditWorker constructs the worker over its readback + submit ports and
// signer. Every dependency must be non-nil.
func NewSafeAuditWorker(readback SafeAuditReadbackPort, submitter SafeAuditVoteSubmitter, signer *Ed25519ClaimSigner) (*SafeAuditWorker, error) {
	if readback == nil || submitter == nil {
		return nil, fmt.Errorf("safe audit worker: readback and submit ports are required")
	}
	if signer == nil {
		return nil, fmt.Errorf("safe audit worker: signer is required")
	}
	return &SafeAuditWorker{readback: readback, submitter: submitter, signer: signer}, nil
}

// RunAudit would read the local active set for the frozen task, sign a
// fail-closed vote (Pass only on a full local match), and submit it. It fails
// closed while the companion C3 SafeAudit seam is absent: no real readback/submit
// port exists, so the worker cannot vote against real storage and must not
// fabricate the SafeAudit protocol.
func (w *SafeAuditWorker) RunAudit(ctx context.Context, task AuditTask) (SafeAuditVote, error) {
	if !CompanionMutationConsensusAvailable {
		return SafeAuditVote{}, fmt.Errorf("safe audit worker: companion C3 SafeAudit ServingEvidence/SubmitAuditVote seam absent; cannot run a serving audit end-to-end")
	}
	if err := task.Valid(); err != nil {
		return SafeAuditVote{}, err
	}
	ev, err := w.readback.ReadLocalActiveSet(ctx, task)
	if err != nil {
		return SafeAuditVote{}, err
	}
	vote, err := SignAuditVote(ctx, w.signer, task, ev)
	if err != nil {
		return SafeAuditVote{}, err
	}
	if err := w.submitter.SubmitAuditVote(ctx, vote); err != nil {
		return SafeAuditVote{}, err
	}
	return vote, nil
}
