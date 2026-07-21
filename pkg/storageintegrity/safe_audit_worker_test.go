package storageintegrity

import (
	"context"
	"testing"
)

// --- fake ports (test doubles, never a companion-protocol stand-in) ---

type fakeAuditReadback struct{ ev LocalAuditEvidence }

func (f fakeAuditReadback) ReadLocalActiveSet(context.Context, AuditTask) (LocalAuditEvidence, error) {
	return f.ev, nil
}

type fakeAuditSubmitter struct{ got []SafeAuditVote }

func (f *fakeAuditSubmitter) SubmitAuditVote(_ context.Context, v SafeAuditVote) error {
	f.got = append(f.got, v)
	return nil
}

func TestNewSafeAuditWorker_RequiresPortsAndSigner(t *testing.T) {
	signer := mustAuditSigner(t, "w-1", 1)
	if _, err := NewSafeAuditWorker(nil, &fakeAuditSubmitter{}, signer); err == nil {
		t.Fatal("nil readback must be a wiring error")
	}
	if _, err := NewSafeAuditWorker(fakeAuditReadback{}, nil, signer); err == nil {
		t.Fatal("nil submitter must be a wiring error")
	}
	if _, err := NewSafeAuditWorker(fakeAuditReadback{}, &fakeAuditSubmitter{}, nil); err == nil {
		t.Fatal("nil signer must be a wiring error")
	}
	if _, err := NewSafeAuditWorker(fakeAuditReadback{}, &fakeAuditSubmitter{}, signer); err != nil {
		t.Fatalf("well-formed wiring must succeed: %v", err)
	}
}

func TestSafeAuditWorker_RunAudit_FailsClosedWhenC3Absent(t *testing.T) {
	task := auditTaskFixture()
	w, _ := NewSafeAuditWorker(fakeAuditReadback{ev: matchingEvidence(task)}, &fakeAuditSubmitter{}, mustAuditSigner(t, "w-1", 1))
	if _, err := w.RunAudit(context.Background(), task); err == nil {
		t.Fatal("RunAudit must fail closed while the C3 SafeAudit seam is absent")
	}
}

// --- Gated: the real post-ACK3 readback + FSM vote submission need the absent C3 seam. ---

func TestSafeAuditWorker_ReadsLocalActiveSetAndVotesAgainstRealManifest(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion C3 SafeAudit ServingEvidence readback seam lands")
}

func TestSafeAuditWorker_SubmitsSignedVoteToArbiterFSM(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion SubmitAuditVote RPC lands")
}

func TestSafeAudit_MinorityQuarantineAppliedToReadSet_ACK3NotRetroactivelyRevoked(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the real safe-cut quarantine-install seam lands (PR20/C3)")
}
