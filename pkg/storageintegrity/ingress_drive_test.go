package storageintegrity

import (
	"context"
	"testing"
)

// TestIngressDrivesOrchestratorToAck2 is the one end-to-end P1e runtime assertion
// PR07 adds: a complete INSERT admission driven through the orchestrator reaches
// ACK2 with an RCBound lifecycle. It lives in-package so it can reach the fake
// submitter/preparer used across the intake tests.
func TestIngressDrivesOrchestratorToAck2(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if !res.Ack2 {
		t.Fatal("a complete INSERT admission must reach ACK2 through the runtime")
	}
	if res.Lifecycle != LifecycleRCBound {
		t.Fatalf("expected RCBound lifecycle at ACK2, got %q", res.Lifecycle)
	}
}
