package storageintegrity

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// recordingStageObserver records each observed stage as "stage" or
// "stage!" when the call failed.
type recordingStageObserver struct {
	mu     sync.Mutex
	stages []string
}

func (r *recordingStageObserver) ObserveIntakeStage(stage string, elapsed time.Duration, err error) {
	if elapsed < 0 {
		panic("negative stage duration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		stage += "!"
	}
	r.stages = append(r.stages, stage)
}

func (r *recordingStageObserver) counts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for _, s := range r.stages {
		out[s]++
	}
	return out
}

// TestOrchestrate_ObservesEveryStageOfAnAck2Intake: an intake that reaches
// ACK2 reports the frontier wait, one prepare, one submit, one claim and its
// journal saves, all successful; the observer changes nothing about the
// outcome.
func TestOrchestrate_ObservesEveryStageOfAnAck2Intake(t *testing.T) {
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	obs := &recordingStageObserver{}
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal, StageObserver: obs})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil || !res.Ack2 {
		t.Fatalf("Orchestrate = %+v, %v; want ACK2", res, err)
	}
	got := obs.counts()
	for _, stage := range []string{IntakeStageFrontierWait, IntakeStagePrepare, IntakeStageSubmit, IntakeStageRegisterClaim} {
		if got[stage] != 1 {
			t.Fatalf("stage %s observed %d times, want 1 (all: %v)", stage, got[stage], got)
		}
	}
	if got[IntakeStageJournalSave] == 0 {
		t.Fatalf("no journal_save observed (all: %v)", got)
	}
	for stage := range got {
		if !slices.Contains(IntakeStages, stage) {
			t.Fatalf("unexpected stage %q (all: %v)", stage, got)
		}
	}
}

// TestOrchestrate_ObservesAFailedSubmit: a submit transport error is reported
// as a failed submit stage, and the claim stage never runs.
func TestOrchestrate_ObservesAFailedSubmit(t *testing.T) {
	obs := &recordingStageObserver{}
	prep := &recordingPreparer{prepared: boundSource()}
	sub := &recordingSubmitter{err: errors.New("arbiter down")}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A", StageObserver: obs})
	if _, err := orch.Orchestrate(context.Background(), admissionFixture()); err == nil {
		t.Fatal("Orchestrate succeeded with a failing submitter")
	}
	got := obs.counts()
	if got[IntakeStageSubmit+"!"] != 1 || got[IntakeStagePrepare] != 1 || got[IntakeStageRegisterClaim] != 0 {
		t.Fatalf("stages = %v; want one failed submit, one prepare, no claim", got)
	}
}
