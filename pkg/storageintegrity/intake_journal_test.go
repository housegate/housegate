package storageintegrity

import (
	"context"
	"sync/atomic"
	"testing"
)

type panicSubmitter struct {
	t *testing.T
}

func (s panicSubmitter) SubmitStatement(context.Context, StatementEnvelope) (SubmitOutcome, error) {
	s.t.Fatal("SubmitStatement must not run when journal resumes AbortPending")
	return SubmitOutcome{}, nil
}

func TestFileIntakeJournalResumesAbortPendingAcrossOrchestrators(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	adm := admissionFixture()

	journal1, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	prep1 := &partsRecordingPreparer{prepared: boundSourceWithParts(), failUntil: 1}
	sub1 := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch1 := NewOrchestrator(sub1, prep1, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal1,
	})

	res, err := orch1.Orchestrate(ctx, adm)
	if err == nil {
		t.Fatal("first attempt must surface the failed abort")
	}
	if res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("first lifecycle = %q, want AbortPending", res.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep1.prepareCount); got != 1 {
		t.Fatalf("first attempt prepare count = %d, want 1", got)
	}

	journal2, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal second open: %v", err)
	}
	prep2 := &partsRecordingPreparer{}
	orch2 := NewOrchestrator(panicSubmitter{t: t}, prep2, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal2,
	})

	res2, err := orch2.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("resume after restart: %v", err)
	}
	if res2.Lifecycle != LifecycleCleaned {
		t.Fatalf("resume lifecycle = %q, want Cleaned", res2.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep2.prepareCount); got != 0 {
		t.Fatalf("resume must not re-run prepare, got %d prepare calls", got)
	}
	if got := atomic.LoadInt64(&prep2.abortCalls); got != 1 {
		t.Fatalf("resume must run one abort, got %d calls", got)
	}
	gotParts := partNameSet(prep2.lastAbortedParts())
	wantParts := partNameSet(candidateFixture())
	if len(gotParts) != len(wantParts) {
		t.Fatalf("resume abort targeted %d parts, want %d", len(gotParts), len(wantParts))
	}
	for name := range wantParts {
		if !gotParts[name] {
			t.Fatalf("resume abort missed frozen part %q", name)
		}
	}
}
