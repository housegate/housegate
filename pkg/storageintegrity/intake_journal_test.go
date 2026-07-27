package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"sync"
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

type preparedSaveFailJournal struct {
	base      IntakeJournal
	failUntil int64
	err       error
}

func (j *preparedSaveFailJournal) LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error) {
	return j.base.LoadIntakeRecord(ctx, statementID)
}

func (j *preparedSaveFailJournal) SaveIntakeRecord(ctx context.Context, rec IntakeJournalRecord) error {
	if rec.HasPrepared && atomic.AddInt64(&j.failUntil, -1) >= 0 {
		return j.err
	}
	return j.base.SaveIntakeRecord(ctx, rec)
}

type lookupRecordingPreparer struct {
	prepared     PreparedLocalResult
	claimOutcome ClaimOutcome

	mu                  sync.Mutex
	preparedByStatement map[string]PreparedLocalResult
	prepareCount        int64
	lookupCount         int64
}

func newLookupRecordingPreparer(prepared PreparedLocalResult) *lookupRecordingPreparer {
	return &lookupRecordingPreparer{
		prepared:            prepared,
		claimOutcome:        ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
		preparedByStatement: map[string]PreparedLocalResult{},
	}
}

func (p *lookupRecordingPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	p.mu.Lock()
	p.preparedByStatement[env.StatementID] = clonePreparedLocalResult(res)
	p.mu.Unlock()
	return res, nil
}

func (p *lookupRecordingPreparer) LookupPreparedStatement(_ context.Context, statementID string) (PreparedLocalResult, bool, error) {
	atomic.AddInt64(&p.lookupCount, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	res, ok := p.preparedByStatement[statementID]
	return clonePreparedLocalResult(res), ok, nil
}

func (p *lookupRecordingPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return p.claimOutcome, nil
}

func (p *lookupRecordingPreparer) AbortPreparedStatement(context.Context, string, []CandidatePart, string) error {
	return nil
}

func TestFileIntakeJournalPreparedSaveFailureRestartsViaSourceLookup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	adm := admissionFixture()

	baseJournal, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	diskFull := errors.New("journal disk full")
	journal1 := &preparedSaveFailJournal{base: baseJournal, failUntil: 1, err: diskFull}
	prep := newLookupRecordingPreparer(boundSource())
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch1 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal1,
	})

	res, err := orch1.Orchestrate(ctx, adm)
	if err == nil || !strings.Contains(err.Error(), diskFull.Error()) {
		t.Fatalf("first Orchestrate err = %v, want prepared journal save failure", err)
	}
	if res.Prepared.StatementID != adm.StatementID {
		t.Fatalf("first result prepared statement = %q, want %q", res.Prepared.StatementID, adm.StatementID)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("first attempt prepare count = %d, want 1", got)
	}

	journal2, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal after restart: %v", err)
	}
	orch2 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal2,
	})

	res2, err := orch2.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("restart Orchestrate: %v", err)
	}
	if !res2.Ack2 || res2.Lifecycle != LifecycleRCBound {
		t.Fatalf("restart result Ack2/lifecycle = %v/%q, want ACK2/RCBound", res2.Ack2, res2.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("restart must not re-run PrepareLocalStatement, got %d prepare calls", got)
	}
	if got := atomic.LoadInt64(&prep.lookupCount); got != 1 {
		t.Fatalf("restart lookup count = %d, want 1", got)
	}
}
