package storageintegrity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
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

func (j *preparedSaveFailJournal) ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error) {
	return j.base.ListIntakeRecords(ctx)
}

func (j *preparedSaveFailJournal) SaveIntakeRecord(ctx context.Context, rec IntakeJournalRecord) error {
	if rec.HasPrepared && atomic.AddInt64(&j.failUntil, -1) >= 0 {
		return j.err
	}
	return j.base.SaveIntakeRecord(ctx, rec)
}

type failFirstInitialSaveJournal struct {
	base  IntakeJournal
	err   error
	saves int64
}

func (j *failFirstInitialSaveJournal) LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error) {
	return j.base.LoadIntakeRecord(ctx, statementID)
}

func (j *failFirstInitialSaveJournal) ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error) {
	return j.base.ListIntakeRecords(ctx)
}

func (j *failFirstInitialSaveJournal) SaveIntakeRecord(ctx context.Context, rec IntakeJournalRecord) error {
	n := atomic.AddInt64(&j.saves, 1)
	if n == 1 && rec.Stage == LifecyclePreparing && !rec.HasPrepared {
		return j.err
	}
	return j.base.SaveIntakeRecord(ctx, rec)
}

func (j *failFirstInitialSaveJournal) saveCount() int64 {
	return atomic.LoadInt64(&j.saves)
}

type failFirstTerminalSaveJournal struct {
	base          IntakeJournal
	err           error
	terminalSaves int64
}

func (j *failFirstTerminalSaveJournal) LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error) {
	return j.base.LoadIntakeRecord(ctx, statementID)
}

func (j *failFirstTerminalSaveJournal) ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error) {
	return j.base.ListIntakeRecords(ctx)
}

func (j *failFirstTerminalSaveJournal) SaveIntakeRecord(ctx context.Context, rec IntakeJournalRecord) error {
	if rec.IsTerminal && atomic.AddInt64(&j.terminalSaves, 1) == 1 {
		return j.err
	}
	return j.base.SaveIntakeRecord(ctx, rec)
}

func TestOrchestrate_TerminalSaveFailureDoesNotPublishMemoryTerminal(t *testing.T) {
	ctx := context.Background()
	baseJournal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	saveErr := errors.New("terminal journal save failed")
	journal := &failFirstTerminalSaveJournal{base: baseJournal, err: saveErr}
	prep := newLookupRecordingPreparer(boundSource())
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})
	adm := admissionFixture()

	res, err := orch.Orchestrate(ctx, adm)
	if err == nil || !strings.Contains(err.Error(), saveErr.Error()) {
		t.Fatalf("first Orchestrate err = %v, want terminal save failure", err)
	}
	if !res.Ack2 {
		t.Fatal("first result should describe the computed ACK2 even though it was not published")
	}

	res, err = orch.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("retry Orchestrate: %v", err)
	}
	if !res.Ack2 || res.Lifecycle != LifecycleRCBound {
		t.Fatalf("retry result Ack2/lifecycle = %v/%q, want ACK2/RCBound", res.Ack2, res.Lifecycle)
	}
	if got := atomic.LoadInt64(&journal.terminalSaves); got != 2 {
		t.Fatalf("terminal save attempts = %d, want 2", got)
	}
	persisted, ok, err := baseJournal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil {
		t.Fatalf("LoadIntakeRecord: %v", err)
	}
	if !ok || !persisted.IsTerminal {
		t.Fatalf("persisted terminal = %v/%v, want present terminal record", ok, persisted.IsTerminal)
	}
}

func TestFileIntakeJournalCompactsTerminalPayloadAndReplaysFromEnvelope(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	prep := newLookupRecordingPreparer(boundSource())
	orch := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}, prep,
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	res, err := orch.Orchestrate(ctx, adm)
	if err != nil || !res.Ack2 {
		t.Fatalf("Orchestrate=(%+v, %v), want ACK2", res, err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil {
		t.Fatalf("LoadIntakeRecord: %v", err)
	}
	if !ok || !persisted.IsTerminal {
		t.Fatalf("persisted terminal=%v/%v, want present terminal", ok, persisted.IsTerminal)
	}
	if len(persisted.Admission.Payload) != 0 {
		t.Fatalf("terminal journal retained %d payload bytes", len(persisted.Admission.Payload))
	}

	restartedPrep := newLookupRecordingPreparer(boundSource())
	restarted := NewOrchestrator(panicSubmitter{t: t}, restartedPrep, OrchestratorConfig{
		ExpectedSource: "snode-A", Journal: journal,
	})
	replayed, err := restarted.Orchestrate(ctx, adm)
	if err != nil || !replayed.Ack2 {
		t.Fatalf("cached terminal replay=(%+v, %v), want ACK2", replayed, err)
	}
	if got := atomic.LoadInt64(&restartedPrep.prepareCount); got != 0 {
		t.Fatalf("cached terminal replay prepared %d times", got)
	}
}

func TestFileIntakeJournalListCompactsLegacyTerminalPayloadBeforeRetainingHistory(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		adm := admissionFixture()
		adm.StatementID = fixtureStatementID(seq)
		adm.Payload = []byte(strings.Repeat(string(rune('a'+seq)), 1<<20))
		adm.PayloadLength = uint64(len(adm.Payload))
		adm.PayloadHash = replay.DigestBytes(adm.Payload)
		env, err := EnvelopeFromAdmission(adm)
		if err != nil {
			t.Fatalf("EnvelopeFromAdmission %d: %v", seq, err)
		}
		prepared := boundSource()
		prepared.StatementID = adm.StatementID
		prepared.PayloadRef = adm.PayloadHash
		prepared.PayloadHash = adm.PayloadHash
		prepared.PayloadLength = adm.PayloadLength
		if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
			StatementID: adm.StatementID, Source: "snode-A", FrontierOrdinal: seq,
			Env: env, Admission: adm, Stage: LifecycleRCBound,
			Prepared: prepared, HasPrepared: true,
			TerminalResult: IntakeResult{
				StatementID: adm.StatementID, Ack2: true, Lifecycle: LifecycleRCBound,
				Prepared: prepared,
			},
			IsTerminal: true,
		}); err != nil {
			t.Fatalf("SaveIntakeRecord %d: %v", seq, err)
		}
	}

	records, err := journal.ListIntakeRecords(ctx)
	if err != nil {
		t.Fatalf("ListIntakeRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records=%d, want 2", len(records))
	}
	for _, record := range records {
		if len(record.Admission.Payload) != 0 {
			t.Fatalf("listed terminal %s retained %d legacy payload bytes", record.StatementID, len(record.Admission.Payload))
		}
		persisted, ok, err := journal.LoadIntakeRecord(ctx, record.StatementID)
		if err != nil || !ok {
			t.Fatalf("LoadIntakeRecord %s=(%+v, %v, %v)", record.StatementID, persisted, ok, err)
		}
		if len(persisted.Admission.Payload) != 0 {
			t.Fatalf("persisted terminal %s retained %d legacy payload bytes", record.StatementID, len(persisted.Admission.Payload))
		}
	}
}

func TestObservedMarkerCannotRegressDurableTerminalRecord(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	prepared := boundSourceWithParts()
	prep := &partsRecordingPreparer{
		prepared:     prepared,
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	orch := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}, prep,
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	adm := admissionFixture()
	res, err := orch.Orchestrate(ctx, adm)
	if err != nil || !res.Ack2 {
		t.Fatalf("Orchestrate=(%+v, %v), want ACK2", res, err)
	}

	// Recreate the only state window that matters: terminal is already durable,
	// while an observation writer still holds the pre-publish in-memory view.
	orch.mu.Lock()
	rec := orch.records[adm.StatementID]
	rec.isTerminal = false
	rec.stage = LifecycleRCBound
	rec.adm.Payload = append([]byte(nil), adm.Payload...)
	orch.mu.Unlock()
	if err := orch.MarkCandidateObserved(ctx, adm.StatementID, prepared.CandidateParts[0]); err != nil {
		t.Fatalf("MarkCandidateObserved: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if !persisted.IsTerminal || !persisted.TerminalResult.Ack2 || len(persisted.Admission.Payload) != 0 {
		t.Fatalf("observed marker regressed durable terminal: terminal=%v ack2=%v payload=%d", persisted.IsTerminal, persisted.TerminalResult.Ack2, len(persisted.Admission.Payload))
	}
}

func TestOrchestrate_TerminalSaveFailureRestartsFromDurableStage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	baseJournal, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	saveErr := errors.New("terminal journal save failed")
	journal := &failFirstTerminalSaveJournal{base: baseJournal, err: saveErr}
	prep := newLookupRecordingPreparer(boundSource())
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})
	adm := admissionFixture()

	res, err := orch.Orchestrate(ctx, adm)
	if err == nil || !strings.Contains(err.Error(), saveErr.Error()) {
		t.Fatalf("first Orchestrate err = %v, want terminal save failure", err)
	}
	if !res.Ack2 || res.Lifecycle != LifecycleRCBound {
		t.Fatalf("first result Ack2/lifecycle = %v/%q, want computed ACK2/RCBound", res.Ack2, res.Lifecycle)
	}
	persisted, ok, err := baseJournal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil {
		t.Fatalf("LoadIntakeRecord after failed terminal save: %v", err)
	}
	if !ok || persisted.IsTerminal || persisted.Stage != LifecycleRCBound {
		t.Fatalf("persisted after failed terminal save = ok:%v terminal:%v stage:%q, want non-terminal RCBound", ok, persisted.IsTerminal, persisted.Stage)
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
	persisted, ok, err = journal2.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil {
		t.Fatalf("LoadIntakeRecord after restart: %v", err)
	}
	if !ok || !persisted.IsTerminal || persisted.Stage != LifecycleRCBound {
		t.Fatalf("persisted after restart = ok:%v terminal:%v stage:%q, want terminal RCBound", ok, persisted.IsTerminal, persisted.Stage)
	}
}

type initialPersistRetryPreparer struct {
	prepared     PreparedLocalResult
	journalSaves func() int64
	prepareCount int64
}

func (p *initialPersistRetryPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	if saves := p.journalSaves(); saves < 2 {
		return PreparedLocalResult{}, errors.New("prepare ran before retrying the initial journal save")
	}
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *initialPersistRetryPreparer) RegisterPreparedClaim(context.Context, string) (ClaimOutcome, error) {
	return ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *initialPersistRetryPreparer) AbortPreparedStatement(context.Context, string, []CandidatePart, string) error {
	return nil
}

func TestFileIntakeJournalInitialPersistFailureRetriesBeforePrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	baseJournal, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	persistErr := errors.New("initial journal persist failed")
	journal := &failFirstInitialSaveJournal{base: baseJournal, err: persistErr}
	prep := &initialPersistRetryPreparer{
		prepared:     boundSource(),
		journalSaves: journal.saveCount,
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})
	adm := admissionFixture()

	_, err = orch.Orchestrate(ctx, adm)
	if err == nil || !strings.Contains(err.Error(), persistErr.Error()) {
		t.Fatalf("first Orchestrate err = %v, want initial persist failure", err)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 0 {
		t.Fatalf("prepare ran after failed initial persist, got %d calls", got)
	}

	res, err := orch.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("retry Orchestrate: %v", err)
	}
	if !res.Ack2 || res.Lifecycle != LifecycleRCBound {
		t.Fatalf("retry result Ack2/lifecycle = %v/%q, want ACK2/RCBound", res.Ack2, res.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare count after retry = %d, want 1", got)
	}
	if got := journal.saveCount(); got < 2 {
		t.Fatalf("journal saves before retry prepare = %d, want at least 2", got)
	}
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

type ambiguousPreparePreparer struct {
	*lookupRecordingPreparer
	failOnce int64
}

func (p *ambiguousPreparePreparer) PrepareLocalStatement(ctx context.Context, env StatementEnvelope, payload []byte) (PreparedLocalResult, error) {
	res, err := p.lookupRecordingPreparer.PrepareLocalStatement(ctx, env, payload)
	if err != nil {
		return PreparedLocalResult{}, err
	}
	if atomic.CompareAndSwapInt64(&p.failOnce, 1, 0) {
		return PreparedLocalResult{}, errors.New("prepare response lost after durable source write")
	}
	return res, nil
}

func TestOrchestrate_AmbiguousPrepareErrorRequiresLookupBeforeRetry(t *testing.T) {
	ctx := context.Background()
	base := newLookupRecordingPreparer(boundSource())
	prep := &ambiguousPreparePreparer{lookupRecordingPreparer: base, failOnce: 1}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})
	adm := admissionFixture()

	if _, err = orch.Orchestrate(ctx, adm); err == nil || !strings.Contains(err.Error(), "prepare response lost") {
		t.Fatalf("first Orchestrate err = %v, want ambiguous prepare failure", err)
	}
	res, err := orch.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("retry Orchestrate: %v", err)
	}
	if !res.Ack2 || res.Lifecycle != LifecycleRCBound {
		t.Fatalf("retry result Ack2/lifecycle = %v/%q, want ACK2/RCBound", res.Ack2, res.Lifecycle)
	}
	if got := atomic.LoadInt64(&base.prepareCount); got != 1 {
		t.Fatalf("prepare count = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&base.lookupCount); got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
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

type restartFrontierSubmitter struct {
	aID    string
	aCalls int64
}

func (s *restartFrontierSubmitter) SubmitStatement(_ context.Context, env StatementEnvelope) (SubmitOutcome, error) {
	if env.StatementID == s.aID && atomic.AddInt64(&s.aCalls, 1) == 1 {
		return SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}, nil
	}
	return SubmitOutcome{Category: OutcomeAccepted}, nil
}

func TestFileIntakeJournalRecoveryFencesNonTerminalSourceBeforeNextStatement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)

	journal1, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	sub := &restartFrontierSubmitter{aID: admA.StatementID}
	orch1 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal1,
	})

	resA1, err := orch1.Orchestrate(ctx, admA)
	if err != nil {
		t.Fatalf("A first Orchestrate: %v", err)
	}
	if resA1.Ack2 || resA1.Submit.Category != OutcomeRetryable {
		t.Fatalf("A first result Ack2/submit = %v/%v, want non-terminal retryable", resA1.Ack2, resA1.Submit.Category)
	}
	if got := prep.prepareCountFor(admA.StatementID); got != 1 {
		t.Fatalf("A first prepare count = %d, want 1", got)
	}

	journal2, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal after restart: %v", err)
	}
	orch2 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal2,
	})

	bDone := make(chan error, 1)
	go func() {
		_, err := orch2.Orchestrate(ctx, admB)
		bDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if got := prep.prepareCountFor(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times before non-terminal A resumed; restart recovery must fence snode-A", got)
	}

	resA2, err := orch2.Orchestrate(ctx, admA)
	if err != nil {
		t.Fatalf("A retry after restart: %v", err)
	}
	if !resA2.Ack2 || resA2.Lifecycle != LifecycleRCBound {
		t.Fatalf("A retry result Ack2/lifecycle = %v/%q, want ACK2/RCBound", resA2.Ack2, resA2.Lifecycle)
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B Orchestrate after A terminal: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B did not resume after A reached terminal")
	}
	if got := prep.prepareCountFor(admB.StatementID); got != 1 {
		t.Fatalf("B prepare count after A terminal = %d, want 1", got)
	}
}

func TestFileIntakeJournalListIgnoresTempWriteFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal, err := NewFileIntakeJournal(dir)
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID: adm.StatementID,
		Source:      "snode-A",
		Env:         env,
		Admission:   adm,
		Stage:       LifecycleUnsafeWritten,
		Prepared:    boundSource(),
		HasPrepared: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp-intake-orphan.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write orphan temp: %v", err)
	}

	records, err := journal.ListIntakeRecords(ctx)
	if err != nil {
		t.Fatalf("ListIntakeRecords with orphan temp: %v", err)
	}
	if len(records) != 1 || records[0].StatementID != adm.StatementID {
		t.Fatalf("listed records = %#v, want only %s", records, adm.StatementID)
	}
}

func TestFileIntakeJournalOrdersNonTerminalRecordsByFrontierOrdinal(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	a := IntakeJournalRecord{
		StatementID:     fixtureStatementID(1),
		Source:          "snode-A",
		Stage:           LifecyclePreparing,
		FrontierOrdinal: 1,
	}
	b := IntakeJournalRecord{
		StatementID:     fixtureStatementID(2),
		Source:          "snode-A",
		Stage:           LifecyclePreparing,
		FrontierOrdinal: 2,
	}
	if err := journal.SaveIntakeRecord(ctx, a); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := journal.SaveIntakeRecord(ctx, b); err != nil {
		t.Fatalf("save B: %v", err)
	}
	if err := journal.SaveIntakeRecord(ctx, a); err != nil {
		t.Fatalf("update A: %v", err)
	}

	records, err := journal.ListIntakeRecords(ctx)
	if err != nil {
		t.Fatalf("ListIntakeRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want 2", len(records))
	}
	if records[0].StatementID != a.StatementID || records[1].StatementID != b.StatementID {
		t.Fatalf("recovery order = [%s %s], want [%s %s]", records[0].StatementID, records[1].StatementID, a.StatementID, b.StatementID)
	}
}

func TestOrchestratorPersistsFrontierOrdinalBeforeSourceWrite(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	prep := newLookupRecordingPreparer(boundSource())
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})
	a := admissionFixture()
	b := admissionFixture()
	b.StatementID = fixtureStatementID(2)

	if _, err := orch.Orchestrate(ctx, a); err != nil {
		t.Fatalf("Orchestrate A: %v", err)
	}
	if _, err := orch.Orchestrate(ctx, b); err != nil {
		t.Fatalf("Orchestrate B: %v", err)
	}
	aRecord, ok, err := journal.LoadIntakeRecord(ctx, a.StatementID)
	if err != nil || !ok {
		t.Fatalf("load A record: ok=%v err=%v", ok, err)
	}
	bRecord, ok, err := journal.LoadIntakeRecord(ctx, b.StatementID)
	if err != nil || !ok {
		t.Fatalf("load B record: ok=%v err=%v", ok, err)
	}
	if aRecord.FrontierOrdinal != 1 || bRecord.FrontierOrdinal != 2 {
		t.Fatalf("frontier ordinals = A:%d B:%d, want 1/2", aRecord.FrontierOrdinal, bRecord.FrontierOrdinal)
	}
}

func TestOrchestratorRecoveryRejectsMultipleLegacyRecordsWithoutOrdinal(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	for i := 1; i <= 2; i++ {
		adm := admissionFixture()
		adm.StatementID = fixtureStatementID(uint64(i))
		env, err := EnvelopeFromAdmission(adm)
		if err != nil {
			t.Fatalf("EnvelopeFromAdmission %d: %v", i, err)
		}
		if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
			StatementID: adm.StatementID,
			Source:      "snode-A",
			Env:         env,
			Admission:   adm,
			Stage:       LifecyclePreparing,
		}); err != nil {
			t.Fatalf("save legacy record %d: %v", i, err)
		}
	}
	orch := NewOrchestrator(&recordingSubmitter{}, newLookupRecordingPreparer(boundSource()), OrchestratorConfig{
		ExpectedSource: "snode-A",
		Journal:        journal,
	})

	err = orch.ensureJournalRecovered(ctx)
	if err == nil || !strings.Contains(err.Error(), "frontier ordinal") {
		t.Fatalf("ensureJournalRecovered err = %v, want ambiguous legacy frontier error", err)
	}
}

func TestOrchestratorTerminalWithCandidatesAndUnknownTouchedPartitionsStaysLegacy(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	prepared := boundSourceWithParts()
	prep := &recordingPreparer{
		prepared: prepared,
		claimOutcome: ClaimOutcome{
			Category: OutcomeAccepted, BoundSource: "snode-A",
		},
	}
	orch := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}, prep,
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	res, err := orch.Orchestrate(ctx, adm)
	if err != nil || !res.Ack2 {
		t.Fatalf("Orchestrate=(%+v, %v), want ACK2", res, err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.Admission.TouchedPartitionIDs != nil {
		t.Fatalf("unknown touched partitions=%v, want nil", persisted.Admission.TouchedPartitionIDs)
	}
	if persisted.JournalVersion != 0 {
		t.Fatalf("incomplete terminal journal version=%d, want legacy version 0", persisted.JournalVersion)
	}
	for _, candidate := range prepared.CandidateParts {
		if err := orch.MarkCandidateObserved(ctx, adm.StatementID, candidate); err != nil {
			t.Fatalf("MarkCandidateObserved(%s): %v", candidate.PartName, err)
		}
	}
	persisted, ok, err = journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord after observations=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.JournalVersion != 0 {
		t.Fatalf("observed but partition-incomplete terminal journal version=%d, want 0", persisted.JournalVersion)
	}
}

func TestOrchestratorKnownEmptyTouchedPartitionsSurviveTerminalReplay(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	adm.TouchedPartitionIDs = []string{}
	first := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}},
		&recordingPreparer{
			prepared: boundSource(),
			claimOutcome: ClaimOutcome{
				Category: OutcomeAccepted, BoundSource: "snode-A",
			},
		},
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(ctx, adm); err != nil || !res.Ack2 {
		t.Fatalf("first Orchestrate=(%+v, %v), want ACK2", res, err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.Admission.TouchedPartitionIDs == nil || len(persisted.Admission.TouchedPartitionIDs) != 0 {
		t.Fatalf("persisted known-empty touched partitions=%v, want non-nil empty", persisted.Admission.TouchedPartitionIDs)
	}
	if persisted.JournalVersion == 0 {
		t.Fatal("known-complete zero-candidate terminal retained legacy journal version")
	}

	restarted := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := restarted.Orchestrate(ctx, adm); err != nil || !res.Ack2 {
		t.Fatalf("terminal replay=(%+v, %v), want cached ACK2", res, err)
	}
}
