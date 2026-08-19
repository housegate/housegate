package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type retryThenAcceptSubmitter struct {
	retryUntil int64
	calls      int64
}

type failFirstListJournal struct {
	base    IntakeJournal
	err     error
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (j *failFirstListJournal) LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error) {
	return j.base.LoadIntakeRecord(ctx, statementID)
}

func (j *failFirstListJournal) ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error) {
	if j.calls.Add(1) == 1 {
		close(j.started)
		select {
		case <-j.release:
			return nil, j.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return j.base.ListIntakeRecords(ctx)
}

func (j *failFirstListJournal) SaveIntakeRecord(ctx context.Context, record IntakeJournalRecord) error {
	return j.base.SaveIntakeRecord(ctx, record)
}

func (s *retryThenAcceptSubmitter) SubmitStatement(context.Context, StatementEnvelope) (SubmitOutcome, error) {
	if atomic.AddInt64(&s.calls, 1) <= s.retryUntil {
		return SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}, nil
	}
	return SubmitOutcome{Category: OutcomeAccepted}, nil
}

func TestRecoverPendingResumesHolderWithoutClientRetry(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	prep := newLookupRecordingPreparer(boundSource())
	sub := &retryThenAcceptSubmitter{retryUntil: 1}
	orch1 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
	})

	res, err := orch1.Orchestrate(ctx, adm)
	if err != nil {
		t.Fatalf("initial Orchestrate: %v", err)
	}
	if res.Ack2 || res.Submit.Category != OutcomeRetryable {
		t.Fatalf("initial result Ack2/submit = %v/%v, want retryable", res.Ack2, res.Submit.Category)
	}

	orch2 := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
	})
	if err := orch2.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil {
		t.Fatalf("LoadIntakeRecord: %v", err)
	}
	if !ok || !persisted.IsTerminal || persisted.Stage != LifecycleRCBound {
		t.Fatalf("persisted recovery = ok:%v terminal:%v stage:%q, want terminal RCBound", ok, persisted.IsTerminal, persisted.Stage)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare count = %d, want 1", got)
	}
	if len(persisted.Admission.Payload) != 0 {
		t.Fatalf("terminal recovery retained %d payload bytes", len(persisted.Admission.Payload))
	}
	orch2.mu.Lock()
	recoveredHistory := len(orch2.recoveredJournal)
	orch2.mu.Unlock()
	if recoveredHistory != 0 {
		t.Fatalf("recovery retained %d full-journal records after runtime restore", recoveredHistory)
	}
}

func TestRecoverPendingCompactsLegacyTerminalPayload(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := boundSource()
	prepared.StatementID = adm.StatementID
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID: adm.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: adm, Stage: LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: IntakeResult{
			StatementID: adm.StatementID, Ack2: true, Lifecycle: LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	orch := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if err := orch.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if len(persisted.Admission.Payload) != 0 {
		t.Fatalf("legacy terminal retained %d payload bytes after recovery", len(persisted.Admission.Payload))
	}
	if persisted.JournalVersion == 0 {
		t.Fatal("legacy zero-candidate terminal retained version 0")
	}
	if persisted.Admission.TouchedPartitionIDs == nil || len(persisted.Admission.TouchedPartitionIDs) != 0 {
		t.Fatalf("legacy zero-candidate touched partitions=%v, want non-nil empty", persisted.Admission.TouchedPartitionIDs)
	}
	replay := adm
	replay.TouchedPartitionIDs = []string{}
	if res, err := orch.Orchestrate(ctx, replay); err != nil || !res.Ack2 {
		t.Fatalf("migrated zero-candidate terminal replay=(%+v, %v), want cached ACK2", res, err)
	}
}

func TestRecoverPendingRejectsUnknownJournalVersion(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := boundSource()
	prepared.StatementID = adm.StatementID
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		JournalVersion: currentIntakeJournalVersion + 1,
		StatementID:    adm.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: adm, Stage: LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: IntakeResult{
			StatementID: adm.StatementID, Ack2: true, Lifecycle: LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	orch := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	err = orch.RecoverPending(ctx)
	if err == nil || !strings.Contains(err.Error(), "newer than supported") || !strings.Contains(err.Error(), adm.StatementID) {
		t.Fatalf("RecoverPending error=%v, want named unsupported journal version", err)
	}
}

func TestRecoverPendingConcurrentCallsRestoreRuntimeStateOnce(t *testing.T) {
	orch := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A"},
	)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	orch.SetBeforeRecovery(func(context.Context, []IntakeJournalRecord) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	})
	errs := make(chan error, 2)
	go func() { errs <- orch.RecoverPending(context.Background()) }()
	<-started
	secondLaunched := make(chan struct{})
	go func() {
		close(secondLaunched)
		errs <- orch.RecoverPending(context.Background())
	}()
	<-secondLaunched
	deadline := time.NewTimer(30 * time.Millisecond)
	for calls.Load() < 2 {
		select {
		case <-deadline.C:
			goto releaseRestore
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if !deadline.Stop() {
		<-deadline.C
	}

releaseRestore:
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("RecoverPending: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent runtime restore calls=%d, want one", got)
	}
}

func TestRecoverPendingConcurrentRestoreFailureIsSharedByWaiters(t *testing.T) {
	orch := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A"},
	)
	started := make(chan struct{})
	release := make(chan struct{})
	restoreErr := errors.New("restore inventory unavailable")
	var calls atomic.Int64
	orch.SetBeforeRecovery(func(context.Context, []IntakeJournalRecord) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return restoreErr
	})

	errs := make(chan error, 2)
	go func() { errs <- orch.RecoverPending(context.Background()) }()
	<-started
	waiterStarted := make(chan struct{})
	go func() {
		close(waiterStarted)
		errs <- orch.RecoverPending(context.Background())
	}()
	<-waiterStarted
	time.Sleep(10 * time.Millisecond)
	close(release)
	for range 2 {
		if err := <-errs; !errors.Is(err, restoreErr) {
			t.Fatalf("concurrent RecoverPending error=%v, want shared %v", err, restoreErr)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("failed runtime restore calls=%d, want one shared attempt", got)
	}

	orch.SetBeforeRecovery(func(context.Context, []IntakeJournalRecord) error {
		calls.Add(1)
		return nil
	})
	if err := orch.RecoverPending(context.Background()); err != nil {
		t.Fatalf("independent retry after failed cohort: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("runtime restore calls after independent retry=%d, want 2", got)
	}
}

func TestRecoverPendingConcurrentJournalFailureIsSharedByWaiters(t *testing.T) {
	base, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	listErr := errors.New("journal listing unavailable")
	journal := &failFirstListJournal{
		base: base, err: listErr, started: make(chan struct{}), release: make(chan struct{}),
	}
	orch := NewOrchestrator(
		panicSubmitter{t: t}, newLookupRecordingPreparer(boundSource()),
		OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	errs := make(chan error, 2)
	go func() { errs <- orch.RecoverPending(context.Background()) }()
	<-journal.started
	waiterStarted := make(chan struct{})
	go func() {
		close(waiterStarted)
		errs <- orch.RecoverPending(context.Background())
	}()
	<-waiterStarted
	time.Sleep(10 * time.Millisecond)
	close(journal.release)
	for range 2 {
		if err := <-errs; !errors.Is(err, listErr) {
			t.Fatalf("concurrent RecoverPending error=%v, want shared %v", err, listErr)
		}
	}
	if got := journal.calls.Load(); got != 1 {
		t.Fatalf("failed journal recovery calls=%d, want one shared attempt", got)
	}

	if err := orch.RecoverPending(context.Background()); err != nil {
		t.Fatalf("independent retry after failed journal cohort: %v", err)
	}
	if got := journal.calls.Load(); got != 2 {
		t.Fatalf("journal recovery calls after independent retry=%d, want 2", got)
	}
}

func TestRecoverPendingRetriesRetryableOutcomeUntilTerminal(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := boundSource()
	prepared.StatementID = adm.StatementID
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID:     adm.StatementID,
		Source:          "snode-A",
		FrontierOrdinal: 1,
		Env:             env,
		Admission:       adm,
		Stage:           LifecycleUnsafeWritten,
		Prepared:        prepared,
		HasPrepared:     true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	sub := &retryThenAcceptSubmitter{retryUntil: 2}
	orch := NewOrchestrator(sub, newLookupRecordingPreparer(boundSource()), OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
	})

	if err := orch.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	if got := atomic.LoadInt64(&sub.calls); got != 3 {
		t.Fatalf("submit calls = %d, want 3", got)
	}
}

type orderedRecoverySubmitter struct {
	mu    sync.Mutex
	order []string
}

func (s *orderedRecoverySubmitter) SubmitStatement(_ context.Context, env StatementEnvelope) (SubmitOutcome, error) {
	s.mu.Lock()
	s.order = append(s.order, env.StatementID)
	s.mu.Unlock()
	return SubmitOutcome{Category: OutcomeAccepted}, nil
}

func (s *orderedRecoverySubmitter) callOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

type recordingPayloadLeaseManager struct {
	ensured  atomic.Bool
	ensures  int64
	releases int64
}

func (m *recordingPayloadLeaseManager) EnsurePayloadLease(context.Context, AdmissionRecord, string) error {
	m.ensured.Store(true)
	atomic.AddInt64(&m.ensures, 1)
	return nil
}

func (m *recordingPayloadLeaseManager) ReleasePayloadLease(string) {
	atomic.AddInt64(&m.releases, 1)
}

func (m *recordingPayloadLeaseManager) Run(context.Context) {}

type leaseCheckingSubmitter struct {
	lease *recordingPayloadLeaseManager
}

func (s *leaseCheckingSubmitter) SubmitStatement(context.Context, StatementEnvelope) (SubmitOutcome, error) {
	if !s.lease.ensured.Load() {
		return SubmitOutcome{}, errors.New("submit ran before payload lease was ensured")
	}
	return SubmitOutcome{Category: OutcomeAccepted}, nil
}

func TestRecoverPendingRegistersLeaseBeforeSubmit(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := admissionFixture()
	adm.PayloadRef = "payload://store/ref-1"
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := boundSource()
	prepared.StatementID = adm.StatementID
	prepared.PayloadRef = adm.PayloadRef
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID:     adm.StatementID,
		Source:          "snode-A",
		FrontierOrdinal: 1,
		Env:             env,
		Admission:       adm,
		Stage:           LifecycleUnsafeWritten,
		Prepared:        prepared,
		HasPrepared:     true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	lease := &recordingPayloadLeaseManager{}
	orch := NewOrchestrator(&leaseCheckingSubmitter{lease: lease}, newLookupRecordingPreparer(boundSource()), OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
		PayloadLeaseManager:   lease,
	})

	if err := orch.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	if got := atomic.LoadInt64(&lease.ensures); got != 1 {
		t.Fatalf("lease ensures = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&lease.releases); got != 1 {
		t.Fatalf("lease releases = %d, want 1 after durable submit acceptance", got)
	}
}

func TestRecoverPendingDrainsAThenBInOrdinalOrder(t *testing.T) {
	ctx := context.Background()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	admissions := []AdmissionRecord{admissionFixture(), admissionFixture()}
	admissions[1].StatementID = fixtureStatementID(2)
	for idx, adm := range admissions {
		env, err := EnvelopeFromAdmission(adm)
		if err != nil {
			t.Fatalf("EnvelopeFromAdmission %d: %v", idx, err)
		}
		prepared := boundSource()
		prepared.StatementID = adm.StatementID
		if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
			StatementID:     adm.StatementID,
			Source:          "snode-A",
			FrontierOrdinal: uint64(idx + 1),
			Env:             env,
			Admission:       adm,
			Stage:           LifecycleUnsafeWritten,
			Prepared:        prepared,
			HasPrepared:     true,
		}); err != nil {
			t.Fatalf("SaveIntakeRecord %d: %v", idx, err)
		}
	}
	sub := &orderedRecoverySubmitter{}
	orch := NewOrchestrator(sub, newLookupRecordingPreparer(boundSource()), OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
	})

	if err := orch.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	order := sub.callOrder()
	if len(order) != 2 || order[0] != admissions[0].StatementID || order[1] != admissions[1].StatementID {
		t.Fatalf("submit order = %v, want [%s %s]", order, admissions[0].StatementID, admissions[1].StatementID)
	}
}

func TestRecoverPendingDrainsKnownUnwrittenHolderBeforeLaterUnsafeRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)
	envA, err := EnvelopeFromAdmission(admA)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission A: %v", err)
	}
	envB, err := EnvelopeFromAdmission(admB)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission B: %v", err)
	}
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID:           admA.StatementID,
		Source:                "snode-A",
		FrontierOrdinal:       1,
		Env:                   envA,
		Admission:             admA,
		Stage:                 LifecyclePreparing,
		PrepareKnownUnwritten: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord A: %v", err)
	}
	preparedB := boundSource()
	preparedB.StatementID = admB.StatementID
	if err := journal.SaveIntakeRecord(ctx, IntakeJournalRecord{
		StatementID:     admB.StatementID,
		Source:          "snode-A",
		FrontierOrdinal: 2,
		Env:             envB,
		Admission:       admB,
		Stage:           LifecycleUnsafeWritten,
		Prepared:        preparedB,
		HasPrepared:     true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord B: %v", err)
	}

	sub := &orderedRecoverySubmitter{}
	orch := NewOrchestrator(sub, newLookupRecordingPreparer(boundSource()), OrchestratorConfig{
		ExpectedSource:        "snode-A",
		Journal:               journal,
		RecoveryRetryInterval: time.Millisecond,
	})
	if err := orch.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending mixed known-unwritten/unsafe records: %v", err)
	}
	order := sub.callOrder()
	if len(order) != 2 || order[0] != admA.StatementID || order[1] != admB.StatementID {
		t.Fatalf("submit order = %v, want safe-retry A then unsafe B", order)
	}
}
