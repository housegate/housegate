package storageintegrity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type retryThenAcceptSubmitter struct {
	retryUntil int64
	calls      int64
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
