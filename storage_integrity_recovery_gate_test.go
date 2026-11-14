package housegate

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// gateSubmitter answers retryable until the source is ready, the way an
// arbiter answers a source that cannot serve its statement yet.
type gateSubmitter struct {
	ready atomic.Bool
	calls atomic.Int64
}

func (s *gateSubmitter) SubmitStatement(context.Context, sicore.StatementEnvelope) (sicore.SubmitOutcome, error) {
	s.calls.Add(1)
	if !s.ready.Load() {
		return sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "source not ready"}, nil
	}
	return sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}, nil
}

type gatePreparer struct {
	mu       sync.Mutex
	prepares int
	claims   int
}

func (p *gatePreparer) PrepareLocalStatement(_ context.Context, env sicore.StatementEnvelope, _ []byte) (sicore.PreparedLocalResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prepares++
	return sicore.PreparedLocalResult{
		StatementID: env.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision, Lifecycle: sicore.LifecycleUnsafeWritten,
	}, nil
}

func (p *gatePreparer) RegisterPreparedClaim(context.Context, string) (sicore.ClaimOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.claims++
	return sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *gatePreparer) AbortPreparedStatement(context.Context, string, []sicore.CandidatePart, string) error {
	return nil
}

func (p *gatePreparer) claimCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.claims
}

// pendingGateIngress persists one pending intake (retryable submit) and returns
// a restarted ingress over the same journal, as after a host restart.
func pendingGateIngress(t *testing.T) (*StorageIntegrityIngress, *gateSubmitter, *gatePreparer, sicore.IntakeJournal, string) {
	t.Helper()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sub := &gateSubmitter{}
	prep := &gatePreparer{}
	adm := AdmissionRecordFromPlugin(bpEUAdmission())
	first := sicore.NewOrchestrator(sub, prep, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	if res, err := first.Orchestrate(context.Background(), adm); err != nil || res.Ack2 {
		t.Fatalf("first Orchestrate = %+v, %v; want a pending intake", res, err)
	}
	restarted := sicore.NewOrchestrator(sub, prep, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal, RecoveryRetryInterval: 5 * time.Millisecond})
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingress.Close)
	return ingress, sub, prep, journal, adm.StatementID
}

// TestStartStorageIntegrityRuntime_RecoveryGateDefersTheDrain is the startup
// deadlock of an embedding host whose source becomes ready only after its
// listeners are up: with a recovery gate, preServe restores the pending record
// and returns at once, and the drain runs once the host opens the gate.
func TestStartStorageIntegrityRuntime_RecoveryGateDefersTheDrain(t *testing.T) {
	ingress, sub, prep, journal, id := pendingGateIngress(t)
	gate := make(chan struct{})
	opened := func(ctx context.Context) error {
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	submitsBefore := sub.calls.Load()
	done := make(chan error, 1)
	go func() { done <- startStorageIntegrityRuntime(ctx, ingress, nil, false, opened) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("startStorageIntegrityRuntime: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preServe blocked on the recovery drain although a recovery gate is set")
	}
	time.Sleep(50 * time.Millisecond)
	if got := sub.calls.Load(); got != submitsBefore {
		t.Fatalf("the drain touched the arbiter before the gate opened (%d -> %d submits)", submitsBefore, got)
	}

	close(gate)
	time.Sleep(50 * time.Millisecond)
	sub.ready.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok, err := journal.LoadIntakeRecord(context.Background(), id); err == nil && ok && rec.IsTerminal {
			if rec.Stage != sicore.LifecycleRCBound || prep.claimCount() != 1 {
				t.Fatalf("drained record stage %q claims %d, want RCBound and one claim", rec.Stage, prep.claimCount())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the background drain did not finish after the gate opened")
}

// Without a gate, preServe still drains before it returns (the standalone
// contract), and a gate that never opens leaves Close able to join the drain.
func TestStartStorageIntegrityRuntime_NoGateDrainsBeforeReturning(t *testing.T) {
	ingress, sub, _, journal, id := pendingGateIngress(t)
	sub.ready.Store(true)
	if err := startStorageIntegrityRuntime(context.Background(), ingress, nil, false, nil); err != nil {
		t.Fatalf("startStorageIntegrityRuntime: %v", err)
	}
	if rec, ok, err := journal.LoadIntakeRecord(context.Background(), id); err != nil || !ok || !rec.IsTerminal {
		t.Fatalf("record after a gateless start = terminal:%v ok:%v err:%v, want terminal", rec.IsTerminal, ok, err)
	}
}

func TestStartStorageIntegrityRuntime_CloseJoinsADrainWaitingOnTheGate(t *testing.T) {
	ingress, _, _, _, _ := pendingGateIngress(t)
	never := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	if err := startStorageIntegrityRuntime(context.Background(), ingress, nil, false, never); err != nil {
		t.Fatalf("startStorageIntegrityRuntime: %v", err)
	}
	closed := make(chan struct{})
	go func() { ingress.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while the drain waited on the gate")
	}
}

func TestRecoveryRetryLoggerRateLimitsPerStatement(t *testing.T) {
	now := time.Unix(0, 0)
	l := newRecoveryRetryLogger(func() time.Time { return now }, 30*time.Second)
	logged := func(id string) bool {
		before := l.last[id]
		l.observe(id, sicore.IntakeResult{}, nil)
		return !l.last[id].Equal(before) || before.IsZero()
	}
	if !logged("a") {
		t.Fatal("the first pending attempt is logged")
	}
	now = now.Add(10 * time.Second)
	if logged("a") {
		t.Fatal("a second attempt inside the interval is not logged")
	}
	if !logged("b") {
		t.Fatal("another statement is logged on its own schedule")
	}
	now = now.Add(25 * time.Second)
	if !logged("a") {
		t.Fatal("an attempt after the interval is logged again")
	}
	if l.count["a"] != 3 {
		t.Fatalf("attempts counted = %d, want 3", l.count["a"])
	}
}
