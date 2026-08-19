package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// candidateFixture returns two exact candidate parts in one partition, the unit
// of terminal-reject cleanup. Both belong to the admissionFixture table.
func candidateFixture() []CandidatePart {
	return []CandidatePart{
		{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_1_1_0", PartRowLtHash: "lt-a", RowCount: 3, Bytes: 128},
		{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_2_2_0", PartRowLtHash: "lt-b", RowCount: 2, Bytes: 96},
	}
}

// boundSourceWithParts is boundSource() plus a frozen exact candidate-part
// inventory, so an abort has a concrete exact set to target.
func boundSourceWithParts() PreparedLocalResult {
	p := boundSource()
	p.CandidateParts = candidateFixture()
	return p
}

// partsRecordingPreparer records the exact CandidateParts handed to each
// AbortPreparedStatement call, so a test can assert cleanup targets exactly the
// journal's frozen parts and nothing partition-wide. It is a HouseGate-local
// test double, not a companion-seam substitute.
type partsRecordingPreparer struct {
	prepared     PreparedLocalResult
	claimOutcome ClaimOutcome

	mu           sync.Mutex
	abortedParts [][]CandidatePart // one entry per AbortPreparedStatement call
	abortErr     error
	failUntil    int64 // fail the first N abort calls
	abortCalls   int64
	prepareCount int64
}

func (p *partsRecordingPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *partsRecordingPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return p.claimOutcome, nil
}

func (p *partsRecordingPreparer) AbortPreparedStatement(_ context.Context, _ string, parts []CandidatePart, _ string) error {
	n := atomic.AddInt64(&p.abortCalls, 1)
	p.mu.Lock()
	// Copy so a later mutation of the caller's slice can't rewrite history.
	cp := append([]CandidatePart(nil), parts...)
	p.abortedParts = append(p.abortedParts, cp)
	p.mu.Unlock()
	if n <= p.failUntil {
		return errors.New("DROP PART transient failure")
	}
	return p.abortErr
}

func (p *partsRecordingPreparer) lastAbortedParts() []CandidatePart {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.abortedParts) == 0 {
		return nil
	}
	return p.abortedParts[len(p.abortedParts)-1]
}

func partNameSet(parts []CandidatePart) map[string]bool {
	s := map[string]bool{}
	for _, c := range parts {
		s[c.PartName] = true
	}
	return s
}

// TestOrchestrate_AbortPassesExactCandidateParts pins the core PR06 contract: on
// a terminal reject, the parts handed to AbortPreparedStatement are exactly the
// journal's frozen CandidateParts — same part names, no extras, none dropped.
// Cleanup is bounded by HouseGate's journal, not left to a companion-side lookup
// that could clean partition-wide.
func TestOrchestrate_AbortPassesExactCandidateParts(t *testing.T) {
	prep := &partsRecordingPreparer{prepared: boundSourceWithParts()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("terminal reject must not ACK2")
	}
	got := partNameSet(prep.lastAbortedParts())
	want := partNameSet(candidateFixture())
	if len(got) != len(want) {
		t.Fatalf("abort targeted %d parts, want exactly %d (the journal candidate parts)", len(got), len(want))
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("abort did not target frozen candidate part %q", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Fatalf("abort targeted extra part %q not in the journal candidate set", name)
		}
	}
}

func TestOrchestrate_TerminalRejectDoesNotAbortMismatchedCandidateTable(t *testing.T) {
	prepared := boundSourceWithParts()
	prepared.CandidateParts[0].TableID = prepared.CandidateParts[0].TableID + "__alias"
	prep := &partsRecordingPreparer{prepared: prepared}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflict"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err == nil || !strings.Contains(err.Error(), "candidate table mismatch") {
		t.Fatalf("terminal mismatched candidate=(%+v, %v), want fail-closed consistency error", res, err)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 0 {
		t.Fatalf("source abort ran %d times for an untrusted logical table binding", got)
	}
}

// TestOrchestrate_AbortNeverTargetsWholePartition pins that every abort target
// is a concrete part name (part-level), never a bare partition id / "all parts
// in partition" set. Design rule 4 forbids dropping a whole partition.
func TestOrchestrate_AbortNeverTargetsWholePartition(t *testing.T) {
	prep := &partsRecordingPreparer{prepared: boundSourceWithParts()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "malformed"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	if _, err := orch.Orchestrate(context.Background(), admissionFixture()); err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	for _, c := range prep.lastAbortedParts() {
		if c.PartName == "" {
			t.Fatal("abort target has an empty PartName — a partition-wide drop is forbidden; cleanup must be exact part names")
		}
	}
}

// TestOrchestrate_EmptyCandidatePartsAbortCleansIdempotently pins that a
// terminal reject whose frozen candidate set is empty still reaches Cleaned:
// nothing to drop is "already clean" (design rule 4, part-not-exists = cleaned).
func TestOrchestrate_EmptyCandidatePartsAbortCleansIdempotently(t *testing.T) {
	prepared := boundSource() // no CandidateParts
	prep := &partsRecordingPreparer{prepared: prepared}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "gap budget exceeded"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("empty-candidate abort must not error: %v", err)
	}
	if res.Lifecycle != LifecycleCleaned {
		t.Fatalf("empty-candidate terminal reject must reach Cleaned, got %q", res.Lifecycle)
	}
	if got := prep.lastAbortedParts(); len(got) != 0 {
		t.Fatalf("empty candidate set must hand zero parts to abort, got %d", len(got))
	}
}

func TestOrchestrate_PreCleanupProofFailureDoesNotRunSourceAbort(t *testing.T) {
	prep := &partsRecordingPreparer{prepared: boundSourceWithParts()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflict"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	proofErr := errors.New("pre-cleanup inventory unavailable")
	orch.SetBeforeExactCleanup(func(context.Context, IntakeResult) error { return proofErr })

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if !errors.Is(err, proofErr) || res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("first attempt=(%+v, %v), want AbortPending proof error", res, err)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 0 {
		t.Fatalf("source abort ran %d times without a pre-cleanup inventory proof", got)
	}

	// Same-ID retry is re-entrant on its source frontier and reruns proof before
	// the one exact abort; the unsafe prepare itself is never repeated.
	orch.SetBeforeExactCleanup(func(context.Context, IntakeResult) error { return nil })
	res, err = orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("proof retry=(%+v, %v), want Cleaned", res, err)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 1 {
		t.Fatalf("source abort count=%d want 1", got)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("unsafe prepare count=%d want 1", got)
	}
}

// TestOrchestrate_AbortPendingResumesExactPartsAfterFailure extends the
// failed-abort retry contract with the exact-parts assertion: a mid-abort
// failure leaves a resumable AbortPending record; both the failed attempt and
// the successful retry hand the SAME exact candidate parts, the retry reaches
// Cleaned, and prepare never re-runs.
func TestOrchestrate_AbortPendingResumesExactPartsAfterFailure(t *testing.T) {
	prep := &partsRecordingPreparer{prepared: boundSourceWithParts(), failUntil: 1}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	res, err := orch.Orchestrate(context.Background(), adm)
	if err == nil {
		t.Fatal("a failed abort must surface an error")
	}
	if res.Lifecycle == LifecycleCleaned {
		t.Fatal("a failed abort must not report Cleaned")
	}

	res2, err2 := orch.Orchestrate(context.Background(), adm)
	if err2 != nil {
		t.Fatalf("retry after abort recovery: %v", err2)
	}
	if res2.Lifecycle != LifecycleCleaned {
		t.Fatalf("retry must complete cleanup, got %q", res2.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 2 {
		t.Fatalf("abort must be retried exactly once more, got %d calls", got)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("abort retry must reuse the prepared record, prepareCount=%d", got)
	}
	// Both the failed attempt and the retry handed the SAME exact parts.
	prep.mu.Lock()
	defer prep.mu.Unlock()
	if len(prep.abortedParts) != 2 {
		t.Fatalf("expected 2 recorded abort calls, got %d", len(prep.abortedParts))
	}
	first, second := partNameSet(prep.abortedParts[0]), partNameSet(prep.abortedParts[1])
	want := partNameSet(candidateFixture())
	for _, got := range []map[string]bool{first, second} {
		if len(got) != len(want) {
			t.Fatalf("each abort attempt must target exactly the frozen parts; got %d want %d", len(got), len(want))
		}
		for name := range want {
			if !got[name] {
				t.Fatalf("abort attempt missing frozen part %q", name)
			}
		}
	}
}

// TestOrchestrate_RetryableThenTerminalAbortsExactParts proves a submit that is
// retryable first and terminal-reject on retry aborts exactly the cached
// candidate parts without re-preparing. It makes no green-ACK2 claim.
func TestOrchestrate_RetryableThenTerminalAbortsExactParts(t *testing.T) {
	prep := &partsRecordingPreparer{prepared: boundSourceWithParts()}
	sub := &seqSubmitter{outcomes: []SubmitOutcome{
		{Category: OutcomeRetryable, Reason: "NotLeader"},
		{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"},
	}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first (retryable) Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 0 {
		t.Fatalf("a retryable submit must not abort, got %d abort calls", got)
	}

	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("retry (terminal) Orchestrate: %v", err)
	}
	if res.Lifecycle != LifecycleCleaned {
		t.Fatalf("terminal reject on retry must reach Cleaned, got %q", res.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("abort on retry must reuse the prepared record, prepareCount=%d", got)
	}
	got := partNameSet(prep.lastAbortedParts())
	want := partNameSet(candidateFixture())
	if len(got) != len(want) {
		t.Fatalf("abort targeted %d parts, want exactly %d", len(got), len(want))
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("abort did not target frozen candidate part %q", name)
		}
	}
}

// seqSubmitter returns configured outcomes in order; the last repeats.
type seqSubmitter struct {
	mu       sync.Mutex
	outcomes []SubmitOutcome
	calls    int64
}

func (s *seqSubmitter) SubmitStatement(_ context.Context, _ StatementEnvelope) (SubmitOutcome, error) {
	n := atomic.AddInt64(&s.calls, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := int(n) - 1
	if idx >= len(s.outcomes) {
		idx = len(s.outcomes) - 1
	}
	return s.outcomes[idx], nil
}
