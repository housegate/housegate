package storageintegrity

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Green-today: pure convergence mapping. No companion seam needed. ---

// TestClassifyQueryConvergence pins how a status-query result collapses an
// indeterminate (Unknown) outcome into a resend decision (design section 3.4,
// 结果未知 row). Accepted/ExactIdempotent means the server already landed the
// operation → converge forward without re-sending. NotFound/Retryable/Unknown
// means the server has no record or is still transient → an idempotent re-send
// is safe. TerminalReject means the server authoritatively rejected → route to
// the existing terminal path. A query is a pure status read; it never
// synthesizes a new category.
func TestClassifyQueryConvergence(t *testing.T) {
	cases := []struct {
		name string
		in   OutcomeCategory
		want queryConvergence
	}{
		{"accepted converges forward", OutcomeAccepted, convergeForward},
		{"idempotent converges forward", OutcomeExactIdempotent, convergeForward},
		{"terminal reject routes to reject", OutcomeTerminalReject, convergeReject},
		{"retryable allows resend", OutcomeRetryable, convergeResend},
		{"unknown allows resend", OutcomeUnknown, convergeResend},
		{"unspecified (not found) allows resend", OutcomeUnspecified, convergeResend},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyQueryConvergence(tc.in); got != tc.want {
				t.Fatalf("classifyQueryConvergence(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// countingQuerier records how many times each query method was called and
// returns configured outcomes. It is an in-test IntakeStatusQuerier double, NOT
// a substitute for the missing companion query seam.
type countingQuerier struct {
	submitStatus  SubmitOutcome
	submitErr     error
	claimStatus   ClaimOutcome
	claimErr      error
	submitQueries int64
	claimQueries  int64
}

func (q *countingQuerier) QuerySubmitStatus(_ context.Context, _ string) (SubmitOutcome, error) {
	atomic.AddInt64(&q.submitQueries, 1)
	return q.submitStatus, q.submitErr
}

func (q *countingQuerier) QueryClaimStatus(_ context.Context, _ string) (ClaimOutcome, error) {
	atomic.AddInt64(&q.claimQueries, 1)
	return q.claimStatus, q.claimErr
}

// unknownThenRecordingSubmitter returns OutcomeUnknown on the first submit and a
// configured outcome afterwards, and records how many submits ran and the last
// query-vs-submit order. It lets a test prove a query precedes any re-submit.
type orderRecordingSubmitter struct {
	mu       sync.Mutex
	outcomes []SubmitOutcome // consumed in order; last repeats
	calls    int64
}

func (s *orderRecordingSubmitter) SubmitStatement(_ context.Context, _ StatementEnvelope) (SubmitOutcome, error) {
	n := atomic.AddInt64(&s.calls, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := int(n) - 1
	if idx >= len(s.outcomes) {
		idx = len(s.outcomes) - 1
	}
	return s.outcomes[idx], nil
}

// TestOrchestrate_UnknownSubmitDoesNotAbortOrAck is the green-today regression
// companion to the retryable test: an unknown submit must not abort candidate
// parts and must not ACK2. It stays on the non-terminal path, so it claims no
// green ACK2 intake and needs no companion seam.
func TestOrchestrate_UnknownSubmitDoesNotAbortOrAck(t *testing.T) {
	prep := &retryablePreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeUnknown, Reason: "timeout"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("unknown submit must not yield ACK2")
	}
	if got := atomic.LoadInt64(&prep.abortCnt); got != 0 {
		t.Fatalf("unknown submit must not abort candidate parts, abortCnt=%d", got)
	}
}

// TestOrchestrate_UnknownOutcomeHoldsFrontier proves an unknown (not merely
// retryable) intake keeps the source frontier held against a different
// statement until it converges — the same serial-source guarantee the retryable
// path has. Green-today: frontier logic is pure HouseGate-local coordination.
func TestOrchestrate_UnknownOutcomeHoldsFrontier(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()

	// adm1's submit is unknown, so its intake stays non-terminal and holds the
	// frontier; adm2 on the same source must not start its prepare.
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeUnknown, Reason: "timeout"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	adm1 := admissionFixture()
	adm2 := admissionFixture()
	adm2.StatementID = fixtureStatementID(2)

	if _, err := orch.Orchestrate(context.Background(), adm1); err != nil {
		t.Fatalf("adm1 Orchestrate: %v", err)
	}
	// adm1 returned unknown and is holding the frontier. adm2 must block; run it
	// in a goroutine and confirm it never prepares within a short window.
	q2done := make(chan struct{})
	go func() {
		defer close(q2done)
		orch.Orchestrate(context.Background(), adm2)
	}()
	select {
	case <-q2done:
		t.Fatal("adm2 must block on the frontier held by the unknown adm1 intake")
	case <-time.After(50 * time.Millisecond):
	}
	if got := prep.prepareCountFor(adm2.StatementID); got != 0 {
		t.Fatalf("adm2 prepared %d times while unknown adm1 held the frontier", got)
	}
}

// --- Gated: deterministic query-first convergence. Needs the companion query
// seam, so requireCompanionStagedIntake(t) keeps these tied to the companion
// availability flag. ---

// TestOrchestrate_UnknownSubmitQueriesBeforeResend proves that after an unknown
// submit, the next attempt calls QuerySubmitStatus BEFORE any SubmitStatement
// re-send when a querier is wired.
func TestOrchestrate_UnknownSubmitQueriesBeforeResend(t *testing.T) {
	requireCompanionStagedIntake(t)

	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	// First submit unknown; if the code ever re-sends without querying, the
	// second element would be consumed — but the querier resolves it first.
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{{Category: OutcomeUnknown, Reason: "timeout"}, {Category: OutcomeAccepted}}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	submitsAfterFirst := atomic.LoadInt64(&sub.calls)

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&q.submitQueries); got == 0 {
		t.Fatal("unknown submit resume must query submit status first")
	}
	// Query returned Accepted → converge forward, no re-submit on the resume.
	if got := atomic.LoadInt64(&sub.calls); got != submitsAfterFirst {
		t.Fatalf("query returned Accepted, resume must not re-submit; submits went %d -> %d", submitsAfterFirst, got)
	}
}

// TestOrchestrate_UnknownSubmitQueryFindsAcceptedConvergesForward proves an
// unknown submit whose query finds Accepted proceeds to the RC gate and reaches
// ACK2 without re-submitting or re-preparing.
func TestOrchestrate_UnknownSubmitQueryFindsAcceptedConvergesForward(t *testing.T) {
	requireCompanionStagedIntake(t)

	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{{Category: OutcomeUnknown, Reason: "timeout"}}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if !res.Ack2 {
		t.Fatal("unknown submit whose query finds Accepted must converge to ACK2")
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("convergence must reuse the cached prepare, prepareCount=%d", got)
	}
}

// TestOrchestrate_UnknownSubmitQueryNotFoundAllowsResend proves an unknown
// submit whose query returns NotFound (mapped to resend-safe) proceeds to an
// idempotent re-submit rather than being stuck.
func TestOrchestrate_UnknownSubmitQueryNotFoundAllowsResend(t *testing.T) {
	requireCompanionStagedIntake(t)

	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	// First submit unknown; query returns NotFound (Unspecified) → resend; the
	// re-submit returns Accepted.
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{{Category: OutcomeUnknown, Reason: "timeout"}, {Category: OutcomeAccepted}}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeUnspecified}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	submitsAfterFirst := atomic.LoadInt64(&sub.calls)
	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&q.submitQueries); got == 0 {
		t.Fatal("resume must query before resend")
	}
	if got := atomic.LoadInt64(&sub.calls); got <= submitsAfterFirst {
		t.Fatalf("NotFound query must allow an idempotent re-submit; submits stayed at %d", got)
	}
	if !res.Ack2 {
		t.Fatal("re-submit Accepted + bound RC must converge to ACK2")
	}
}

// TestOrchestrate_UnknownRCQueriesBeforeReregister proves an unknown RC resume
// calls QueryClaimStatus before re-registering, and a Bound query result
// converges to ACK2.
func TestOrchestrate_UnknownRCQueriesBeforeReregister(t *testing.T) {
	requireCompanionStagedIntake(t)

	// Submit accepted; first RC unknown; querier reports the claim already bound.
	prep := &rcUnknownThenProbe{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	q := &countingQuerier{claimStatus: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&q.claimQueries); got == 0 {
		t.Fatal("unknown RC resume must query claim status first")
	}
	if got := atomic.LoadInt64(&prep.registerCnt); got != 1 {
		t.Fatalf("a Bound query result must not re-register; registerCnt=%d", got)
	}
	if !res.Ack2 {
		t.Fatal("unknown RC whose query finds Bound must converge to ACK2")
	}
}

// rcUnknownThenProbe returns an unknown RC on the first RegisterPreparedClaim
// and counts registrations, so a test can prove a Bound query result skips
// re-registration.
type rcUnknownThenProbe struct {
	prepared     PreparedLocalResult
	registerCnt  int64
	prepareCount int64
}

func (p *rcUnknownThenProbe) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *rcUnknownThenProbe) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	atomic.AddInt64(&p.registerCnt, 1)
	return ClaimOutcome{Category: OutcomeUnknown, Reason: "timeout"}, nil
}

func (p *rcUnknownThenProbe) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	return nil
}
