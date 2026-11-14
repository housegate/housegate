package storageintegrity

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// staleTokenRefusal is what an Arbiter answers a resubmit whose user JWS has
// aged past its ingress.max_statement_age.
func staleTokenRefusal() SubmitOutcome {
	return submitOutcomeFromRPCError(status.Error(codes.InvalidArgument, ArbiterStaleStatementTokenReason))
}

func TestSubmitOutcomeFromRPCError_StaleTokenIsATerminalRejectWithItsReason(t *testing.T) {
	got := staleTokenRefusal()
	if got.Category != OutcomeTerminalReject || got.Reason != ArbiterStaleStatementTokenReason {
		t.Fatalf("stale token refusal = %+v", got)
	}
}

// TestOrchestrate_StaleTokenRefusalOfASequencedStatementConvergesForward: the
// first submit's answer is lost after the Arbiter sequenced the statement (a
// failed custody pin, a restart), and by the time the source resubmits
// the user JWS is older than max_statement_age, so the Arbiter refuses the
// resubmit for freshness. That refusal says nothing about a statement the
// Arbiter already sequenced; the source must not abort its candidate (the
// sealed block would then wait for its result claim forever). The status
// query finds the statement, so intake converges forward to the RC gate.
func TestOrchestrate_StaleTokenRefusalOfASequencedStatementConvergesForward(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{
		{Category: OutcomeRetryable, Reason: "payload custody pin unavailable; retry"},
		staleTokenRefusal(),
	}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeExactIdempotent, Reason: "Sequenced"}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&q.submitQueries); got != 1 {
		t.Fatalf("a stale-token refusal must be checked against the statement status once, queries=%d", got)
	}
	if atomic.LoadInt64(&prep.abortAt) != 0 {
		t.Fatal("a sequenced statement must never be aborted on a stale-token refusal")
	}
	if !res.Ack2 {
		t.Fatalf("a sequenced statement must converge to ACK2: %+v", res)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("convergence must reuse the cached prepare, prepareCount=%d", got)
	}
}

// A stale-token refusal of a statement the Arbiter has no record of is still a
// terminal reject: the token can never be admitted again, so the candidate is
// cleaned as before.
func TestOrchestrate_StaleTokenRefusalOfAnUnsequencedStatementAborts(t *testing.T) {
	prep := &recordingPreparer{prepared: boundSource()}
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{staleTokenRefusal()}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeUnspecified, Reason: "arbiter has no statement record"}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, _ := orch.Orchestrate(context.Background(), admissionFixture())
	if got := atomic.LoadInt64(&q.submitQueries); got != 1 {
		t.Fatalf("queries=%d, want 1", got)
	}
	if atomic.LoadInt64(&prep.abortAt) == 0 || res.Ack2 {
		t.Fatalf("an unsequenced stale statement must be aborted: %+v", res)
	}
	if res.Submit.Category != OutcomeTerminalReject || res.Submit.Reason != ArbiterStaleStatementTokenReason {
		t.Fatalf("submit outcome = %+v, want the stale-token terminal reject", res.Submit)
	}
}

// When the status query fails, whether the statement is sequenced is unknown:
// intake holds it (no abort, no ACK2) and the next attempt queries first.
func TestOrchestrate_StaleTokenRefusalWithAFailedQueryHolds(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{staleTokenRefusal()}}
	q := &countingQuerier{submitErr: errors.New("arbiter unreachable")}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	res, _ := orch.Orchestrate(context.Background(), adm)
	if atomic.LoadInt64(&prep.abortAt) != 0 || res.Ack2 {
		t.Fatalf("an unresolved stale-token refusal must hold: %+v", res)
	}
	if _, pending := orch.recoveryAdmission(adm.StatementID); !pending {
		t.Fatal("the statement must stay pending for a later attempt")
	}

	q.submitErr = nil
	q.submitStatus = SubmitOutcome{Category: OutcomeExactIdempotent, Reason: "Sequenced"}
	submits := atomic.LoadInt64(&sub.calls)
	res, err := orch.Orchestrate(context.Background(), adm)
	if err != nil || !res.Ack2 {
		t.Fatalf("resume once the query answers found = %+v, %v; want ACK2", res, err)
	}
	if got := atomic.LoadInt64(&sub.calls); got != submits {
		t.Fatalf("the resume must converge by query, not resubmit; submits %d -> %d", submits, got)
	}
}

// Other terminal rejects are authoritative refusals of this envelope and are
// not second-guessed by a status query.
func TestOrchestrate_OtherTerminalRejectsDoNotQuery(t *testing.T) {
	prep := &recordingPreparer{prepared: boundSource()}
	sub := &orderRecordingSubmitter{outcomes: []SubmitOutcome{
		submitOutcomeFromRPCError(status.Error(codes.InvalidArgument, "user_jws iat is too far in the future")),
	}}
	q := &countingQuerier{submitStatus: SubmitOutcome{Category: OutcomeExactIdempotent}}
	orch := NewOrchestratorWithQuerier(sub, prep, q, OrchestratorConfig{ExpectedSource: "snode-A"})

	res, _ := orch.Orchestrate(context.Background(), admissionFixture())
	if got := atomic.LoadInt64(&q.submitQueries); got != 0 {
		t.Fatalf("queries=%d, want none", got)
	}
	if atomic.LoadInt64(&prep.abortAt) == 0 || res.Ack2 {
		t.Fatalf("a terminal reject must abort: %+v", res)
	}
}
