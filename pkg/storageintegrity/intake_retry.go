package storageintegrity

import (
	"context"
	"fmt"
)

// IntakeStatusQuerier is the optional status-query port (design section 3.4's
// unknown-outcome convergence). When an earlier SubmitStatement or
// RegisterResultClaim returned an indeterminate (OutcomeUnknown) result — a
// timeout or a broken connection where HouseGate cannot tell whether the server
// accepted the operation — a resume queries the server by statement_id to
// collapse that unknown into a definite category before deciding whether to
// re-send. This lets an already-sequenced statement converge forward instead of
// being blindly re-submitted.
//
// Like StatementSubmitter and SourcePreparer, this is a HouseGate-defined port.
// The current paired Arbiter branch still does not expose a query-status RPC, so
// embedders leave this nil unless their topology can answer status by
// statement_id. The querier only reads status; it never mutates protocol state,
// and it never returns a category the two operations could not themselves return.
type IntakeStatusQuerier interface {
	// QuerySubmitStatus reports the Arbiter's current status for a previously
	// submitted statement: Accepted / ExactIdempotent if it was sequenced,
	// TerminalReject if it was rejected, Retryable if still transient, and
	// Unspecified for "no record" (never arrived). It never itself sequences.
	QuerySubmitStatus(ctx context.Context, statementID string) (SubmitOutcome, error)
	// QueryClaimStatus reports the SNode/FSM's current status for a previously
	// attempted result-claim registration, with the same category meanings. It
	// never itself binds.
	QueryClaimStatus(ctx context.Context, statementID string) (ClaimOutcome, error)
}

// NewOrchestratorWithQuerier constructs an Orchestrator that converges
// indeterminate (Unknown) outcomes deterministically via the status-query port
// before re-sending. It is otherwise identical to NewOrchestrator. When the
// querier is nil (the NewOrchestrator path), an Unknown outcome instead follows
// the design's equally-permitted idempotent-retry branch — a re-send that
// reuses the cached prepare and never repeats the unsafe write.
func NewOrchestratorWithQuerier(submitter StatementSubmitter, preparer SourcePreparer, querier IntakeStatusQuerier, cfg OrchestratorConfig) *Orchestrator {
	o := NewOrchestrator(submitter, preparer, cfg)
	o.querier = querier
	return o
}

// queryConvergence is the resend decision a status query yields for an
// indeterminate outcome.
type queryConvergence int

const (
	// convergeResend: the server has no record or is still transient
	// (Unspecified/Retryable/Unknown) — an idempotent re-send is safe.
	convergeResend queryConvergence = iota
	// convergeForward: the server already accepted the operation
	// (Accepted/ExactIdempotent) — converge forward without re-sending.
	convergeForward
	// convergeReject: the server authoritatively rejected (TerminalReject) —
	// route to the existing terminal-reject path.
	convergeReject
)

// classifyQueryConvergence maps a queried outcome category to a resend decision.
// A query is a pure status read: it collapses an unknown into one of the
// existing categories and never invents a new one. A "not found" (Unspecified)
// query result is treated as resend-safe, never as a terminal — a missing
// record means the operation never landed, not that it was rejected.
func classifyQueryConvergence(c OutcomeCategory) queryConvergence {
	switch {
	case c.PermitsAck2(): // Accepted / ExactIdempotent
		return convergeForward
	case c == OutcomeTerminalReject:
		return convergeReject
	default: // Unspecified (not found) / Retryable / Unknown
		return convergeResend
	}
}

// convergeUnknownSubmit resolves a prior indeterminate submit before a resume
// re-submits. It returns (resolved, outcome, err):
//   - resolved == false, err == nil: nothing to converge (the prior submit was a
//     plain retryable, or no querier is wired) — the caller performs a normal
//     idempotent re-send. This is design 3.4's "或幂等重试" branch.
//   - resolved == true: the query collapsed the unknown into `outcome`, which the
//     caller feeds through afterSubmit exactly as a fresh submit result
//     (Accepted → forward to the RC gate; TerminalReject → abort).
//   - err != nil: the query itself failed, so the outcome is still unknown; the
//     caller surfaces the error and the frontier stays held for a later retry.
//
// The submitUnknown flag is cleared once queried: a subsequent resume that still
// finds the statement unknown will have re-set it on the intervening attempt.
func (o *Orchestrator) convergeUnknownSubmit(ctx context.Context, env StatementEnvelope, rec *intakeRecord) (bool, SubmitOutcome, error) {
	if o.querier == nil || !o.submitUnknownFlag(rec) {
		return false, SubmitOutcome{}, nil
	}
	status, err := o.querier.QuerySubmitStatus(ctx, env.StatementID)
	if err != nil {
		return false, SubmitOutcome{}, fmt.Errorf("intake: query submit status failed for %s: %w", env.StatementID, err)
	}
	switch classifyQueryConvergence(status.Category) {
	case convergeForward, convergeReject:
		// A definite answer: hand it to afterSubmit as the effective submit result.
		o.setSubmitUnknown(rec, false)
		return true, status, nil
	default: // convergeResend
		// Still no record / transient: fall through to an idempotent re-send. Clear
		// the flag so the re-send path is a plain submit (it will re-set the flag if
		// it, too, comes back unknown).
		o.setSubmitUnknown(rec, false)
		return false, SubmitOutcome{}, nil
	}
}

// convergeUnknownClaim resolves a prior indeterminate RC before a resume
// re-registers. It returns (res, err, done):
//   - done == false: nothing to converge (the prior RC was a plain retryable, or
//     no querier is wired) — the caller re-registers via registerAndFinish.
//   - done == true, err == nil: the query collapsed the unknown into a definite
//     claim outcome, finalized directly via finalizeClaim (Bound → ACK2;
//     TerminalReject → abort), so no second RegisterPreparedClaim is issued.
//   - done == true, err != nil: the query itself failed; the outcome is still
//     unknown and the error is surfaced so the frontier stays held for a retry.
func (o *Orchestrator) convergeUnknownClaim(ctx context.Context, prepared PreparedLocalResult, rec *intakeRecord) (IntakeResult, error, bool) {
	if o.querier == nil || !o.claimUnknownFlag(rec) {
		return IntakeResult{}, nil, false
	}
	status, err := o.querier.QueryClaimStatus(ctx, rec.statementID)
	if err != nil {
		return o.resultFor(rec), fmt.Errorf("intake: query claim status failed for %s: %w", rec.statementID, err), true
	}
	if classifyQueryConvergence(status.Category) == convergeResend {
		// Still no record / transient: re-register on this attempt. Clear the flag
		// so the re-register path is plain (it re-sets the flag if still unknown).
		o.setClaimUnknown(rec, false)
		return IntakeResult{}, nil, false
	}
	// A definite answer (Bound or TerminalReject): finalize it directly without a
	// second registration.
	o.setClaimUnknown(rec, false)
	res, ferr := o.finalizeClaim(ctx, prepared, status, rec)
	return res, ferr, true
}

func (o *Orchestrator) setSubmitUnknown(rec *intakeRecord, v bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec.submitUnknown = v
}

func (o *Orchestrator) setClaimUnknown(rec *intakeRecord, v bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec.claimUnknown = v
}

func (o *Orchestrator) submitUnknownFlag(rec *intakeRecord) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return rec.submitUnknown
}

func (o *Orchestrator) claimUnknownFlag(rec *intakeRecord) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return rec.claimUnknown
}
