package storageintegrity

import "testing"

// ackReadyFixture returns an Ack2Inputs value with all five ACK2 conditions
// satisfied. Each test below perturbs exactly one field so the gate's
// "five conditions, none optional" contract is pinned field by field.
func ackReadyFixture() Ack2Inputs {
	return Ack2Inputs{
		PayloadDurable:   true,
		UnsafeWriteDone:  true,
		Submit:           SubmitOutcome{Category: OutcomeAccepted},
		Claim:            ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
		JournalLifecycle: LifecycleRCBound,
		PreparedSource:   "snode-A",
	}
}

// TestAck2Gate_AllFiveConditionsGrantAck2 is the one positive case: with all
// five conditions present the gate returns ACK2 with no reason.
func TestAck2Gate_AllFiveConditionsGrantAck2(t *testing.T) {
	ok, reason := Ack2Ready(ackReadyFixture())
	if !ok {
		t.Fatalf("all five conditions satisfied must grant ACK2, got reject %q", reason)
	}
	if reason != "" {
		t.Fatalf("granted ACK2 must carry no reason, got %q", reason)
	}
}

// TestAck2Gate_EachConditionIsNecessary drops or invalidates one condition at a
// time and asserts the gate withholds ACK2. It pins design section 3.4's
// "payload_durable && unsafe_write_complete && Submit in {Accepted,
// ExactIdempotentReAck} && Claim in {Bound, ExactIdempotentAcceptance} &&
// lifecycle == RCBound" — none of the five is optional.
func TestAck2Gate_EachConditionIsNecessary(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Ack2Inputs)
	}{
		{"payload not durable", func(in *Ack2Inputs) { in.PayloadDurable = false }},
		{"unsafe write incomplete", func(in *Ack2Inputs) { in.UnsafeWriteDone = false }},
		{"submit retryable", func(in *Ack2Inputs) { in.Submit = SubmitOutcome{Category: OutcomeRetryable} }},
		{"submit unknown", func(in *Ack2Inputs) { in.Submit = SubmitOutcome{Category: OutcomeUnknown} }},
		{"submit terminal reject", func(in *Ack2Inputs) { in.Submit = SubmitOutcome{Category: OutcomeTerminalReject} }},
		{"submit unspecified", func(in *Ack2Inputs) { in.Submit = SubmitOutcome{} }},
		{"claim retryable", func(in *Ack2Inputs) { in.Claim = ClaimOutcome{Category: OutcomeRetryable, BoundSource: "snode-A"} }},
		{"claim unknown", func(in *Ack2Inputs) { in.Claim = ClaimOutcome{Category: OutcomeUnknown, BoundSource: "snode-A"} }},
		{"claim terminal reject", func(in *Ack2Inputs) { in.Claim = ClaimOutcome{Category: OutcomeTerminalReject, BoundSource: "snode-A"} }},
		{"claim unspecified", func(in *Ack2Inputs) { in.Claim = ClaimOutcome{BoundSource: "snode-A"} }},
		{"lifecycle preparing", func(in *Ack2Inputs) { in.JournalLifecycle = LifecyclePreparing }},
		{"lifecycle unsafe written", func(in *Ack2Inputs) { in.JournalLifecycle = LifecycleUnsafeWritten }},
		{"lifecycle submit accepted", func(in *Ack2Inputs) { in.JournalLifecycle = LifecycleSubmitAccepted }},
		{"lifecycle abort pending", func(in *Ack2Inputs) { in.JournalLifecycle = LifecycleAbortPending }},
		{"lifecycle cleaned", func(in *Ack2Inputs) { in.JournalLifecycle = LifecycleCleaned }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := ackReadyFixture()
			tc.mutate(&in)
			ok, reason := Ack2Ready(in)
			if ok {
				t.Fatal("a missing ACK2 condition must withhold ACK2")
			}
			if reason == "" {
				t.Fatal("a withheld ACK2 must carry a reason")
			}
		})
	}
}

// TestAck2Gate_ExactIdempotentSatisfiesBothGates confirms the success class is
// {Accepted, ExactIdempotent} on both the submit and claim gates: an idempotent
// re-ack / re-bind is equivalent to a fresh accept for ACK2 purposes.
func TestAck2Gate_ExactIdempotentSatisfiesBothGates(t *testing.T) {
	in := ackReadyFixture()
	in.Submit = SubmitOutcome{Category: OutcomeExactIdempotent}
	in.Claim = ClaimOutcome{Category: OutcomeExactIdempotent, BoundSource: "snode-A"}
	ok, reason := Ack2Ready(in)
	if !ok {
		t.Fatalf("exact-idempotent submit + claim must grant ACK2, got %q", reason)
	}
}

// TestAck2Gate_BoundSourceMustBePresentAndMatchPrepared pins that the RC's
// bound source_node — read from the authoritative Claim.BoundSource — must be
// non-empty and equal to the source that prepared the write. A blank or
// mismatched bound source withholds ACK2 even when both categories and the
// lifecycle are otherwise satisfied — the local write and the sequenced claim
// must agree on the same source.
func TestAck2Gate_BoundSourceMustBePresentAndMatchPrepared(t *testing.T) {
	cases := []struct {
		name     string
		prepared string
		bound    string
	}{
		{"blank bound source", "snode-A", ""},
		{"blank prepared source", "", "snode-A"},
		{"both blank", "", ""},
		{"source mismatch", "snode-A", "snode-B"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := ackReadyFixture()
			in.PreparedSource = tc.prepared
			in.Claim.BoundSource = tc.bound
			if ok, _ := Ack2Ready(in); ok {
				t.Fatal("a blank or mismatched bound source must withhold ACK2")
			}
		})
	}
}

// TestAck2Gate_UsesAuthoritativeClaimBoundSource is the contradictory-input
// regression: the gate must read the bound source from Claim.BoundSource, not
// trust a separately-supplied field. A claim actually bound to "snode-B" while
// the prepare produced "snode-A" must withhold ACK2 — there is no redundant
// ClaimBoundSource field to disagree with the Claim, so the gate cannot be
// fooled into acking a genuine cross-source binding.
func TestAck2Gate_UsesAuthoritativeClaimBoundSource(t *testing.T) {
	in := ackReadyFixture()
	in.PreparedSource = "snode-A"
	in.Claim = ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-B"}
	if ok, reason := Ack2Ready(in); ok {
		t.Fatalf("a claim bound to a different source than prepared must withhold ACK2, got ok (reason %q)", reason)
	}
}

// TestUnsafeWriteAloneDoesNotAck2 asserts the specific safety property design
// section 3.4 calls out: a durable, fully-written local part is NOT sufficient
// for ACK2 on its own. Without an accepted submit, a bound claim, and the
// RCBound lifecycle, the client must not be told the write succeeded.
func TestUnsafeWriteAloneDoesNotAck2(t *testing.T) {
	in := Ack2Inputs{
		PayloadDurable:  true,
		UnsafeWriteDone: true,
		// submit not accepted, claim not bound, lifecycle still UnsafeWritten
		Submit:           SubmitOutcome{Category: OutcomeRetryable},
		Claim:            ClaimOutcome{},
		JournalLifecycle: LifecycleUnsafeWritten,
		PreparedSource:   "snode-A",
	}
	if ok, _ := Ack2Ready(in); ok {
		t.Fatal("a complete unsafe write alone must never yield ACK2")
	}
}
