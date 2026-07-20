package storageintegrity

// Ack2Inputs is the complete set of facts the ACK2 dual gate reads to decide
// whether a storage-integrity intake may return ACK2 to the client. It is a
// pure HouseGate-local value: the orchestrator populates it from the durable
// intake record (payload durability, unsafe-write completion, the pinned
// source), the Arbiter SubmitStatement outcome, and the SNode
// RegisterResultClaim outcome. The gate never re-derives these facts; it only
// requires that all five design-section-3.4 conditions hold together, so that a
// single satisfied sub-condition (e.g. a completed unsafe write) can never on
// its own tell the client the statement succeeded.
type Ack2Inputs struct {
	// PayloadDurable is true once the content-addressed payload was durably put
	// on the source (design's payload_durable). The source performs the durable
	// PayloadStore.Put before the unsafe INSERT, so this is a precondition of
	// UnsafeWriteDone in practice, but the gate requires both explicitly.
	PayloadDurable bool
	// UnsafeWriteDone is true once the source's unsafe INSERT completed and its
	// exact candidate inventory was persisted (design's unsafe_write_complete).
	UnsafeWriteDone bool

	// Submit is the Arbiter SubmitStatement outcome. Only {Accepted,
	// ExactIdempotent} keeps ACK2 reachable.
	Submit SubmitOutcome
	// Claim is the SNode RegisterResultClaim (RC late-binding) outcome. Only
	// {Bound (Accepted), ExactIdempotentAcceptance} keeps ACK2 reachable.
	Claim ClaimOutcome

	// JournalLifecycle is the crash-safe intake journal stage. ACK2 requires it
	// to be exactly RCBound — the stage the orchestrator sets only after the RC
	// gate has bound the claim. A lower stage (still writing / submit accepted
	// but claim not yet bound) or an abort stage withholds ACK2.
	JournalLifecycle Lifecycle

	// PreparedSource is the source_node the prepare produced (PreparedLocalResult
	// .SourceNode). The source the FSM recorded for the statement is read directly
	// from Claim.BoundSource — the authoritative field — never from a redundant
	// copy, so the gate cannot be told the sources agree while Claim carries a
	// different bound source. ACK2 requires PreparedSource and Claim.BoundSource
	// to both be non-empty and equal: the local write and the sequenced claim must
	// agree on the same source, so a claim bound to a different (or blank) source
	// can never be acked.
	PreparedSource string
}

// Ack2Ready reports whether all five ACK2 conditions from design section 3.4
// hold together, and if not, a short reason naming the first unmet condition.
// It is the single dual-gate predicate the orchestrator and the ingress plugin
// consult before telling a client the storage-integrity statement succeeded:
//
//	payload_durable
//	&& unsafe_write_complete
//	&& SubmitStatement in {Accepted, ExactIdempotentReAck}
//	&& RegisterResultClaim in {Bound, ExactIdempotentAcceptance}
//	&& local_intake_journal.lifecycle == RCBound
//
// plus the source-agreement invariant (the RC's bound source_node is non-empty
// and equals the source that prepared the write). Every condition is required;
// none is optional, so a single satisfied sub-condition can never grant ACK2 on
// its own. The checks are ordered from earliest lifecycle fact to latest so the
// reason names the earliest thing still missing.
func Ack2Ready(in Ack2Inputs) (bool, string) {
	if !in.PayloadDurable {
		return false, "payload not durable"
	}
	if !in.UnsafeWriteDone {
		return false, "unsafe write not complete"
	}
	if !in.Submit.Category.PermitsAck2() {
		return false, "submit not accepted"
	}
	if !in.Claim.Category.PermitsAck2() {
		return false, "result claim not bound"
	}
	if in.PreparedSource == "" {
		return false, "prepared result has no source_node"
	}
	// Read the bound source from the authoritative Claim, not a caller-supplied
	// copy, so a claim actually bound to a different source can never pass.
	if in.Claim.BoundSource == "" {
		return false, "RC has no bound source_node"
	}
	if in.PreparedSource != in.Claim.BoundSource {
		return false, "RC source_node mismatch"
	}
	if in.JournalLifecycle != LifecycleRCBound {
		return false, "intake journal lifecycle is not RCBound"
	}
	return true, ""
}
