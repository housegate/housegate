package storageintegrity

import (
	"encoding/json"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func TestSubmitOutcomeCarriesTheArbiterRejectCode(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED, Message: "table retired"})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeSchemaNotAllowed || got.Reason != "table retired" {
		t.Fatalf("outcome = %+v", got)
	}
	if accepted := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: 1}); accepted.AdmissionCode != "" {
		t.Fatalf("an accepted outcome carries no reject code, got %q", accepted.AdmissionCode)
	}
}

// TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode pins that records
// written before AdmissionCode existed, and records without one, keep their
// exact JSON bytes in the intake journal.
func TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode(t *testing.T) {
	b, err := json.Marshal(SubmitOutcome{Category: OutcomeAccepted, Reason: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"Category":1,"Reason":"ok"}` {
		t.Fatalf("journal shape = %s", b)
	}
}

func TestSubmitOutcomeCarriesTheStatementSeq(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: 77})
	if got.Category != OutcomeAccepted || got.StatementSeq != 77 {
		t.Fatalf("outcome = %+v, want accepted with statement_seq 77", got)
	}
	if AdmissionCodeDuplicateClientSeq != "ADMISSION_CODE_DUPLICATE_CLIENT_SEQ" {
		t.Fatalf("AdmissionCodeDuplicateClientSeq = %q", AdmissionCodeDuplicateClientSeq)
	}
}

func TestLaneBudgetIsATerminalRejectWithItsCode(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED, Message: "account holds 256 client lanes"})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeLaneBudgetExceeded || got.AdmissionCode != "ADMISSION_CODE_LANE_BUDGET_EXCEEDED" {
		t.Fatalf("outcome = %+v", got)
	}
}

// TestSourceUnavailableIsATerminalRejectWithItsCode pins spec 2026-10-10 §6.4:
// the arbiter changed nothing, so the outcome is a coded terminal reject. Its
// category removes the prepared parts through the terminal-submit path, and
// its code tells the ingress which client refusal to send.
func TestSourceUnavailableIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "storage-integrity source snode-b of indexer 1 is not active"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeSourceUnavailable ||
		got.AdmissionCode != "ADMISSION_CODE_SOURCE_UNAVAILABLE" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("a source-unavailable refusal must remove the prepared parts through the terminal-submit path")
	}
}

// TestIndexerQuarantinedIsATerminalRejectWithItsCode pins spec 2026-10-10
// §7.4 and §9: the arbiter answers ADMISSION_CODE_INDEXER_QUARANTINED with its
// state unchanged, so the outcome is a coded terminal reject. Its category
// removes the prepared parts through the terminal-submit path, and its code
// tells the ingress which client refusal to send.
func TestIndexerQuarantinedIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "storage-integrity indexer 1 is quarantined"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_INDEXER_QUARANTINED, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeIndexerQuarantined ||
		got.AdmissionCode != "ADMISSION_CODE_INDEXER_QUARANTINED" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("an indexer-quarantined refusal must remove the prepared parts through the terminal-submit path")
	}
	// Only codes 11-14 join the coded terminal rejects: a code this binary does not
	// know still converges through the status probe, as before.
	if unknown := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode(15), Message: "future"}); unknown.Category != OutcomeUnknown || unknown.AdmissionCode != "" {
		t.Fatalf("unknown code outcome = %+v, want OutcomeUnknown without a code", unknown)
	}
}

// TestVerifiersUnavailableIsATerminalRejectWithItsCode pins spec 2026-10-14
// §7.3: the arbiter answers ADMISSION_CODE_VERIFIERS_UNAVAILABLE, with its
// state unchanged, when the eligible ring of the statement's group holds
// fewer than three verifiers, so the outcome is a coded terminal reject. Its
// category removes the prepared parts through the terminal-submit path, and
// its code tells the ingress which client refusal to send.
func TestVerifiersUnavailableIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "storage-integrity verifiers unavailable for indexer 1: 2 eligible, at least 3 required"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeVerifiersUnavailable ||
		got.AdmissionCode != "ADMISSION_CODE_VERIFIERS_UNAVAILABLE" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("a verifiers-unavailable refusal must remove the prepared parts through the terminal-submit path")
	}
	if pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE != 12 {
		t.Fatalf("ADMISSION_CODE_VERIFIERS_UNAVAILABLE = %d, want 12 (plan S3-E1 contract §39)", pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE)
	}
}

// TestCopiesBackpressureIsATerminalRejectWithItsCode pins spec 2026-10-14
// §9.3 and plan S3-E1 K59: with strict_copies on, the arbiter answers
// ADMISSION_CODE_COPIES_BACKPRESSURE, with its state unchanged, while the
// copy floor holds the table's cleanup above its uncleaned-parts bound, so
// the outcome is a coded terminal reject. Its category removes the prepared
// parts through the terminal-submit path, and its code tells the ingress to
// answer back-pressure.
func TestCopiesBackpressureIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "storage-integrity copy floor: net1.events holds 20001 uncleaned promoted parts (max_uncleaned_parts_per_table 20000) whose cleanup waits for member checkpoints (strict_copies); retry later"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeCopiesBackpressure ||
		got.AdmissionCode != "ADMISSION_CODE_COPIES_BACKPRESSURE" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("a copies-backpressure refusal must remove the prepared parts through the terminal-submit path")
	}
	if pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE != 13 {
		t.Fatalf("ADMISSION_CODE_COPIES_BACKPRESSURE = %d, want 13 (plan S3-E1 contract §39)", pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE)
	}
	// Without a message the code names the reason, as for every coded reject.
	if bare := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE}); bare.Reason != AdmissionCodeCopiesBackpressure {
		t.Fatalf("reason without a message = %q, want %q", bare.Reason, AdmissionCodeCopiesBackpressure)
	}
}

// TestAccountBudgetExceededIsATerminalRejectWithItsCode pins
// sentioxyz/arbiter#143 layer C: with the arbiter's account_budget enforced,
// a statement over its signer's or the network-wide budget gets
// ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED with the arbiter's state unchanged,
// so the outcome is a coded terminal reject that removes the prepared parts
// and tells the ingress to answer QUOTA_EXCEEDED.
func TestAccountBudgetExceededIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "account budget: account 0xabc exceeded its statements budget (window 1800 L2 blocks); retry once the table-registry cursor reaches L2 block 9 (committed cursor 3)"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeAccountBudgetExceeded ||
		got.AdmissionCode != "ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("a budget refusal must remove the prepared parts through the terminal-submit path")
	}
	if pb.AdmissionCode_ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED != 14 {
		t.Fatalf("ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED = %d, want 14", pb.AdmissionCode_ADMISSION_CODE_ACCOUNT_BUDGET_EXCEEDED)
	}
}
