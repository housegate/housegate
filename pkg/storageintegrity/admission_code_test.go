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
