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
