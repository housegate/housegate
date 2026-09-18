package replay

import (
	"encoding/json"
	"testing"
)

func TestSnapshotQueryExecutionExtensionsAreInProcessOnly(t *testing.T) {
	request := ExecutionRequest{Job: ReplayJob{BlockSeq: 7}}
	oldRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	request.SnapshotQuery = &SnapshotQueryJob{BlockSeq: 99}
	request.SnapshotQueryReferenceID = "private-local-reference"
	extendedRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if string(oldRequest) != string(extendedRequest) {
		t.Fatal("query request fields escaped into legacy JSON")
	}
	result := ExecutionResult{BlockSeq: 7, ComputedStateRoot: "unchanged"}
	oldResult, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	oldHash, err := CanonicalDigest("in-process-compatibility-test", result)
	if err != nil {
		t.Fatal(err)
	}
	result.SnapshotQuery = &SnapshotQueryEvidence{ExecutionOutcome: "applied", OutputRowCount: 5, OutputRowsRoot: "private-result"}
	extendedResult, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	extendedHash, err := CanonicalDigest("in-process-compatibility-test", result)
	if err != nil {
		t.Fatal(err)
	}
	if string(oldResult) != string(extendedResult) || oldHash != extendedHash {
		t.Fatal("query result changed legacy JSON/hash")
	}
	var roundTrip ExecutionRequest
	if err = json.Unmarshal(extendedRequest, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.SnapshotQuery != nil || roundTrip.SnapshotQueryReferenceID != "" {
		t.Fatal("in-process request reconstructed from legacy wire")
	}
}
