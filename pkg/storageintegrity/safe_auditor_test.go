package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func auditPartsFixture() []replay.PartManifestEntry {
	return []replay.PartManifestEntry{
		{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartRowLtHash: "lh-p1", RowCount: 10, Bytes: 100},
		{TableID: "net1.events", PartitionID: "p2", PartName: "p2_1_1_0", PartRowLtHash: "lh-p2", RowCount: 20, Bytes: 200},
	}
}

func auditTaskFixture() AuditTask {
	return AuditTask{
		SnapshotID:          "snap-1",
		ExpectedActiveParts: auditPartsFixture(),
		Participants:        []string{"w-1", "w-2", "w-3"},
	}
}

// matchingEvidence is a local readback that exactly matches the task.
func matchingEvidence(task AuditTask) LocalAuditEvidence {
	var parts []CandidatePart
	recomputed := map[string]string{}
	for _, p := range task.ExpectedActiveParts {
		parts = append(parts, CandidatePart{
			TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName,
			PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes,
		})
		recomputed[p.PartName] = p.PartRowLtHash
	}
	return LocalAuditEvidence{ActiveParts: parts, RecomputedRowHash: recomputed}
}

func mustAuditSigner(t *testing.T, id string, seedByte byte) *Ed25519ClaimSigner {
	t.Helper()
	seed := make([]byte, 32)
	seed[0] = seedByte
	s, err := NewEd25519ClaimSigner(id, seed)
	if err != nil {
		t.Fatalf("signer %s: %v", id, err)
	}
	return s
}

func TestAuditTask_Valid(t *testing.T) {
	if err := auditTaskFixture().Valid(); err != nil {
		t.Fatalf("well-formed task must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*AuditTask)
	}{
		{"blank snapshot", func(a *AuditTask) { a.SnapshotID = "" }},
		{"empty parts", func(a *AuditTask) { a.ExpectedActiveParts = nil }},
		{"too few participants", func(a *AuditTask) { a.Participants = []string{"w-1"} }},
		{"duplicate participant", func(a *AuditTask) { a.Participants = []string{"w-1", "w-1", "w-3"} }},
		{"blank participant", func(a *AuditTask) { a.Participants = []string{"w-1", "", "w-3"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := auditTaskFixture()
			tc.mutate(&task)
			if err := task.Valid(); err == nil {
				t.Fatal("invalid task must fail closed")
			}
		})
	}
}

func TestComputeAuditHash_DeterministicOrderInsensitiveAndContentSensitive(t *testing.T) {
	parts := auditPartsFixture()
	a, err := ComputeAuditHash(parts)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	reordered := []replay.PartManifestEntry{parts[1], parts[0]}
	b, err := ComputeAuditHash(reordered)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if a != b {
		t.Fatal("audit hash must be order-insensitive across parts")
	}
	// Independent recompute equals (cross-worker recomputability).
	c, _ := ComputeAuditHash(auditPartsFixture())
	if a != c {
		t.Fatal("audit hash must be deterministic across independent recompute")
	}
	// Content change flips the hash.
	changed := auditPartsFixture()
	changed[0].PartRowLtHash = "lh-p1-DIFFERENT"
	d, _ := ComputeAuditHash(changed)
	if a == d {
		t.Fatal("audit hash must change when a part's checksum changes")
	}
	if _, err := ComputeAuditHash(nil); err == nil {
		t.Fatal("empty part set must fail closed")
	}
}

func TestVerifyLocalActiveSet_PassAndFailMatrix(t *testing.T) {
	task := auditTaskFixture()
	if o, r := VerifyLocalActiveSet(task, matchingEvidence(task)); o != AuditVotePass || r != AuditNoFailure {
		t.Fatalf("exact match must Pass, got %s/%s", o, r)
	}
	cases := []struct {
		name   string
		mutate func(*LocalAuditEvidence)
		want   AuditFailReason
	}{
		{"missing part", func(e *LocalAuditEvidence) { e.ActiveParts = e.ActiveParts[:1] }, AuditActiveSetMismatch},
		{"extra part", func(e *LocalAuditEvidence) {
			e.ActiveParts = append(e.ActiveParts, CandidatePart{TableID: "net1.events", PartitionID: "p9", PartName: "p9_1_1_0", PartRowLtHash: "x", RowCount: 1, Bytes: 1})
		}, AuditActiveSetMismatch},
		{"row count drift", func(e *LocalAuditEvidence) { e.ActiveParts[0].RowCount = 999 }, AuditPartMetadataMismatch},
		{"bytes drift", func(e *LocalAuditEvidence) { e.ActiveParts[0].Bytes = 999 }, AuditPartMetadataMismatch},
		{"checksum drift", func(e *LocalAuditEvidence) { e.ActiveParts[0].PartRowLtHash = "tampered" }, AuditChecksumMismatch},
		{"recomputed row-hash drift", func(e *LocalAuditEvidence) { e.RecomputedRowHash["p1_1_1_0"] = "tampered" }, AuditRowHashMismatch},
		{"missing readback", func(e *LocalAuditEvidence) { delete(e.RecomputedRowHash, "p1_1_1_0") }, AuditReadbackMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := matchingEvidence(task)
			tc.mutate(&ev)
			o, r := VerifyLocalActiveSet(task, ev)
			if o != AuditVoteFail {
				t.Fatalf("%s must Fail, got %s", tc.name, o)
			}
			if r != tc.want {
				t.Fatalf("%s reason = %s, want %s", tc.name, r, tc.want)
			}
		})
	}
}

func TestSignAuditVote_RefusesPassOnMismatch(t *testing.T) {
	task := auditTaskFixture()
	signer := mustAuditSigner(t, "w-1", 1)

	// Genuine match => signed Pass.
	good, err := SignAuditVote(context.Background(), signer, task, matchingEvidence(task))
	if err != nil {
		t.Fatalf("genuine pass: %v", err)
	}
	if good.Outcome != AuditVotePass {
		t.Fatalf("matching evidence must sign a Pass, got %s", good.Outcome)
	}
	if err := VerifyAuditVoteSignature(good, signer.PublicKey()); err != nil {
		t.Fatalf("genuine vote must verify: %v", err)
	}

	// Mismatch => a signed Fail, never a forged Pass.
	bad := matchingEvidence(task)
	bad.ActiveParts[0].PartRowLtHash = "tampered"
	badVote, err := SignAuditVote(context.Background(), signer, task, bad)
	if err != nil {
		t.Fatalf("fail vote: %v", err)
	}
	if badVote.Outcome != AuditVoteFail {
		t.Fatal("a mismatch must never be signed as a Pass")
	}
	if err := VerifyAuditVoteSignature(badVote, signer.PublicKey()); err != nil {
		t.Fatalf("even a Fail vote must be a valid signature: %v", err)
	}
}

func TestVerifyAuditVoteSignature_RejectsTamperAndWrongKey(t *testing.T) {
	task := auditTaskFixture()
	signer := mustAuditSigner(t, "w-1", 1)
	v, _ := SignAuditVote(context.Background(), signer, task, matchingEvidence(task))

	tamperedHash := v
	tamperedHash.AuditHash = "tampered"
	if err := VerifyAuditVoteSignature(tamperedHash, signer.PublicKey()); err == nil {
		t.Fatal("tampered audit hash must fail verification")
	}
	tamperedOutcome := v
	tamperedOutcome.Outcome = AuditVoteFail
	if err := VerifyAuditVoteSignature(tamperedOutcome, signer.PublicKey()); err == nil {
		t.Fatal("tampered outcome must fail verification")
	}
	other := mustAuditSigner(t, "w-2", 2)
	if err := VerifyAuditVoteSignature(v, other.PublicKey()); err == nil {
		t.Fatal("wrong key must fail verification")
	}
}

func TestDeriveAuditDecision(t *testing.T) {
	task := auditTaskFixture()
	participants := task.Participants
	h, _ := ComputeAuditHash(task.ExpectedActiveParts)
	pass := func(w string) SafeAuditVote {
		return SafeAuditVote{SnapshotID: "snap-1", WorkerID: w, Outcome: AuditVotePass, AuditHash: h}
	}
	fail := func(w string) SafeAuditVote {
		return SafeAuditVote{SnapshotID: "snap-1", WorkerID: w, Outcome: AuditVoteFail}
	}

	t.Run("3/3 pass", func(t *testing.T) {
		d, q, err := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), pass("w-2"), pass("w-3")}, participants, 2)
		if err != nil || d != AuditDecisionPass || len(q) != 0 {
			t.Fatalf("3/3 must Pass with no quarantine: %s %v %v", d, q, err)
		}
	})
	t.Run("2/3 pass quarantines minority", func(t *testing.T) {
		d, q, _ := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), pass("w-2"), fail("w-3")}, participants, 2)
		if d != AuditDecisionPassWithQuarantine || len(q) != 1 || q[0] != "w-3" {
			t.Fatalf("2/3 must PassWithQuarantine minority w-3: %s %v", d, q)
		}
	})
	t.Run("timeout (no vote) is a non-agreeing participant", func(t *testing.T) {
		d, q, _ := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), pass("w-2")}, participants, 2)
		if d != AuditDecisionPassWithQuarantine || len(q) != 1 || q[0] != "w-3" {
			t.Fatalf("missing w-3 vote must quarantine w-3: %s %v", d, q)
		}
	})
	t.Run("no majority fails", func(t *testing.T) {
		d, q, _ := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), fail("w-2"), fail("w-3")}, participants, 2)
		if d != AuditDecisionFailed || len(q) != 0 {
			t.Fatalf("1/3 must Fail with no quarantine: %s %v", d, q)
		}
	})
	t.Run("pass votes on different hashes do not agree", func(t *testing.T) {
		v1 := pass("w-1")
		v2 := SafeAuditVote{SnapshotID: "snap-1", WorkerID: "w-2", Outcome: AuditVotePass, AuditHash: "different"}
		v3 := SafeAuditVote{SnapshotID: "snap-1", WorkerID: "w-3", Outcome: AuditVotePass, AuditHash: "another"}
		d, _, _ := DeriveAuditDecision([]SafeAuditVote{v1, v2, v3}, participants, 2)
		if d != AuditDecisionFailed {
			t.Fatalf("three disjoint hashes have no 2-agree majority: %s", d)
		}
	})
	t.Run("order-independent", func(t *testing.T) {
		a, qa, _ := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), pass("w-2"), fail("w-3")}, participants, 2)
		b, qb, _ := DeriveAuditDecision([]SafeAuditVote{fail("w-3"), pass("w-2"), pass("w-1")}, participants, 2)
		if a != b || len(qa) != len(qb) || qa[0] != qb[0] {
			t.Fatal("decision must be independent of vote order")
		}
	})
	t.Run("non-participant vote ignored", func(t *testing.T) {
		d, _, _ := DeriveAuditDecision([]SafeAuditVote{pass("w-1"), pass("w-2"), pass("intruder")}, participants, 2)
		if d != AuditDecisionPassWithQuarantine {
			t.Fatalf("intruder vote must not count toward the 3/3 unanimity: %s", d)
		}
	})
	if _, _, err := DeriveAuditDecision(nil, nil, 2); err == nil {
		t.Fatal("no participants must error")
	}
}

func TestAuditEnums_StableStrings(t *testing.T) {
	for _, o := range []AuditVoteOutcome{AuditVotePass, AuditVoteFail} {
		if o.String() == "" || o.String() == "Unknown" {
			t.Fatalf("outcome %d needs a stable string", o)
		}
	}
	for _, r := range []AuditFailReason{AuditNoFailure, AuditActiveSetMismatch, AuditPartMetadataMismatch, AuditChecksumMismatch, AuditRowHashMismatch, AuditReadbackMissing} {
		if r.String() == "" || r.String() == "Unknown" {
			t.Fatalf("reason %d needs a stable string", r)
		}
	}
	for _, d := range []AuditDecision{AuditDecisionFailed, AuditDecisionPass, AuditDecisionPassWithQuarantine} {
		if d.String() == "" || d.String() == "Unknown" {
			t.Fatalf("decision %d needs a stable string", d)
		}
	}
}
