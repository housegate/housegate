package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func reentryManifestParts() []replay.PartManifestEntry {
	return []replay.PartManifestEntry{
		{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartRowLtHash: "lh1", RowCount: 5, Bytes: 50},
		{TableID: "net1.events", PartitionID: "p2", PartName: "p2_1_1_0", PartRowLtHash: "lh2", RowCount: 7, Bytes: 70},
	}
}

func matchingReadback() []CandidatePart {
	var out []CandidatePart
	for _, p := range reentryManifestParts() {
		out = append(out, CandidatePart{
			TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName,
			PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes,
		})
	}
	return out
}

func fullyRepairedInput() ReadSetReentryInput {
	return ReadSetReentryInput{
		WorkerID:            "w-3",
		TargetManifestID:    "m-2",
		ManifestActiveParts: reentryManifestParts(),
		WorkerReadbackParts: matchingReadback(),
		RepairSource:        RepairSourceAuthoritativeManifest,
		StillQuarantined:    false,
		ServingAuditPassed:  true,
	}
}

func TestVerifyReadbackAgainstManifest(t *testing.T) {
	if err := VerifyReadbackAgainstManifest(reentryManifestParts(), matchingReadback()); err != nil {
		t.Fatalf("exact match must verify: %v", err)
	}
	// Order-insensitive.
	rb := matchingReadback()
	rb[0], rb[1] = rb[1], rb[0]
	if err := VerifyReadbackAgainstManifest(reentryManifestParts(), rb); err != nil {
		t.Fatalf("order must not matter: %v", err)
	}
	cases := []struct {
		name   string
		mutate func([]CandidatePart) []CandidatePart
	}{
		{"missing part", func(r []CandidatePart) []CandidatePart { return r[:1] }},
		{"extra part", func(r []CandidatePart) []CandidatePart {
			return append(r, CandidatePart{TableID: "net1.events", PartitionID: "p9", PartName: "x", RowCount: 1, Bytes: 1})
		}},
		{"checksum drift", func(r []CandidatePart) []CandidatePart { r[0].PartRowLtHash = "bad"; return r }},
		{"rows drift", func(r []CandidatePart) []CandidatePart { r[0].RowCount = 999; return r }},
		// A duplicate readback part must not "cover" for a missing manifest part
		// when the slice lengths coincide: manifest {A,B} vs readback [A,A].
		{"duplicate readback masks a missing part", func(r []CandidatePart) []CandidatePart {
			return []CandidatePart{r[0], r[0]}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := VerifyReadbackAgainstManifest(reentryManifestParts(), tc.mutate(matchingReadback())); err == nil {
				t.Fatal("mismatch must fail closed")
			}
		})
	}
}

func TestDecideReadSetReentry_EligibleOnlyWhenAllGatesPass(t *testing.T) {
	d := DecideReadSetReentry(fullyRepairedInput())
	if !d.EligibleForReadSet || d.Stage != RepairStageReentered || d.Reason != ReentryAllowed {
		t.Fatalf("fully repaired worker must re-enter: %+v", d)
	}
}

func TestDecideReadSetReentry_FailClosedMatrix(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*ReadSetReentryInput)
		wantStage  RepairStage
		wantReason ReentryDenyReason
	}{
		{"still quarantined dominates", func(in *ReadSetReentryInput) { in.StillQuarantined = true }, RepairStageExcluded, ReentryDenyStillQuarantined},
		{"illegal repair source", func(in *ReadSetReentryInput) { in.RepairSource = RepairSourceUnknown }, RepairStageSyncing, ReentryDenyIllegalRepairSource},
		{"exact verification failed", func(in *ReadSetReentryInput) { in.WorkerReadbackParts[0].PartRowLtHash = "tampered" }, RepairStageSynced, ReentryDenyExactVerificationFailed},
		{"audit not passed", func(in *ReadSetReentryInput) { in.ServingAuditPassed = false }, RepairStageAuditPending, ReentryDenyAuditNotPassed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := fullyRepairedInput()
			tc.mutate(&in)
			d := DecideReadSetReentry(in)
			if d.EligibleForReadSet {
				t.Fatalf("%s must not be eligible", tc.name)
			}
			if d.Stage != tc.wantStage {
				t.Fatalf("%s stage = %s, want %s", tc.name, d.Stage, tc.wantStage)
			}
			if d.Reason != tc.wantReason {
				t.Fatalf("%s reason = %s, want %s", tc.name, d.Reason, tc.wantReason)
			}
		})
	}
}

func TestDecideReadSetReentry_QuarantineDominatesEvenWhenOtherwiseReady(t *testing.T) {
	// A worker that synced, verified, and passed audit is STILL held out while
	// quarantined — quarantine dominates the whole progression.
	in := fullyRepairedInput()
	in.StillQuarantined = true
	if DecideReadSetReentry(in).EligibleForReadSet {
		t.Fatal("quarantine must dominate all other gates")
	}
}

func TestRepairEnums_StableStrings(t *testing.T) {
	for _, s := range []RepairSource{RepairSourceAuthoritativeManifest, RepairSourceCanonicalPeer} {
		if s.String() == "" || s.String() == "Unknown" {
			t.Fatalf("source %d needs a stable string", s)
		}
	}
	for _, s := range []RepairStage{RepairStageExcluded, RepairStageSyncing, RepairStageSynced, RepairStageVerified, RepairStageAuditPending, RepairStageAuditPassed, RepairStageReentered} {
		if s.String() == "" || s.String() == "Unknown" {
			t.Fatalf("stage %d needs a stable string", s)
		}
	}
	for _, r := range []ReentryDenyReason{ReentryAllowed, ReentryDenyStillQuarantined, ReentryDenyIllegalRepairSource, ReentryDenyExactVerificationFailed, ReentryDenyAuditNotPassed} {
		if r.String() == "" || r.String() == "Unknown" {
			t.Fatalf("reason %d needs a stable string", r)
		}
	}
}

// --- fake gated ports ---

type fakeRepairSyncer struct {
	source   RepairSource
	readback []CandidatePart
}

func (f fakeRepairSyncer) SyncToManifest(context.Context, string, string, []AffectedPartition) (RepairSource, []CandidatePart, error) {
	return f.source, f.readback, nil
}

type fakeReentryAuditor struct{ passed bool }

func (f fakeReentryAuditor) ServingAuditPassed(context.Context, string, string) (bool, error) {
	return f.passed, nil
}

func TestNewRepairWorker_RequiresPorts(t *testing.T) {
	if _, err := NewRepairWorker("", fakeRepairSyncer{}, fakeReentryAuditor{}); err == nil {
		t.Fatal("blank worker id must fail")
	}
	if _, err := NewRepairWorker("w-3", nil, fakeReentryAuditor{}); err == nil {
		t.Fatal("nil syncer must fail")
	}
	if _, err := NewRepairWorker("w-3", fakeRepairSyncer{}, nil); err == nil {
		t.Fatal("nil auditor must fail")
	}
	if _, err := NewRepairWorker("w-3", fakeRepairSyncer{}, fakeReentryAuditor{}); err != nil {
		t.Fatalf("well-formed wiring must succeed: %v", err)
	}
}

func TestRepairWorker_Recover_FailsClosedWhenSeamAbsent(t *testing.T) {
	w, _ := NewRepairWorker("w-3", fakeRepairSyncer{source: RepairSourceAuthoritativeManifest, readback: matchingReadback()}, fakeReentryAuditor{passed: true})
	if _, err := w.Recover(context.Background(), "m-2", reentryManifestParts(), false); err == nil {
		t.Fatal("Recover must fail closed while the companion repair/SafeAudit seam is absent")
	}
}

// --- Gated: the real authoritative sync + SafeAudit re-entry need the absent seam. ---

func TestRepairWorker_RecoversFromAuthoritativeManifestAndReenters(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion authoritative-sync + C3 SafeAudit re-entry seam lands")
}
