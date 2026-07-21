package storageintegrity

import "testing"

func cutFixture() SafeCutView {
	return NewSafeCutView(
		"manifest-1", 10,
		map[string]uint64{"w-1": 10, "w-2": 10, "w-3": 8},
		map[string]bool{"w-1": true, "w-2": true},
		3,
		map[string]bool{"w-4": true},
	)
}

func TestSafeCutView_ValidAcceptsCompleteCut(t *testing.T) {
	if err := cutFixture().Valid(); err != nil {
		t.Fatalf("complete cut must validate: %v", err)
	}
}

func TestSafeCutView_ValidRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SafeCutView)
	}{
		{"blank manifest", func(v *SafeCutView) { v.ManifestID = "" }},
		{"zero global watermark", func(v *SafeCutView) { v.GlobalWatermark = 0 }},
		{"zero route-cache epoch", func(v *SafeCutView) { v.RouteCacheEpoch = 0 }},
		{"nil read-set", func(v *SafeCutView) { v.ReadSet = nil }},
		{"read-set worker missing watermark", func(v *SafeCutView) { v.ReadSet["w-9"] = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := cutFixture()
			tc.mutate(&v)
			if err := v.Valid(); err == nil {
				t.Fatal("invalid cut must fail closed")
			}
		})
	}
}

func TestNewSafeCutView_DefensiveCopiesMaps(t *testing.T) {
	readSet := map[string]bool{"w-1": true}
	wm := map[string]uint64{"w-1": 10}
	v := NewSafeCutView("m", 10, wm, readSet, 3, nil)
	readSet["w-2"] = true // mutate caller input after construction
	wm["w-1"] = 999
	if v.ReadSet["w-2"] || v.PerWorkerWatermark["w-1"] != 10 {
		t.Fatal("NewSafeCutView must defensively copy its input maps")
	}
	c := v.Clone()
	c.ReadSet["w-3"] = true
	if v.ReadSet["w-3"] {
		t.Fatal("Clone must be independent")
	}
}

func TestSafeReadGate_MayServe(t *testing.T) {
	gate, err := NewSafeReadGate(cutFixture())
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	cases := []struct {
		name    string
		worker  string
		snap    uint64
		allowed bool
		reason  GateDenyReason
	}{
		{"in read-set covering watermarks", "w-1", 10, true, GateAllowed},
		{"not in read-set", "w-3", 8, false, GateDenyNotInReadSet},
		{"quarantined not in read-set", "w-4", 1, false, GateDenyNotInReadSet},
		{"worker watermark behind", "w-2", 11, false, GateDenyWorkerWatermarkBehind},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := gate.MayServe(tc.worker, tc.snap)
			if d.Allowed != tc.allowed || d.Reason != tc.reason {
				t.Fatalf("MayServe(%s,%d) = {allowed=%v reason=%s}, want {%v %s}", tc.worker, tc.snap, d.Allowed, d.Reason, tc.allowed, tc.reason)
			}
		})
	}
}

func TestSafeReadGate_MayServe_DeniesQuarantinedInReadSet(t *testing.T) {
	cut := NewSafeCutView("m", 10, map[string]uint64{"w-1": 10}, map[string]bool{"w-1": true}, 3, map[string]bool{"w-1": true})
	gate, _ := NewSafeReadGate(cut)
	d := gate.MayServe("w-1", 10)
	if d.Allowed || d.Reason != GateDenyQuarantined {
		t.Fatalf("a quarantined in-read-set worker must be denied Quarantined, got %+v", d)
	}
}

func TestSafeReadGate_MayServe_DeniesGlobalWatermarkBehind(t *testing.T) {
	cut := NewSafeCutView("m", 5, map[string]uint64{"w-1": 10}, map[string]bool{"w-1": true}, 3, nil)
	gate, _ := NewSafeReadGate(cut)
	d := gate.MayServe("w-1", 8) // worker wm 10 >= 8, but global 5 < 8
	if d.Allowed || d.Reason != GateDenyGlobalWatermarkBehind {
		t.Fatalf("global watermark behind must deny, got %+v", d)
	}
}

// TestSafeReadGate_DeniesSingleWorkerAckNotYetCommitted models a worker that
// locally applied (Applied=true) but is NOT in the committed read-set: it must
// be denied, proving no single-worker-ack early serving (design section 5.2).
func TestSafeReadGate_DeniesSingleWorkerAckNotYetCommitted(t *testing.T) {
	// The cut's read-set does not include w-locally-applied.
	cut := NewSafeCutView("m", 10, map[string]uint64{"w-1": 10}, map[string]bool{"w-1": true}, 3, nil)
	gate, _ := NewSafeReadGate(cut)
	d := gate.MayServe("w-locally-applied", 10)
	if d.Allowed || d.Reason != GateDenyNotInReadSet {
		t.Fatalf("a locally-applied worker absent from the committed cut must be denied, got %+v", d)
	}
}

func equationInputFixture() PublicationEquationInput {
	return PublicationEquationInput{
		RequiredServingSet:       []string{"w-1", "w-2", "w-3"},
		RetainedServingSet:       []string{"w-1", "w-2"},
		AppliedEquivalentSet:     []string{"w-1", "w-2", "w-3"},
		ExcludedBeforeCut:        []string{"w-3"},
		ServingAvailabilityFloor: 2,
		CanonicalReadbackDigest:  "canon",
		RetainedReadbackDigests:  map[string]string{"w-1": "canon", "w-2": "canon"},
	}
}

func TestVerifyPublicationEquation_AcceptsWhenAllHold(t *testing.T) {
	if err := VerifyPublicationEquation(equationInputFixture()); err != nil {
		t.Fatalf("valid equation must pass: %v", err)
	}
}

func TestVerifyPublicationEquation_Rejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PublicationEquationInput)
	}{
		{"floor unmet", func(in *PublicationEquationInput) {
			in.RetainedServingSet = []string{"w-1"}
			in.ExcludedBeforeCut = []string{"w-2", "w-3"}
			in.RetainedReadbackDigests = map[string]string{"w-1": "canon"}
		}},
		{"retained not subset of applied", func(in *PublicationEquationInput) { in.AppliedEquivalentSet = []string{"w-1"} }},
		{"overlap retained and excluded", func(in *PublicationEquationInput) { in.ExcludedBeforeCut = []string{"w-2", "w-3"} }},
		{"coverage mismatch", func(in *PublicationEquationInput) { in.RequiredServingSet = []string{"w-1", "w-2", "w-3", "w-4"} }},
		{"missing readback", func(in *PublicationEquationInput) { delete(in.RetainedReadbackDigests, "w-2") }},
		{"readback mismatch", func(in *PublicationEquationInput) { in.RetainedReadbackDigests["w-2"] = "wrong" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := equationInputFixture()
			tc.mutate(&in)
			if err := VerifyPublicationEquation(in); err == nil {
				t.Fatal("equation violation must fail closed")
			}
		})
	}
}

func TestVerifyPublicationEquation_DefaultsFloorToProfileConstant(t *testing.T) {
	in := equationInputFixture()
	in.ServingAvailabilityFloor = 0 // unset → defaults to MutationServingAvailabilityFloor (2)
	if err := VerifyPublicationEquation(in); err != nil {
		t.Fatalf("floor should default to the profile constant: %v", err)
	}
}

func TestGateDenyReason_StringStable(t *testing.T) {
	for _, r := range []GateDenyReason{GateAllowed, GateDenyNotInReadSet, GateDenyQuarantined, GateDenyWorkerWatermarkBehind, GateDenyGlobalWatermarkBehind, GateDenyUnknownWorker} {
		if r.String() == "" || r.String() == "Unknown" {
			t.Fatalf("reason %d must have a stable non-empty string", r)
		}
	}
}

// --- Gated: consuming a real Arbiter-published cut needs the absent C2 seam. ---

func TestConsumeArbiterPublishedSafeCut_ReflectsAtomicCut(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until a real PublishMutationSafeCut result can be consumed")
}

func TestSafeReadGate_ServesOnlyAfterCommittedCut_EndToEnd(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion atomic safe-cut lands")
}
