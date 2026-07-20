package storageintegrity

import "testing"

// requireCompanionMutationConsensus fails closed while the companion C2
// mutation-consensus seam is absent. The mutation end-to-end tests assert the
// behavior HouseGate must have once SubmitMutation / SubmitMutationClaim /
// PublishMutationSafeCut exist in the Sentio companion topology. Until then
// there is no real mutation service to exercise, and this guard skips those
// tests with a message naming the missing seam so a red (skipped) run is never
// mistaken for a green (passed) one. It is deliberately independent of
// requireCompanionStagedIntake: C2 (mutation consensus) is a distinct companion
// capability from C1 (staged prepare), and coupling them would let a C1-only
// landing wrongly un-skip mutation tests.
func requireCompanionMutationConsensus(t *testing.T) {
	t.Helper()
	if !CompanionMutationConsensusAvailable {
		t.Skip("companion C2 mutation-consensus seam absent: SubmitMutation / " +
			"SubmitMutationClaim / PublishMutationSafeCut are not exposed by the " +
			"Sentio arbiter/arbiter-proto topology (INSERT-only); end-to-end mutation " +
			"consensus is blocked until the companion seam lands (see " +
			"CompanionMutationConsensusAvailable)")
	}
}

// --- Green-today: pure contract projection, version stamping, equality key. ---

func TestMutationContractVersion_IsPinned(t *testing.T) {
	if MutationContractVersion != "sentio-mutation-contract-v1" {
		t.Fatalf("MutationContractVersion drifted: %q", MutationContractVersion)
	}
}

func mutationEnvelopeFixture() MutationStatementEnvelope {
	return MutationStatementEnvelope{
		ContractVersion:    MutationContractVersion,
		StatementID:        "m-1",
		StatementKind:      KindUpdate,
		SQL:                "ALTER TABLE events UPDATE v = 1 WHERE k = 2",
		SQLHash:            "sha256:sql",
		TargetTableID:      "net1.events",
		SchemaSnapshotID:   "schema-1",
		ExecutorProfileID:  "profile-1",
		PrevSafeSnapshotID: "safe-1",
		AffectedPartitions: []AffectedPartition{{TableID: "net1.events", PartitionID: "20260720"}},
		Signer:             "0xabc",
	}
}

func TestMutationEnvelope_Valid_AcceptsStampedVersion(t *testing.T) {
	if err := mutationEnvelopeFixture().Valid(); err != nil {
		t.Fatalf("stamped envelope must validate: %v", err)
	}
	d := mutationEnvelopeFixture()
	d.StatementKind = KindDelete
	if err := d.Valid(); err != nil {
		t.Fatalf("DELETE envelope must validate: %v", err)
	}
}

func TestMutationEnvelope_Valid_RejectsBlankVersion(t *testing.T) {
	e := mutationEnvelopeFixture()
	e.ContractVersion = ""
	if err := e.Valid(); err == nil {
		t.Fatal("blank contract version must fail closed")
	}
}

func TestMutationEnvelope_Valid_RejectsWrongVersion(t *testing.T) {
	e := mutationEnvelopeFixture()
	e.ContractVersion = "sentio-mutation-contract-v0"
	if err := e.Valid(); err == nil {
		t.Fatal("wrong contract version must fail closed (consume only the pinned shape)")
	}
}

func TestMutationEnvelope_Valid_RejectsInsertKind(t *testing.T) {
	e := mutationEnvelopeFixture()
	e.StatementKind = KindInsert
	if err := e.Valid(); err == nil {
		t.Fatal("INSERT is not a mutation; must fail closed")
	}
}

func TestMutationEnvelope_Valid_RejectsMissingBoundField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MutationStatementEnvelope)
	}{
		{"blank statement id", func(e *MutationStatementEnvelope) { e.StatementID = "" }},
		{"blank target table", func(e *MutationStatementEnvelope) { e.TargetTableID = "" }},
		{"blank schema snapshot", func(e *MutationStatementEnvelope) { e.SchemaSnapshotID = "" }},
		{"blank executor profile", func(e *MutationStatementEnvelope) { e.ExecutorProfileID = "" }},
		{"blank prev safe snapshot", func(e *MutationStatementEnvelope) { e.PrevSafeSnapshotID = "" }},
		{"no affected partitions", func(e *MutationStatementEnvelope) { e.AffectedPartitions = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := mutationEnvelopeFixture()
			tc.mutate(&e)
			if err := e.Valid(); err == nil {
				t.Fatal("missing bound field must fail closed")
			}
		})
	}
}

func mutationClaimFixture() MutationClaim {
	return MutationClaim{
		ContractVersion:          MutationContractVersion,
		MutationID:               "m-1",
		WorkerID:                 "w-1",
		StatementKind:            KindUpdate,
		PostStateRoot:            "post-root-1",
		PartitionDeltas:          []PartitionDelta{{TableID: "net1.events", PartitionID: "20260720", AddLtHashSum: "add", RemoveLtHashSum: "rem", RowsUpdated: 3}},
		PostPartitionCommitments: []PartitionCommitment{{TableID: "net1.events", PartitionID: "20260720", Commitment: "post-c"}},
		SchemaSnapshotID:         "schema-1",
		ExecutorProfileID:        "profile-1",
		PrevSafeSnapshotID:       "safe-1",
		BasePartitionRoots:       []PartitionCommitment{{TableID: "net1.events", PartitionID: "20260720", Commitment: "base-c"}},
		AffectedPartitions:       []AffectedPartition{{TableID: "net1.events", PartitionID: "20260720"}},
		RowsBefore:               10,
		RowsAfter:                10,
		Signature:                "sig",
	}
}

func TestMutationClaim_Valid_RequiresFullEqualityKey(t *testing.T) {
	if err := mutationClaimFixture().Valid(); err != nil {
		t.Fatalf("complete claim must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*MutationClaim)
	}{
		{"blank post state root", func(c *MutationClaim) { c.PostStateRoot = "" }},
		{"nil partition deltas", func(c *MutationClaim) { c.PartitionDeltas = nil }},
		{"nil post commitments", func(c *MutationClaim) { c.PostPartitionCommitments = nil }},
		{"blank schema snapshot", func(c *MutationClaim) { c.SchemaSnapshotID = "" }},
		{"blank executor profile", func(c *MutationClaim) { c.ExecutorProfileID = "" }},
		{"blank prev safe snapshot", func(c *MutationClaim) { c.PrevSafeSnapshotID = "" }},
		{"nil base roots", func(c *MutationClaim) { c.BasePartitionRoots = nil }},
		{"nil affected partitions", func(c *MutationClaim) { c.AffectedPartitions = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := mutationClaimFixture()
			tc.mutate(&c)
			if err := c.Valid(); err == nil {
				t.Fatal("a claim missing an equality-key field must fail closed")
			}
		})
	}
}

func TestDeriveEqualityKey_ProjectsAllEightFields(t *testing.T) {
	c := mutationClaimFixture()
	k := DeriveEqualityKey(c)
	if k.PostStateRoot != c.PostStateRoot ||
		k.SchemaSnapshotID != c.SchemaSnapshotID ||
		k.ExecutorProfileID != c.ExecutorProfileID ||
		k.PrevSafeSnapshotID != c.PrevSafeSnapshotID ||
		len(k.PartitionDeltas) != len(c.PartitionDeltas) ||
		len(k.PostPartitionCommitments) != len(c.PostPartitionCommitments) ||
		len(k.BasePartitionRoots) != len(c.BasePartitionRoots) ||
		len(k.AffectedPartitions) != len(c.AffectedPartitions) {
		t.Fatal("DeriveEqualityKey must copy exactly the 8 equality-key fields")
	}
}

func TestEqualityKey_Equal_TrueForIdenticalClaims(t *testing.T) {
	if !DeriveEqualityKey(mutationClaimFixture()).Equal(DeriveEqualityKey(mutationClaimFixture())) {
		t.Fatal("identical claims must derive equal keys")
	}
}

func TestEqualityKey_Equal_OrderInsensitive(t *testing.T) {
	a := mutationClaimFixture()
	a.PartitionDeltas = append(a.PartitionDeltas, PartitionDelta{TableID: "net1.events", PartitionID: "20260719", AddLtHashSum: "x"})
	a.PostPartitionCommitments = append(a.PostPartitionCommitments, PartitionCommitment{TableID: "net1.events", PartitionID: "20260719", Commitment: "c2"})
	a.BasePartitionRoots = append(a.BasePartitionRoots, PartitionCommitment{TableID: "net1.events", PartitionID: "20260719", Commitment: "b2"})
	a.AffectedPartitions = append(a.AffectedPartitions, AffectedPartition{TableID: "net1.events", PartitionID: "20260719"})

	b := a
	// b has the same elements but reversed slice order.
	b.PartitionDeltas = []PartitionDelta{a.PartitionDeltas[1], a.PartitionDeltas[0]}
	b.PostPartitionCommitments = []PartitionCommitment{a.PostPartitionCommitments[1], a.PostPartitionCommitments[0]}
	b.BasePartitionRoots = []PartitionCommitment{a.BasePartitionRoots[1], a.BasePartitionRoots[0]}
	b.AffectedPartitions = []AffectedPartition{a.AffectedPartitions[1], a.AffectedPartitions[0]}

	if !DeriveEqualityKey(a).Equal(DeriveEqualityKey(b)) {
		t.Fatal("equality-key comparison must be order-insensitive across partition-keyed slices")
	}
}

func TestEqualityKey_Equal_FalseOnPostStateRootDiff(t *testing.T) {
	b := mutationClaimFixture()
	b.PostStateRoot = "different-root"
	if DeriveEqualityKey(mutationClaimFixture()).Equal(DeriveEqualityKey(b)) {
		t.Fatal("differing post state root must not be equal")
	}
}

func TestEqualityKey_Equal_FalseOnDeltaDiff(t *testing.T) {
	b := mutationClaimFixture()
	b.PartitionDeltas = []PartitionDelta{{TableID: "net1.events", PartitionID: "20260720", AddLtHashSum: "add", RemoveLtHashSum: "rem", RowsUpdated: 3, RowsDeleted: 99}}
	if DeriveEqualityKey(mutationClaimFixture()).Equal(DeriveEqualityKey(b)) {
		t.Fatal("differing partition delta must not be equal (grouping cannot collapse on post_state_root alone)")
	}
}

func TestEqualityKey_Equal_FalseOnBaseRootDiff(t *testing.T) {
	b := mutationClaimFixture()
	b.BasePartitionRoots = []PartitionCommitment{{TableID: "net1.events", PartitionID: "20260720", Commitment: "different-base"}}
	if DeriveEqualityKey(mutationClaimFixture()).Equal(DeriveEqualityKey(b)) {
		t.Fatal("differing base partition roots must not be equal")
	}
}

func publicationAckFixture() PublicationAck {
	return PublicationAck{
		ContractVersion:          MutationContractVersion,
		MutationID:               "m-1",
		WorkerID:                 "w-1",
		PublicationSeq:           1,
		BaseSafeSnapshotID:       "safe-1",
		BasePartitionRoots:       []PartitionCommitment{{TableID: "net1.events", PartitionID: "20260720", Commitment: "base-c"}},
		PostPartitionCommitments: []PartitionCommitment{{TableID: "net1.events", PartitionID: "20260720", Commitment: "post-c"}},
		PostStateRoot:            "post-root-1",
		LocalSafeSnapshotIDAfter: "safe-2",
		ExactActivePartsReadback: []CandidatePart{{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_1_1_0"}},
		Applied:                  true,
	}
}

func TestPublicationAck_Valid_RequiresBoundFields(t *testing.T) {
	if err := publicationAckFixture().Valid(); err != nil {
		t.Fatalf("complete ack must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*PublicationAck)
	}{
		{"blank mutation id", func(a *PublicationAck) { a.MutationID = "" }},
		{"blank worker id", func(a *PublicationAck) { a.WorkerID = "" }},
		{"zero publication seq", func(a *PublicationAck) { a.PublicationSeq = 0 }},
		{"blank base snapshot", func(a *PublicationAck) { a.BaseSafeSnapshotID = "" }},
		{"nil base roots", func(a *PublicationAck) { a.BasePartitionRoots = nil }},
		{"blank local snapshot after", func(a *PublicationAck) { a.LocalSafeSnapshotIDAfter = "" }},
		{"non-empty post missing readback", func(a *PublicationAck) { a.ExactActivePartsReadback = nil }},
		{"non-empty post missing commitments", func(a *PublicationAck) { a.PostPartitionCommitments = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := publicationAckFixture()
			tc.mutate(&a)
			if err := a.Valid(); err == nil {
				t.Fatal("missing bound ack field must fail closed")
			}
		})
	}
}

// TestPublicationAck_Valid_AcceptsEmptyDeletePost confirms an empty-DELETE post
// (zero post commitment + empty readback) is a legitimate ack shape.
func TestPublicationAck_Valid_AcceptsEmptyDeletePost(t *testing.T) {
	a := publicationAckFixture()
	a.PostStateRoot = ""
	a.PostPartitionCommitments = nil
	a.ExactActivePartsReadback = nil
	if err := a.Valid(); err != nil {
		t.Fatalf("empty-DELETE post ack must validate: %v", err)
	}
}

func TestPublicationAck_ReadbackIsCandidatePart(t *testing.T) {
	a := publicationAckFixture()
	want := CandidatePart{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_1_1_0"}
	if a.ExactActivePartsReadback[0] != want {
		t.Fatal("exact_active_parts_readback must round-trip a CandidatePart unchanged")
	}
}

func TestPublishMutationSafeCut_ServingFloorIsProfileConstant(t *testing.T) {
	if MutationServingAvailabilityFloor != 2 {
		t.Fatalf("P2 v1 serving availability floor must be 2, got %d", MutationServingAvailabilityFloor)
	}
}

// --- Gated: end-to-end mutation consensus needs the absent C2 seam. ---

func TestSubmitMutation_SequencesToAck1(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion mutation service lands")
}

func TestMutationClaimQuorum_TwoOfThreeReachesProvisional(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion mutation FSM lands")
}

func TestPublishMutationSafeCut_AtomicCutReturnsAck3(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion safe-cut RPC lands")
}

func TestMutationEndToEnd_IngressToAck3(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the full companion mutation consensus path lands")
}
