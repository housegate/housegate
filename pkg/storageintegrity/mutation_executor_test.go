package storageintegrity

import (
	"context"
	"testing"
)

// mutationTaskFixture returns a complete UPDATE task over one partition, with a
// frozen base-partition-root the scratch must reproduce.
func mutationTaskFixture() MutationTask {
	return MutationTask{
		ContractVersion:    MutationContractVersion,
		MutationID:         "m-1",
		StatementID:        "m-1",
		WorkerID:           "",
		PrevSafeSnapshotID: "safe-1",
		SchemaSnapshotID:   "schema-1",
		ExecutorProfileID:  "profile-1",
		AffectedPartitions: []AffectedPartition{{TableID: "net1.events", PartitionID: "p1"}},
		BasePartitionRoots: []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "base-p1"}},
		MaterializedSQL:    "ALTER TABLE net1.events UPDATE v = 1 WHERE p = 'p1'",
	}
}

func mutationTaskFixtureKind(k Kind) MutationTask {
	t := mutationTaskFixture()
	t.StatementKind = k
	return t
}

// scratchResultFixture is the "everything correct" readback: scratch initial
// roots equal the task base roots, and every affected partition has a post
// commitment + delta.
func scratchResultFixture() ScratchReplayResult {
	return ScratchReplayResult{
		MutationID:               "m-1",
		WorkerID:                 "w-1",
		ScratchInitialBaseRoots:  []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "base-p1"}},
		PostStateRoot:            "post-root-1",
		PostPartitionCommitments: []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "post-p1"}},
		PartitionDeltas:          []PartitionDelta{{TableID: "net1.events", PartitionID: "p1", RowsUpdated: 3}},
		RowsBefore:               10,
		RowsAfter:                10,
	}
}

// --- Green-today: pure claim assembly / equality key / signing. ---

func TestAssembleMutationClaim_BindsAllEqualityKeyFields(t *testing.T) {
	claim, err := AssembleMutationClaim(mutationTaskFixtureKind(KindUpdate), scratchResultFixture(), "w-1")
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if err := claim.Valid(); err != nil {
		t.Fatalf("assembled claim must be valid: %v", err)
	}
	if claim.PostStateRoot != "post-root-1" ||
		len(claim.PartitionDeltas) != 1 ||
		len(claim.PostPartitionCommitments) != 1 ||
		claim.SchemaSnapshotID != "schema-1" ||
		claim.ExecutorProfileID != "profile-1" ||
		claim.PrevSafeSnapshotID != "safe-1" ||
		len(claim.BasePartitionRoots) != 1 ||
		len(claim.AffectedPartitions) != 1 {
		t.Fatalf("claim missing an equality-key field: %+v", claim)
	}
}

func TestAssembleMutationClaim_RejectsInsertKind(t *testing.T) {
	if _, err := AssembleMutationClaim(mutationTaskFixtureKind(KindInsert), scratchResultFixture(), "w-1"); err == nil {
		t.Fatal("INSERT kind must fail closed")
	}
}

func TestAssembleMutationClaim_RejectsIncompleteResult(t *testing.T) {
	cases := []struct {
		name       string
		mutateTask func(*MutationTask)
		mutateRes  func(*ScratchReplayResult)
	}{
		{"missing post state root", nil, func(r *ScratchReplayResult) { r.PostStateRoot = "" }},
		{"affected partition without post commitment", nil, func(r *ScratchReplayResult) { r.PostPartitionCommitments = nil }},
		{"missing partition deltas", nil, func(r *ScratchReplayResult) { r.PartitionDeltas = nil }},
		{"blank schema snapshot", func(tk *MutationTask) { tk.SchemaSnapshotID = "" }, nil},
		{"blank executor profile", func(tk *MutationTask) { tk.ExecutorProfileID = "" }, nil},
		{"blank prev safe snapshot", func(tk *MutationTask) { tk.PrevSafeSnapshotID = "" }, nil},
		{"empty base roots", func(tk *MutationTask) { tk.BasePartitionRoots = nil }, nil},
		{"no affected partitions", func(tk *MutationTask) { tk.AffectedPartitions = nil }, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := mutationTaskFixtureKind(KindUpdate)
			res := scratchResultFixture()
			if tc.mutateTask != nil {
				tc.mutateTask(&task)
			}
			if tc.mutateRes != nil {
				tc.mutateRes(&res)
			}
			if _, err := AssembleMutationClaim(task, res, "w-1"); err == nil {
				t.Fatal("incomplete result must fail closed")
			}
		})
	}
}

func TestVerifyScratchBaseRoots_MatchPasses(t *testing.T) {
	if err := verifyScratchBaseRoots(mutationTaskFixture(), scratchResultFixture()); err != nil {
		t.Fatalf("matching base roots must pass: %v", err)
	}
}

func TestVerifyScratchBaseRoots_MismatchFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ScratchReplayResult)
	}{
		{"differing root", func(r *ScratchReplayResult) {
			r.ScratchInitialBaseRoots = []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "different"}}
		}},
		{"missing partition", func(r *ScratchReplayResult) { r.ScratchInitialBaseRoots = nil }},
		{"extra partition", func(r *ScratchReplayResult) {
			r.ScratchInitialBaseRoots = append(r.ScratchInitialBaseRoots, PartitionCommitment{TableID: "net1.events", PartitionID: "p2", Commitment: "x"})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := scratchResultFixture()
			tc.mutate(&res)
			if err := verifyScratchBaseRoots(mutationTaskFixture(), res); err == nil {
				t.Fatal("scratch base-root mismatch must fail closed")
			}
		})
	}
}

func claimFixtureForDigest() MutationClaim {
	c, err := AssembleMutationClaim(mutationTaskFixtureKind(KindUpdate), scratchResultFixture(), "w-1")
	if err != nil {
		panic(err)
	}
	return c
}

func TestMutationEqualityKeyDigest_DeterministicAndOnlyEqualityFields(t *testing.T) {
	c := claimFixtureForDigest()
	d1, err := MutationEqualityKeyDigest(c)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	// A non-equality-key field change must NOT change the equality-key digest.
	c2 := c
	c2.WorkerID = "different-worker"
	c2.MutationID = "different-mutation"
	c2.RowsBefore = 999
	d2, err := MutationEqualityKeyDigest(c2)
	if err != nil {
		t.Fatalf("digest2: %v", err)
	}
	if d1 != d2 {
		t.Fatal("equality-key digest must ignore non-equality-key fields (worker/mutation id, rows)")
	}
	// An equality-key field change MUST change it.
	c3 := c
	c3.PostStateRoot = "different-post-root"
	d3, err := MutationEqualityKeyDigest(c3)
	if err != nil {
		t.Fatalf("digest3: %v", err)
	}
	if d1 == d3 {
		t.Fatal("equality-key digest must change when an equality-key field changes")
	}
}

func TestMutationClaimHash_CoversAllFields(t *testing.T) {
	c := claimFixtureForDigest()
	base, _ := MutationClaimHash(c)
	mutators := []func(*MutationClaim){
		func(c *MutationClaim) { c.MutationID = "x" },
		func(c *MutationClaim) { c.WorkerID = "x" },
		func(c *MutationClaim) { c.PostStateRoot = "x" },
		func(c *MutationClaim) { c.RowsBefore = 12345 },
	}
	for i, m := range mutators {
		c2 := c
		m(&c2)
		h, _ := MutationClaimHash(c2)
		if h == base {
			t.Fatalf("claim hash must change when field %d changes", i)
		}
	}
}

func TestEd25519ClaimSigner_SignVerifyRoundTrip(t *testing.T) {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	signer, err := NewEd25519ClaimSigner("w-1", seed)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	sc, err := SignAssembledClaim(context.Background(), signer, claimFixtureForDigest())
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := VerifyMutationClaimSignature(sc, signer.PublicKey()); err != nil {
		t.Fatalf("verify round trip: %v", err)
	}
	// Tampered claim field fails verification (the recomputed hash won't match).
	tampered := sc
	tampered.Claim.PostStateRoot = "tampered"
	if err := VerifyMutationClaimSignature(tampered, signer.PublicKey()); err == nil {
		t.Fatal("tampered claim must fail verification")
	}
	// Wrong public key fails.
	other, _ := NewEd25519ClaimSigner("w-2", make([]byte, 32))
	if err := VerifyMutationClaimSignature(sc, other.PublicKey()); err == nil {
		t.Fatal("wrong public key must fail verification")
	}
}

func TestSignAssembledClaim_PopulatesEqualityKeyAndHash(t *testing.T) {
	seed := make([]byte, 32)
	seed[0] = 7
	signer, _ := NewEd25519ClaimSigner("w-1", seed)
	c := claimFixtureForDigest()
	sc, err := SignAssembledClaim(context.Background(), signer, c)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	wantEKD, _ := MutationEqualityKeyDigest(c)
	wantCH, _ := MutationClaimHash(c)
	if sc.EqualityKeyDigest != wantEKD || sc.ClaimHash != wantCH {
		t.Fatal("signed claim must carry the derived equality-key digest and claim hash")
	}
	if sc.WorkerID != "w-1" || sc.Signature == "" {
		t.Fatalf("signed claim must carry worker id and signature: %+v", sc)
	}
}

// TestMutationEqualityKeyDigest_MatchAcrossPhysicallyDistinctWorkers models the
// section-4.7 2-of-3 grouping: two workers whose logical post-state (roots,
// commitments, deltas, base roots, schema/profile) is identical but whose worker
// ids differ produce the SAME equality-key digest. It asserts equality-key
// equality, not the grouping decision (which is Arbiter FSM work).
func TestMutationEqualityKeyDigest_MatchAcrossPhysicallyDistinctWorkers(t *testing.T) {
	c1, _ := AssembleMutationClaim(mutationTaskFixtureKind(KindUpdate), scratchResultFixture(), "w-1")
	c2, _ := AssembleMutationClaim(mutationTaskFixtureKind(KindUpdate), scratchResultFixture(), "w-2")
	d1, _ := MutationEqualityKeyDigest(c1)
	d2, _ := MutationEqualityKeyDigest(c2)
	if d1 != d2 {
		t.Fatal("logically-equal claims from distinct workers must share an equality-key digest")
	}
}

// --- Gated: real ClickHouse clone/execute needs the absent C2 seam. ---

type fakeScratchExecutor struct {
	res ScratchReplayResult
	err error
}

func (f *fakeScratchExecutor) CloneExecuteAndReadback(_ context.Context, _ MutationTask, _ string) (ScratchReplayResult, error) {
	return f.res, f.err
}

func TestMutationExecutor_Execute_ClonesFrozenBaseAndProducesClaim(t *testing.T) {
	requireCompanionMutationConsensus(t)

	seed := make([]byte, 32)
	seed[0] = 3
	signer, _ := NewEd25519ClaimSigner("w-1", seed)
	exec := NewMutationExecutor(&fakeScratchExecutor{res: scratchResultFixture()}, signer, "w-1")
	sc, err := exec.Execute(context.Background(), mutationTaskFixtureKind(KindUpdate))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if err := VerifyMutationClaimSignature(sc, signer.PublicKey()); err != nil {
		t.Fatalf("produced claim must verify: %v", err)
	}
}

func TestMutationExecutor_Execute_ScratchBaseMismatchFailsClosed(t *testing.T) {
	requireCompanionMutationConsensus(t)

	seed := make([]byte, 32)
	signer, _ := NewEd25519ClaimSigner("w-1", seed)
	bad := scratchResultFixture()
	bad.ScratchInitialBaseRoots = []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "wrong"}}
	exec := NewMutationExecutor(&fakeScratchExecutor{res: bad}, signer, "w-1")
	if _, err := exec.Execute(context.Background(), mutationTaskFixtureKind(KindUpdate)); err == nil {
		t.Fatal("a scratch base-root mismatch must fail closed with no claim")
	}
}
