package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func dropActionFixture(t *testing.T) (SignedDropAction, *Ed25519ClaimSigner) {
	t.Helper()
	seed := make([]byte, 32)
	seed[0] = 5
	signer, err := NewEd25519ClaimSigner("arbiter", seed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	a, err := BuildSignedDropAction("m-1", "w-1", 2, "safe-1",
		[]replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"}},
		"net1.events", "p1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	_, sig, err := signer.SignMutationClaim(context.Background(), a.ActionDigest)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	a.Signature = sig
	return a, signer
}

func TestBuildSignedDropAction_StampsVersionZeroCommitmentDropPlan(t *testing.T) {
	a, err := BuildSignedDropAction("m-1", "w-1", 2, "safe-1",
		[]replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"}},
		"net1.events", "p1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if a.ContractVersion != MutationContractVersion || a.ZeroPostCommitment.Root != "" ||
		a.Plan.Action != PublicationActionDropPartition || len(a.Plan.CanonicalParts) != 0 ||
		a.ActionDigest == "" || a.Signature != "" {
		t.Fatalf("built action wrong: %+v", a)
	}
}

func TestSignedDropAction_Valid_AcceptsSignedZeroPostDrop(t *testing.T) {
	a, _ := dropActionFixture(t)
	if err := a.Valid(); err != nil {
		t.Fatalf("signed zero-post drop must validate: %v", err)
	}
}

func TestSignedDropAction_Valid_RejectMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SignedDropAction)
	}{
		{"missing signature", func(a *SignedDropAction) { a.Signature = "" }},
		{"non-zero post commitment", func(a *SignedDropAction) { a.ZeroPostCommitment.Root = "nonzero" }},
		{"non-drop plan action", func(a *SignedDropAction) { a.Plan.Action = PublicationActionReplacePartition }},
		{"plan with canonical parts", func(a *SignedDropAction) {
			a.Plan.CanonicalParts = []replay.PartManifestEntry{{PartName: "x"}}
		}},
		{"zero seq", func(a *SignedDropAction) { a.PublicationSeq = 0 }},
		{"missing base roots", func(a *SignedDropAction) { a.BasePartitionRoots = nil }},
		{"blank base snapshot", func(a *SignedDropAction) { a.BaseSafeSnapshotID = "" }},
		{"blank version", func(a *SignedDropAction) { a.ContractVersion = "" }},
		{"tampered digest", func(a *SignedDropAction) { a.ActionDigest = "tampered" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := dropActionFixture(t)
			tc.mutate(&a)
			if err := a.Valid(); err == nil {
				t.Fatal("invalid drop action must fail closed")
			}
			if err := ValidateDropAction(a); err == nil {
				t.Fatal("ValidateDropAction must agree with Valid()")
			}
		})
	}
}

func TestComputeDropActionDigest_DeterministicAndOrderInsensitive(t *testing.T) {
	a, _ := BuildSignedDropAction("m-1", "w-1", 2, "safe-1",
		[]replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "r1"},
			{TableID: "net1.events", PartitionID: "p2", Root: "r2"},
		}, "net1.events", "p1")
	b := a
	b.BasePartitionRoots = []replay.PartitionCommitment{a.BasePartitionRoots[1], a.BasePartitionRoots[0]}
	da, _ := ComputeDropActionDigest(a)
	db, _ := ComputeDropActionDigest(b)
	if da != db {
		t.Fatal("digest must be order-insensitive across base roots")
	}
	c := a
	c.PublicationSeq = 99
	dc, _ := ComputeDropActionDigest(c)
	if da == dc {
		t.Fatal("digest must change when the seq changes")
	}
}

func TestVerifyDropActionSignature_RoundTrip(t *testing.T) {
	a, signer := dropActionFixture(t)
	if err := VerifyDropActionSignature(a, signer.PublicKey()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	tampered := a
	tampered.TableID = "other"
	if err := VerifyDropActionSignature(tampered, signer.PublicKey()); err == nil {
		t.Fatal("tampered action must fail verification (digest mismatch)")
	}
	other, _ := NewEd25519ClaimSigner("other", make([]byte, 32))
	if err := VerifyDropActionSignature(a, other.PublicKey()); err == nil {
		t.Fatal("wrong key must fail verification")
	}
}

func TestBuildEmptyDropAck_EmptyReadbackAndZeroPost(t *testing.T) {
	a, _ := dropActionFixture(t)
	ack, err := BuildEmptyDropAck(a, "safe-2", true)
	if err != nil {
		t.Fatalf("build ack: %v", err)
	}
	if len(ack.ExactActivePartsReadback) != 0 || len(ack.PostPartitionCommitments) != 0 || ack.PostStateRoot != "" {
		t.Fatalf("drop ack must be empty-post: %+v", ack)
	}
	if err := ack.Valid(); err != nil {
		t.Fatalf("empty-drop ack must be valid: %v", err)
	}
	if len(ack.BasePartitionRoots) != 1 || ack.BasePartitionRoots[0].Commitment != "base-p1" {
		t.Fatalf("base roots not mapped: %+v", ack.BasePartitionRoots)
	}
	if !ack.Applied {
		t.Fatal("applied flag not carried")
	}
}

func TestBuildEmptyDropAck_AppliedFalseWhenBaseCASMismatch(t *testing.T) {
	a, _ := dropActionFixture(t)
	ack, err := BuildEmptyDropAck(a, "safe-2", false)
	if err != nil {
		t.Fatalf("build ack: %v", err)
	}
	if ack.Applied {
		t.Fatal("applied must be false")
	}
	if err := ack.Valid(); err != nil {
		t.Fatalf("an Applied=false empty ack must still be valid: %v", err)
	}
}

func TestAssertEmptyDropAck(t *testing.T) {
	a, _ := dropActionFixture(t)
	good, _ := BuildEmptyDropAck(a, "safe-2", true)
	if err := AssertEmptyDropAck(a, good); err != nil {
		t.Fatalf("genuine empty ack must pass: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*PublicationAck)
	}{
		{"non-empty readback", func(ack *PublicationAck) {
			ack.ExactActivePartsReadback = []CandidatePart{{PartName: "x"}}
		}},
		{"non-zero post root", func(ack *PublicationAck) { ack.PostStateRoot = "root" }},
		{"seq mismatch", func(ack *PublicationAck) { ack.PublicationSeq = 9 }},
		{"base snapshot mismatch", func(ack *PublicationAck) { ack.BaseSafeSnapshotID = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ack, _ := BuildEmptyDropAck(a, "safe-2", true)
			tc.mutate(&ack)
			if err := AssertEmptyDropAck(a, ack); err == nil {
				t.Fatal("a corrupt drop ack must fail closed")
			}
		})
	}
}

func TestDecideDropApplied(t *testing.T) {
	base := []replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"}}
	if !DecideDropApplied(base, base) {
		t.Fatal("exact match must be applied")
	}
	if DecideDropApplied(base, []replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "advanced"}}) {
		t.Fatal("root mismatch must not be applied")
	}
	if DecideDropApplied(base, nil) {
		t.Fatal("missing partition must not be applied")
	}
}

func TestDropBaseRootsToContract_MapsRootToCommitment(t *testing.T) {
	roots := []replay.PartitionCommitment{{TableID: "t", PartitionID: "p", Root: "r"}}
	got := dropBaseRootsToContract(roots)
	if len(got) != 1 || got[0].TableID != "t" || got[0].PartitionID != "p" || got[0].Commitment != "r" {
		t.Fatalf("mapper wrong: %+v", got)
	}
}

// TestDriveEmptyDrop_RejectsForgedActionBeforeStoreAndExecutor proves the driver
// verifies the Arbiter signature before the idempotent-store and executor paths:
// a forged action with a recomputed digest and any non-empty signature under a
// different key must be rejected and must never touch the store or executor.
func TestDriveEmptyDrop_RejectsForgedActionBeforeStoreAndExecutor(t *testing.T) {
	arbiter := mustDropSigner(t, "arbiter", 5)

	// A forged action: the attacker recomputes a self-consistent digest and signs
	// it with their OWN key (structurally Valid(), so Valid() alone lets it through).
	forged, err := BuildSignedDropAction("m-1", "w-1", 2, "safe-1",
		[]replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"}},
		"net1.events", "p1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	attacker := mustDropSigner(t, "attacker", 9)
	_, sig, err := attacker.SignMutationClaim(context.Background(), forged.ActionDigest)
	if err != nil {
		t.Fatalf("attacker sign: %v", err)
	}
	forged.Signature = sig
	if err := forged.Valid(); err != nil {
		t.Fatalf("forged action is structurally valid by construction: %v", err)
	}

	store := &spyAckStore{}
	exec := &spyDropExecutor{}
	if _, err := DriveEmptyDrop(context.Background(), store, exec, forged, arbiter.PublicKey()); err == nil {
		t.Fatal("a forged (wrong-key) drop action must be rejected before execution")
	}
	if store.gets != 0 {
		t.Fatalf("driver must reject before the idempotent store lookup, got %d Get(s)", store.gets)
	}
	if exec.calls != 0 {
		t.Fatalf("driver must reject before the executor, got %d execution(s)", exec.calls)
	}
}

// TestDriveEmptyDrop_GenuineActionPassesVerification proves a genuinely
// Arbiter-signed action passes the new verification gate (it then reaches the
// store/executor, which the fake executor answers).
func TestDriveEmptyDrop_GenuineActionPassesVerification(t *testing.T) {
	a, signer := dropActionFixture(t)
	store := &spyAckStore{}
	want, err := BuildEmptyDropAck(a, "safe-2", true)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	exec := &spyDropExecutor{ack: want}
	got, err := DriveEmptyDrop(context.Background(), store, exec, a, signer.PublicKey())
	if err != nil {
		t.Fatalf("genuine action must pass verification: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("genuine action must reach the executor once, got %d", exec.calls)
	}
	if got.MutationID != a.MutationID {
		t.Fatalf("driver must return the executor ack: %+v", got)
	}
}

func mustDropSigner(t *testing.T, id string, seedByte byte) *Ed25519ClaimSigner {
	t.Helper()
	seed := make([]byte, 32)
	seed[0] = seedByte
	s, err := NewEd25519ClaimSigner(id, seed)
	if err != nil {
		t.Fatalf("signer %s: %v", id, err)
	}
	return s
}

type spyAckStore struct {
	gets int
}

func (s *spyAckStore) Get(context.Context, PublicationAckKey) (PublicationAck, bool, error) {
	s.gets++
	return PublicationAck{}, false, nil
}

func (s *spyAckStore) Put(_ context.Context, ack PublicationAck, _ CanonicalPublicationSet) (PublicationAck, error) {
	return ack, nil
}

type spyDropExecutor struct {
	calls int
	ack   PublicationAck
}

func (s *spyDropExecutor) ExecuteSignedDrop(context.Context, SignedDropAction) (PublicationAck, error) {
	s.calls++
	return s.ack, nil
}

// --- Gated: real DROP execution + durable ack need the absent C2 seam. ---

func TestDriveEmptyDrop_ExecutesRealDropAndDurableAck(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion signed DROP execution seam lands")
}

func TestDriveEmptyDrop_IdempotentReturnsSameAckBySeq(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the real durable-ack DROP path lands")
}
