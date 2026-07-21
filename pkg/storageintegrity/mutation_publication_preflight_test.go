package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func preflightSigner(t *testing.T) *Ed25519ClaimSigner {
	t.Helper()
	seed := make([]byte, 32)
	seed[0] = 9
	s, err := NewEd25519ClaimSigner("arbiter-authority", seed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return s
}

func publicationCommandFixture() PublicationCommand {
	return PublicationCommand{
		ContractVersion:    MutationContractVersion,
		MutationID:         "m-1",
		PublicationSeq:     2,
		RequiredServingSet: []string{"w-1", "w-2", "w-3"},
		ArtifactCommitment: "commit-1",
		BasePartitionRoots: []replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"},
			{TableID: "net1.events", PartitionID: "p2", Root: "base-p2"},
		},
	}
}

func preflightArtifactFixture() CanonicalArtifact {
	return CanonicalArtifact{
		MutationID:         "m-1",
		PublicationSeq:     2,
		ArtifactCommitment: "commit-1",
		ArtifactSource:     "ledger-majority",
		SchemaSnapshotID:   "schema-1",
		ExecutorProfileID:  "profile-1",
		PrevSafeSnapshotID: "safe-1",
		AffectedPartitions: []replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"},
			{TableID: "net1.events", PartitionID: "p2", Root: "base-p2"},
		},
		PostStateRoot: "post-root-1",
	}
}

func preflightCurrentFixture() PreflightCurrentState {
	return PreflightCurrentState{
		CurrentPublicationSeq: 1,
		CurrentSafeRoots: []replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"},
			{TableID: "net1.events", PartitionID: "p2", Root: "base-p2"},
		},
	}
}

// --- Green-today: command validity, signing, preflight verification. ---

func TestPublicationCommand_Valid(t *testing.T) {
	if err := publicationCommandFixture().Valid(); err != nil {
		t.Fatalf("complete command must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*PublicationCommand)
	}{
		{"blank version", func(c *PublicationCommand) { c.ContractVersion = "" }},
		{"wrong version", func(c *PublicationCommand) { c.ContractVersion = "v0" }},
		{"blank mutation id", func(c *PublicationCommand) { c.MutationID = "" }},
		{"zero seq", func(c *PublicationCommand) { c.PublicationSeq = 0 }},
		{"empty serving set", func(c *PublicationCommand) { c.RequiredServingSet = nil }},
		{"blank commitment", func(c *PublicationCommand) { c.ArtifactCommitment = "" }},
		{"no base roots", func(c *PublicationCommand) { c.BasePartitionRoots = nil }},
		{"incomplete base root", func(c *PublicationCommand) {
			c.BasePartitionRoots = []replay.PartitionCommitment{{TableID: "t", PartitionID: "p"}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := publicationCommandFixture()
			tc.mutate(&c)
			if err := c.Valid(); err == nil {
				t.Fatal("invalid command must fail closed")
			}
		})
	}
}

func TestPublicationCommandHash_OrderInsensitive(t *testing.T) {
	a := publicationCommandFixture()
	b := publicationCommandFixture()
	// Reverse the slices; the canonical hash must be identical.
	b.RequiredServingSet = []string{"w-3", "w-2", "w-1"}
	b.BasePartitionRoots = []replay.PartitionCommitment{a.BasePartitionRoots[1], a.BasePartitionRoots[0]}
	ha, _ := PublicationCommandHash(a)
	hb, _ := PublicationCommandHash(b)
	if ha != hb {
		t.Fatal("command hash must be order-insensitive across serving set and base roots")
	}
}

func TestSignAndVerifyPublicationCommand(t *testing.T) {
	signer := preflightSigner(t)
	sc, err := SignPublicationCommand(context.Background(), signer, publicationCommandFixture())
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := VerifyPublicationCommandSignature(sc, signer.PublicKey()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Tampered command fails verification.
	tampered := sc
	tampered.Command.ArtifactCommitment = "tampered"
	if err := VerifyPublicationCommandSignature(tampered, signer.PublicKey()); err == nil {
		t.Fatal("tampered command must fail verification")
	}
	// Wrong key fails.
	other := preflightSigner(t)
	other2, _ := NewEd25519ClaimSigner("other", make([]byte, 32))
	_ = other
	if err := VerifyPublicationCommandSignature(sc, other2.PublicKey()); err == nil {
		t.Fatal("wrong public key must fail verification")
	}
}

func TestPreflightPublication_AcceptsValidCommand(t *testing.T) {
	signer := preflightSigner(t)
	sc, _ := SignPublicationCommand(context.Background(), signer, publicationCommandFixture())
	reject, err := PreflightPublication(sc, preflightArtifactFixture(), preflightCurrentFixture(), signer.PublicKey())
	if err != nil || reject != PreflightRejectNone {
		t.Fatalf("valid command must pass preflight, got %s: %v", reject, err)
	}
}

func TestPreflightPublication_RejectMatrix(t *testing.T) {
	signer := preflightSigner(t)
	cases := []struct {
		name       string
		mutateCmd  func(*PublicationCommand)
		mutateArt  func(*CanonicalArtifact)
		mutateCur  func(*PreflightCurrentState)
		wrongKey   bool
		wantReject PreflightReject
	}{
		{"invalid command", func(c *PublicationCommand) { c.MutationID = "" }, nil, nil, false, PreflightRejectInvalidCommand},
		{"bad signature (wrong key)", nil, nil, nil, true, PreflightRejectBadSignature},
		{"seq not monotonic", func(c *PublicationCommand) { c.PublicationSeq = 1 }, nil, func(s *PreflightCurrentState) { s.CurrentPublicationSeq = 1 }, false, PreflightRejectSeqNotMonotonic},
		{"artifact commitment mismatch", func(c *PublicationCommand) { c.ArtifactCommitment = "other" }, nil, nil, false, PreflightRejectArtifactCommitmentMismatch},
		{"artifact wrong mutation", nil, func(a *CanonicalArtifact) { a.MutationID = "m-other" }, nil, false, PreflightRejectArtifactCommitmentMismatch},
		{"missing base root", nil, nil, func(s *PreflightCurrentState) { s.CurrentSafeRoots = s.CurrentSafeRoots[:1] }, false, PreflightRejectMissingBaseRoot},
		{"base cas mismatch", nil, nil, func(s *PreflightCurrentState) { s.CurrentSafeRoots[0].Root = "advanced" }, false, PreflightRejectBaseCASMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := publicationCommandFixture()
			if tc.mutateCmd != nil {
				tc.mutateCmd(&cmd)
			}
			sc, err := SignPublicationCommand(context.Background(), signer, cmd)
			if err != nil {
				// An invalid command can't be signed via the helper (it validates
				// first); build an unsigned wrapper so preflight can reject it.
				sc = SignedPublicationCommand{Command: cmd}
			}
			art := preflightArtifactFixture()
			if tc.mutateArt != nil {
				tc.mutateArt(&art)
			}
			cur := preflightCurrentFixture()
			if tc.mutateCur != nil {
				tc.mutateCur(&cur)
			}
			key := signer.PublicKey()
			if tc.wrongKey {
				other, _ := NewEd25519ClaimSigner("other", make([]byte, 32))
				key = other.PublicKey()
			}
			reject, err := PreflightPublication(sc, art, cur, key)
			if reject != tc.wantReject {
				t.Fatalf("got reject %s, want %s (err %v)", reject, tc.wantReject, err)
			}
			if reject != PreflightRejectNone && err == nil {
				t.Fatal("a reject must carry an error")
			}
		})
	}
}

// --- Gated: driving a real Arbiter-issued publication command. ---

func TestPreflightPublication_DrivesRealArbiterCommand(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion mutation-publication command seam lands")
}
