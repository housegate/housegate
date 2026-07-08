package storageintegrity

import (
	"crypto/ed25519"
	"testing"
)

func newTestLeader(t *testing.T) (*Ed25519LeaderSigner, *LeaderSignatureVerifier) {
	t.Helper()
	// A fixed 32-byte hex seed (deterministic).
	seedHex := "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	signer, err := NewEd25519LeaderSignerFromSeed(seedHex)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	verifier, err := NewLeaderSignatureVerifier(signer.PublicKeyHex())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return signer, verifier
}

func TestPromotionLeaderSignatureRoundTrip(t *testing.T) {
	signer, verifier := newTestLeader(t)
	task := PromotionTask{
		PromotionID:  "promotion-s1",
		PromotionSeq: 5,
		Kind:         "insert",
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607", "202606"},
		StatementIDs: []string{"s1"},
	}
	digest, err := PromotionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)

	if err := ValidatePromotionLeaderSignature(verifier, task); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	// Partition order must not matter (canonical sort).
	reordered := task
	reordered.PartitionIDs = []string{"202606", "202607"}
	if err := ValidatePromotionLeaderSignature(verifier, reordered); err != nil {
		t.Fatalf("re-ordered partitions rejected: %v", err)
	}

	// Tampering with a bound field must fail closed.
	tampered := task
	tampered.SafeTable = "`hg_safe`.`other`"
	if err := ValidatePromotionLeaderSignature(verifier, tampered); err == nil {
		t.Fatalf("expected tampered task to fail leader verification")
	}

	// A missing signature must fail closed.
	unsigned := task
	unsigned.LeaderSignature = ""
	if err := ValidatePromotionLeaderSignature(verifier, unsigned); err == nil {
		t.Fatalf("expected missing signature to fail closed")
	}
}

func TestCompactionLeaderSignatureRoundTrip(t *testing.T) {
	signer, verifier := newTestLeader(t)
	task := CompactionTask{
		CompactionID: "c1",
		PromotionSeq: 9,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	}
	digest, err := CompactionLeaderCommandDigest(task)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	task.LeaderSignature = signer.SignCommandDigest(digest)
	if err := ValidateCompactionLeaderSignature(verifier, task); err != nil {
		t.Fatalf("valid compaction signature rejected: %v", err)
	}
	tampered := task
	tampered.PromotionSeq = 10
	if err := ValidateCompactionLeaderSignature(verifier, tampered); err == nil {
		t.Fatalf("expected tampered compaction task to fail leader verification")
	}
}

func TestDisabledVerifierIsNoOp(t *testing.T) {
	verifier, err := NewLeaderSignatureVerifier("")
	if err != nil {
		t.Fatalf("new disabled verifier: %v", err)
	}
	if verifier.Enabled() {
		t.Fatalf("empty key should be disabled")
	}
	// A disabled verifier lets an unsigned task through (opt-out).
	if err := ValidatePromotionLeaderSignature(verifier, PromotionTask{PromotionID: "p"}); err != nil {
		t.Fatalf("disabled verifier should be a no-op, got %v", err)
	}
}

// TestLeaderCommandDigestGoldenVector pins the canonical leader-command digest
// to the value sentio-storage-mocks' port asserts (mockstorage's
// TestLeaderCommandDigestGoldenVector). The mock cannot import this package, so
// this is the tripwire: if the command projection / domain tag / hashing
// profile changes here, this and the mock's constant must move in lockstep or
// leader-signed publications will fail verification in the e2e.
func TestLeaderCommandDigestGoldenVector(t *testing.T) {
	const (
		wantPromo   = "0xf716c6b32b913fb693a744aeed2fd09bbfc49e0b6164495e1d30205e73451d63"
		wantCompact = "0x33df6438d5bcfc8e62fb2cb71c60e181edf5f9fe45e52b85dfa0f86a360dbc09"
	)
	promo, err := PromotionLeaderCommandDigest(PromotionTask{
		PromotionID:  "promotion-golden",
		PromotionSeq: 42,
		Kind:         "insert",
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607", "202606"},
		StatementIDs: []string{"s2", "s1"},
	})
	if err != nil {
		t.Fatalf("promo digest: %v", err)
	}
	if promo != wantPromo {
		t.Fatalf("promotion leader digest = %s, want %s — update mock leader.go golden", promo, wantPromo)
	}
	compact, err := CompactionLeaderCommandDigest(CompactionTask{
		CompactionID: "compaction-golden",
		PromotionSeq: 7,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("compact digest: %v", err)
	}
	if compact != wantCompact {
		t.Fatalf("compaction leader digest = %s, want %s — update mock leader.go golden", compact, wantCompact)
	}
}

func TestLeaderVerifierRejectsBadKeyLength(t *testing.T) {
	if _, err := NewLeaderSignatureVerifier("0x00"); err == nil {
		t.Fatalf("expected error for short public key")
	}
	if ed25519.PublicKeySize != 32 {
		t.Fatalf("unexpected ed25519 public key size")
	}
}
