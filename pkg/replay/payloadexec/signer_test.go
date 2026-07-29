package payloadexec

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func TestEd25519SignerProducesVerifiableSignature(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	signer, err := NewEd25519Signer("replica-a", seed)
	if err != nil {
		t.Fatalf("NewEd25519Signer: %v", err)
	}

	var _ replay.Signer = signer

	receiptHash := replay.DigestString("a-receipt")
	replicaID, sig, err := signer.SignReplayReceipt(context.Background(), receiptHash)
	if err != nil {
		t.Fatalf("SignReplayReceipt: %v", err)
	}
	if replicaID != "replica-a" {
		t.Fatalf("replica id = %q, want replica-a", replicaID)
	}
	raw, err := hex.DecodeString(sig)
	if err != nil {
		t.Fatalf("signature is not hex: %v", err)
	}
	if !ed25519.Verify(signer.PublicKey(), []byte(receiptHash), raw) {
		t.Fatal("ed25519 signature did not verify against the signer public key")
	}
}

func TestEd25519SignerIsDeterministicForSeed(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	a, err := NewEd25519Signer("r", seed)
	if err != nil {
		t.Fatalf("signer a: %v", err)
	}
	b, err := NewEd25519Signer("r", seed)
	if err != nil {
		t.Fatalf("signer b: %v", err)
	}
	_, sigA, err := a.SignReplayReceipt(context.Background(), "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	_, sigB, err := b.SignReplayReceipt(context.Background(), "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if sigA != sigB {
		t.Fatal("same seed must produce the same signature")
	}
}

func TestEd25519SignerRejectsBadSeed(t *testing.T) {
	if _, err := NewEd25519Signer("r", []byte("too-short")); err == nil {
		t.Fatal("expected error for short seed")
	}
}
