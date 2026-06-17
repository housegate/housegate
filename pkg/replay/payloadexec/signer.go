package payloadexec

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
)

// Ed25519Signer is an MVP replay receipt signer. It signs the receipt hash with
// an ed25519 key so attestations are non-repudiable and independently
// verifiable (design §7, Appendix C.4). Production may instead reuse the
// relay/indexer secp256k1 key via pkg/auth.
type Ed25519Signer struct {
	replicaID string
	priv      ed25519.PrivateKey
}

// NewEd25519Signer builds a signer from a 32-byte seed (deterministic).
func NewEd25519Signer(replicaID string, seed []byte) (*Ed25519Signer, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("ed25519 seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	if replicaID == "" {
		return nil, fmt.Errorf("replica id is required")
	}
	return &Ed25519Signer{replicaID: replicaID, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// PublicKey returns the verifying key for the signatures this signer produces.
func (s *Ed25519Signer) PublicKey() ed25519.PublicKey {
	return s.priv.Public().(ed25519.PublicKey)
}

// SignReplayReceipt signs the receipt hash bytes and returns the replica id and
// a hex-encoded signature.
func (s *Ed25519Signer) SignReplayReceipt(_ context.Context, receiptHash string) (string, string, error) {
	if receiptHash == "" {
		return "", "", fmt.Errorf("receipt hash is empty")
	}
	sig := ed25519.Sign(s.priv, []byte(receiptHash))
	return s.replicaID, hex.EncodeToString(sig), nil
}
