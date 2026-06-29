package storageintegrity

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Ed25519WorkerSigner struct {
	workerID string
	priv     ed25519.PrivateKey
}

func NewEd25519WorkerSigner(workerID string, seedMaterial []byte) (*Ed25519WorkerSigner, error) {
	if workerID == "" {
		return nil, fmt.Errorf("worker_id is required")
	}
	seed := sha256.Sum256(append(append([]byte("housegate-storage-integrity-worker-v1\x00"), seedMaterial...), workerID...))
	return &Ed25519WorkerSigner{
		workerID: workerID,
		priv:     ed25519.NewKeyFromSeed(seed[:]),
	}, nil
}

func (s *Ed25519WorkerSigner) SignReplayReceipt(ctx context.Context, receiptHash string) (string, string, error) {
	return s.sign(ctx, receiptHash)
}

func (s *Ed25519WorkerSigner) SignSafeAuditVote(ctx context.Context, voteHash string) (string, string, error) {
	return s.sign(ctx, voteHash)
}

func (s *Ed25519WorkerSigner) sign(ctx context.Context, digest string) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if s == nil || len(s.priv) == 0 {
		return "", "", fmt.Errorf("worker signer is nil")
	}
	if digest == "" {
		return "", "", fmt.Errorf("digest is required")
	}
	return s.workerID, hex.EncodeToString(ed25519.Sign(s.priv, []byte(digest))), nil
}
