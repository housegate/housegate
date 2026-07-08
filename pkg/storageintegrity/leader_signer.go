package storageintegrity

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"housegate/housegate/pkg/replay"
)

// Leader-signed publication (spec §9.1 PromotionIssued, §10). The leader
// orchestrator on the arbiter signs every promotion / compaction publication
// command; a HouseGate worker verifies the signature against a configured
// leader public key BEFORE it touches physical ClickHouse state, and fails
// closed if the signature is missing or invalid. This prevents a compromised
// or buggy control-plane path from getting a worker to publish an
// un-authorized REPLACE/ATTACH/DROP.
//
// The signature is ed25519 over a canonical digest of the command's binding
// fields (the same primitive the replay receipt signer uses — crypto/ed25519,
// stdlib, no external dependency), so the mock arbiter can produce it without
// importing HouseGate. The digest goes through replay.CanonicalDigest with a
// dedicated domain tag, matching the single canonical hashing profile used
// everywhere else in the integrity layer.

// leaderPublicationCommand is the canonical projection of a publication command
// that the leader signs and the worker verifies. Field order / JSON tags are
// part of the wire contract: the mock's port MUST match byte-for-byte.
type leaderPublicationCommand struct {
	Kind               string   `json:"kind"`
	PromotionSeq       uint64   `json:"promotion_seq"`
	PromotionID        string   `json:"promotion_id,omitempty"`
	CompactionID       string   `json:"compaction_id,omitempty"`
	TableID            string   `json:"table_id,omitempty"`
	SafeTable          string   `json:"safe_table,omitempty"`
	BaseSafeSnapshotID string   `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot  string   `json:"base_partition_root,omitempty"`
	PartitionIDs       []string `json:"partition_ids,omitempty"`
	StatementIDs       []string `json:"statement_ids,omitempty"`
	DropPartitionIDs   []string `json:"drop_partition_ids,omitempty"`
}

func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// PromotionLeaderCommandDigest returns the canonical digest a leader signs for a
// promotion publication. Exposed so the arbiter/mock and the worker derive an
// identical value from the task.
func PromotionLeaderCommandDigest(task PromotionTask) (string, error) {
	kind := task.Kind
	if kind == "" {
		kind = "insert"
	}
	return replay.CanonicalDigest("storage-integrity-leader-publication", leaderPublicationCommand{
		Kind:               kind,
		PromotionSeq:       task.PromotionSeq,
		PromotionID:        task.PromotionID,
		TableID:            task.TableID,
		SafeTable:          task.SafeTable,
		BaseSafeSnapshotID: task.BaseSafeSnapshotID,
		BasePartitionRoot:  task.BasePartitionRoot,
		PartitionIDs:       sortedCopy(task.PartitionIDs),
		StatementIDs:       sortedCopy(task.StatementIDs),
		DropPartitionIDs:   sortedCopy(task.DropPartitionIDs),
	})
}

// CompactionLeaderCommandDigest returns the canonical digest a leader signs for
// a controlled-compaction publication.
func CompactionLeaderCommandDigest(task CompactionTask) (string, error) {
	return replay.CanonicalDigest("storage-integrity-leader-publication", leaderPublicationCommand{
		Kind:               "compaction",
		PromotionSeq:       task.PromotionSeq,
		CompactionID:       task.CompactionID,
		TableID:            task.TableID,
		SafeTable:          task.SafeTable,
		BaseSafeSnapshotID: task.BaseSafeSnapshotID,
		BasePartitionRoot:  task.BasePartitionRoot,
		PartitionIDs:       sortedCopy(task.PartitionIDs),
	})
}

// Ed25519LeaderSigner signs publication command digests. The arbiter leader
// holds it; the mock ports the same construction.
type Ed25519LeaderSigner struct {
	priv ed25519.PrivateKey
}

// NewEd25519LeaderSignerFromSeed builds a deterministic signer from a 32-byte
// hex seed.
func NewEd25519LeaderSignerFromSeed(seedHex string) (*Ed25519LeaderSigner, error) {
	seed, err := hex.DecodeString(strings.TrimPrefix(seedHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode leader seed hex: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("leader seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return &Ed25519LeaderSigner{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// SignCommandDigest signs a command digest, returning a hex signature.
func (s *Ed25519LeaderSigner) SignCommandDigest(commandDigest string) string {
	sig := ed25519.Sign(s.priv, []byte(commandDigest))
	return "0x" + hex.EncodeToString(sig)
}

// PublicKeyHex returns the leader public key as hex (for configuring workers).
func (s *Ed25519LeaderSigner) PublicKeyHex() string {
	pub := s.priv.Public().(ed25519.PublicKey)
	return "0x" + hex.EncodeToString(pub)
}

// LeaderSignatureVerifier verifies publication signatures against a configured
// leader public key. A zero verifier (no key configured) reports Enabled()
// false; callers decide whether that is allowed (v1 keeps it optional so
// existing single-node/e2e flows without a leader key still run).
type LeaderSignatureVerifier struct {
	pub ed25519.PublicKey
}

// NewLeaderSignatureVerifier builds a verifier from a hex-encoded ed25519 public
// key. An empty key yields a disabled verifier.
func NewLeaderSignatureVerifier(publicKeyHex string) (*LeaderSignatureVerifier, error) {
	publicKeyHex = strings.TrimSpace(publicKeyHex)
	if publicKeyHex == "" {
		return &LeaderSignatureVerifier{}, nil
	}
	pub, err := hex.DecodeString(strings.TrimPrefix(publicKeyHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode leader public key hex: %w", err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("leader public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	return &LeaderSignatureVerifier{pub: ed25519.PublicKey(pub)}, nil
}

// Enabled reports whether a leader key is configured.
func (v *LeaderSignatureVerifier) Enabled() bool {
	return v != nil && len(v.pub) == ed25519.PublicKeySize
}

// Verify checks that signatureHex is a valid leader signature over
// commandDigest. It fails closed: a disabled verifier rejects a non-empty
// signature only when asked to verify; callers gate on Enabled() first.
func (v *LeaderSignatureVerifier) Verify(commandDigest, signatureHex string) error {
	if !v.Enabled() {
		return fmt.Errorf("leader signature verifier is not configured")
	}
	if signatureHex == "" {
		return fmt.Errorf("publication is missing a leader signature")
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(signatureHex, "0x"))
	if err != nil {
		return fmt.Errorf("decode leader signature hex: %w", err)
	}
	if !ed25519.Verify(v.pub, []byte(commandDigest), sig) {
		return fmt.Errorf("leader signature verification failed")
	}
	return nil
}

// ValidatePromotionLeaderSignature verifies the leader signature carried on a
// promotion task. When the verifier is disabled it is a no-op (the caller opted
// out of leader verification); when enabled it fails closed on any mismatch.
func ValidatePromotionLeaderSignature(v *LeaderSignatureVerifier, task PromotionTask) error {
	if !v.Enabled() {
		return nil
	}
	digest, err := PromotionLeaderCommandDigest(task)
	if err != nil {
		return fmt.Errorf("compute promotion leader command digest: %w", err)
	}
	if err := v.Verify(digest, task.LeaderSignature); err != nil {
		return fmt.Errorf("promotion %s: %w", task.PromotionID, err)
	}
	return nil
}

// ValidateCompactionLeaderSignature verifies the leader signature carried on a
// compaction task, fail-closed when the verifier is enabled.
func ValidateCompactionLeaderSignature(v *LeaderSignatureVerifier, task CompactionTask) error {
	if !v.Enabled() {
		return nil
	}
	digest, err := CompactionLeaderCommandDigest(task)
	if err != nil {
		return fmt.Errorf("compute compaction leader command digest: %w", err)
	}
	if err := v.Verify(digest, task.LeaderSignature); err != nil {
		return fmt.Errorf("compaction %s: %w", task.CompactionID, err)
	}
	return nil
}
