package storageintegrity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"

	"housegate/housegate/pkg/replay"
)

// Leader-signed publication (spec §9.1 PromotionIssued, §10). The leader
// orchestrator on the arbiter signs every promotion / compaction publication
// command; a HouseGate worker verifies the signature against a configured
// leader authority public key BEFORE it touches physical ClickHouse state, and
// fails closed if the signature is missing or invalid. This prevents a
// compromised or buggy control-plane path from getting a worker to publish an
// un-authorized REPLACE/ATTACH/DROP.
//
// HG-P0-03: the signature is secp256k1 ECDSA (spec §9.1) over a canonical
// digest of the command's binding fields. The digest goes through
// replay.CanonicalDigest with a dedicated domain tag, matching the single
// canonical hashing profile used everywhere else in the integrity layer. Both
// HouseGate and the mock arbiter use github.com/decred/dcrd/dcrec/secp256k1/v4
// (a small, pure-Go, dependency-light module) so signatures are byte-portable
// across repos without importing HouseGate into the mock.
//
// The canonical projection MUST cover every field that changes the physical
// publication result — source/candidate parts, expected roots, unsafe buffer
// identity, publication action flags, CAS requirements, and cleanup — so a
// signed task cannot be mutated into a different source, part set, expected
// root, or action without breaking the signature.

// leaderPublicationCommand is the canonical projection of a publication command
// that the leader signs and the worker verifies. Field order / JSON tags are
// part of the wire contract: the mock's port MUST match byte-for-byte.
type leaderPublicationCommand struct {
	Kind                 string                       `json:"kind"`
	PromotionSeq         uint64                       `json:"promotion_seq"`
	PromotionID          string                       `json:"promotion_id,omitempty"`
	CompactionID         string                       `json:"compaction_id,omitempty"`
	TableID              string                       `json:"table_id,omitempty"`
	SafeTable            string                       `json:"safe_table,omitempty"`
	SourceTable          string                       `json:"source_table,omitempty"`
	UnsafeTable          string                       `json:"unsafe_table,omitempty"`
	UnsafeBufferID       int                          `json:"unsafe_buffer_id,omitempty"`
	UnsafeBufferEpoch    uint64                       `json:"unsafe_buffer_epoch,omitempty"`
	UnsafeBufferDatabase string                       `json:"unsafe_buffer_database,omitempty"`
	BaseSafeSnapshotID   string                       `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot    string                       `json:"base_partition_root,omitempty"`
	BasePartitionRoots   []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
	ExpectedPostRoot     string                       `json:"expected_post_root,omitempty"`
	ExpectedPostRoots    []replay.PartitionCommitment `json:"expected_post_roots,omitempty"`
	PartitionIDs         []string                     `json:"partition_ids,omitempty"`
	StatementIDs         []string                     `json:"statement_ids,omitempty"`
	DropPartitionIDs     []string                     `json:"drop_partition_ids,omitempty"`
	CandidateParts       []ByteSidePart               `json:"candidate_parts,omitempty"`
	CleanupUnsafeParts   []ByteSidePart               `json:"cleanup_unsafe_parts,omitempty"`
	ReplacePartition     bool                         `json:"replace_partition,omitempty"`
	InternalDropPartition bool                        `json:"internal_drop_partition,omitempty"`
	SkipBasePartitionAttach bool                      `json:"skip_base_partition_attach,omitempty"`
	CleanupUnsafe        bool                         `json:"cleanup_unsafe,omitempty"`
	RequireBaseRootCAS   bool                         `json:"require_base_root_cas,omitempty"`
	RequirePostRootCAS   bool                         `json:"require_post_root_cas,omitempty"`
	RequirePromotionSeq  bool                         `json:"require_promotion_seq,omitempty"`
}

func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// sortedParts returns a canonical (partition, name, lthash)-ordered copy so the
// signed part set is independent of task-construction order.
func sortedParts(in []ByteSidePart) []ByteSidePart {
	if len(in) == 0 {
		return nil
	}
	out := append([]ByteSidePart(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].PartitionID != out[j].PartitionID {
			return out[i].PartitionID < out[j].PartitionID
		}
		if out[i].PartName != out[j].PartName {
			return out[i].PartName < out[j].PartName
		}
		return out[i].PartRowLtHash < out[j].PartRowLtHash
	})
	return out
}

// sortedCommitments returns a canonical (table, partition)-ordered copy of
// partition commitments so the signed roots are order-independent.
func sortedCommitments(in []replay.PartitionCommitment) []replay.PartitionCommitment {
	if len(in) == 0 {
		return nil
	}
	out := append([]replay.PartitionCommitment(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID != out[j].TableID {
			return out[i].TableID < out[j].TableID
		}
		return out[i].PartitionID < out[j].PartitionID
	})
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
		Kind:                    kind,
		PromotionSeq:            task.PromotionSeq,
		PromotionID:             task.PromotionID,
		TableID:                 task.TableID,
		SafeTable:               task.SafeTable,
		SourceTable:             task.SourceTable,
		UnsafeTable:             task.UnsafeTable,
		UnsafeBufferID:          task.UnsafeBufferID,
		UnsafeBufferEpoch:       task.UnsafeBufferEpoch,
		UnsafeBufferDatabase:    task.UnsafeBufferDatabase,
		BaseSafeSnapshotID:      task.BaseSafeSnapshotID,
		BasePartitionRoot:       task.BasePartitionRoot,
		BasePartitionRoots:      sortedCommitments(task.BasePartitionRoots),
		ExpectedPostRoot:        task.ExpectedPostRoot,
		ExpectedPostRoots:       sortedCommitments(task.ExpectedPostRoots),
		PartitionIDs:            sortedCopy(task.PartitionIDs),
		StatementIDs:            sortedCopy(task.StatementIDs),
		DropPartitionIDs:        sortedCopy(task.DropPartitionIDs),
		CandidateParts:          sortedParts(task.CandidateParts),
		CleanupUnsafeParts:      sortedParts(task.CleanupUnsafeParts),
		ReplacePartition:        task.ReplacePartition,
		InternalDropPartition:   task.InternalDropPartition,
		SkipBasePartitionAttach: task.SkipBasePartitionAttach,
		CleanupUnsafe:           task.CleanupUnsafe,
		RequireBaseRootCAS:      task.RequireBaseRootCAS,
		RequirePostRootCAS:      task.RequirePostRootCAS,
		RequirePromotionSeq:     task.RequirePromotionSeq,
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
		ExpectedPostRoot:   task.ExpectedPostRoot,
		PartitionIDs:       sortedCopy(task.PartitionIDs),
		RequireBaseRootCAS: task.RequireBaseRootCAS,
		RequirePostRootCAS: task.RequirePostRootCAS,
	})
}

// leaderDigestHash hashes the canonical command digest string to the 32-byte
// message secp256k1 ECDSA signs/verifies over. A domain-separated SHA-256 keeps
// HouseGate and the mock byte-identical without depending on the digest's own
// hex encoding.
func leaderDigestHash(commandDigest string) []byte {
	sum := sha256.Sum256([]byte("storage-integrity-leader-sig\x00" + commandDigest))
	return sum[:]
}

// Secp256k1LeaderSigner signs publication command digests with a secp256k1
// authority key (spec §9.1). The arbiter leader holds it; the mock ports the
// same construction so signatures verify across repos.
type Secp256k1LeaderSigner struct {
	priv *secp256k1.PrivateKey
}

// NewSecp256k1LeaderSignerFromSeed builds a deterministic signer from a 32-byte
// hex seed (the seed is the raw secp256k1 scalar).
func NewSecp256k1LeaderSignerFromSeed(seedHex string) (*Secp256k1LeaderSigner, error) {
	seed, err := hex.DecodeString(strings.TrimPrefix(seedHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode leader seed hex: %w", err)
	}
	if len(seed) != 32 {
		return nil, fmt.Errorf("leader seed must be 32 bytes, got %d", len(seed))
	}
	return &Secp256k1LeaderSigner{priv: secp256k1.PrivKeyFromBytes(seed)}, nil
}

// SignCommandDigest signs a command digest, returning a hex-encoded DER
// signature.
func (s *Secp256k1LeaderSigner) SignCommandDigest(commandDigest string) string {
	sig := ecdsa.Sign(s.priv, leaderDigestHash(commandDigest))
	return "0x" + hex.EncodeToString(sig.Serialize())
}

// PublicKeyHex returns the compressed secp256k1 authority public key as hex.
func (s *Secp256k1LeaderSigner) PublicKeyHex() string {
	return "0x" + hex.EncodeToString(s.priv.PubKey().SerializeCompressed())
}

// LeaderSignatureVerifier verifies publication signatures against a configured
// leader authority public key. A zero verifier (no key configured) reports
// Enabled() false; callers decide whether that is allowed. Protected mode
// requires an authority allowlist and fails startup when it is empty
// (HG-P0-03).
type LeaderSignatureVerifier struct {
	pub *secp256k1.PublicKey
}

// NewLeaderSignatureVerifier builds a verifier from a hex-encoded compressed
// secp256k1 public key. An empty key yields a disabled verifier.
func NewLeaderSignatureVerifier(publicKeyHex string) (*LeaderSignatureVerifier, error) {
	publicKeyHex = strings.TrimSpace(publicKeyHex)
	if publicKeyHex == "" {
		return &LeaderSignatureVerifier{}, nil
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(publicKeyHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode leader public key hex: %w", err)
	}
	pub, err := secp256k1.ParsePubKey(raw)
	if err != nil {
		return nil, fmt.Errorf("parse leader secp256k1 public key: %w", err)
	}
	return &LeaderSignatureVerifier{pub: pub}, nil
}

// Enabled reports whether a leader authority key is configured.
func (v *LeaderSignatureVerifier) Enabled() bool {
	return v != nil && v.pub != nil
}

// Verify checks that signatureHex is a valid leader signature over
// commandDigest. It fails closed: a disabled verifier rejects; callers gate on
// Enabled() first when opting out is permitted.
func (v *LeaderSignatureVerifier) Verify(commandDigest, signatureHex string) error {
	if !v.Enabled() {
		return fmt.Errorf("leader signature verifier is not configured")
	}
	if signatureHex == "" {
		return fmt.Errorf("publication is missing a leader signature")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(signatureHex, "0x"))
	if err != nil {
		return fmt.Errorf("decode leader signature hex: %w", err)
	}
	sig, err := ecdsa.ParseDERSignature(raw)
	if err != nil {
		return fmt.Errorf("parse leader DER signature: %w", err)
	}
	if !sig.Verify(leaderDigestHash(commandDigest), v.pub) {
		return fmt.Errorf("leader signature verification failed")
	}
	return nil
}

// ValidatePromotionLeaderSignature verifies the leader signature carried on a
// promotion task. When the verifier is disabled and verification is not
// required it is a no-op (the caller opted out); when required (protected mode)
// a disabled verifier fails closed; when enabled it fails closed on any
// mismatch.
func ValidatePromotionLeaderSignature(v *LeaderSignatureVerifier, required bool, task PromotionTask) error {
	if !v.Enabled() {
		if required {
			return fmt.Errorf("promotion %s: leader signature required but no authority key is configured", task.PromotionID)
		}
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
// compaction task. Fails closed when enabled, or when required in protected
// mode without a configured key.
func ValidateCompactionLeaderSignature(v *LeaderSignatureVerifier, required bool, task CompactionTask) error {
	if !v.Enabled() {
		if required {
			return fmt.Errorf("compaction %s: leader signature required but no authority key is configured", task.CompactionID)
		}
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
