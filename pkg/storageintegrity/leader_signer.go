package storageintegrity

import (
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

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
// HG-P0-03: the signature is recoverable secp256k1 (spec §9.1) over a canonical
// digest of the command's binding fields. The digest goes through
// replay.CanonicalDigest with a dedicated domain tag, matching the single
// canonical hashing profile used everywhere else in the integrity layer. The
// worker recovers the signing address from the compact signature and checks it
// against the configured publication-authority allowlist.
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
	Kind                    string                       `json:"kind"`
	PromotionSeq            uint64                       `json:"promotion_seq"`
	PromotionID             string                       `json:"promotion_id,omitempty"`
	CompactionID            string                       `json:"compaction_id,omitempty"`
	TableID                 string                       `json:"table_id,omitempty"`
	SafeTable               string                       `json:"safe_table,omitempty"`
	SourceTable             string                       `json:"source_table,omitempty"`
	UnsafeTable             string                       `json:"unsafe_table,omitempty"`
	PromoteDatabase         string                       `json:"promote_database,omitempty"`
	CompactDatabase         string                       `json:"compact_database,omitempty"`
	CompactTable            string                       `json:"compact_table,omitempty"`
	UnsafeBufferID          int                          `json:"unsafe_buffer_id,omitempty"`
	UnsafeBufferEpoch       uint64                       `json:"unsafe_buffer_epoch,omitempty"`
	UnsafeBufferDatabase    string                       `json:"unsafe_buffer_database,omitempty"`
	BaseSafeSnapshotID      string                       `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot       string                       `json:"base_partition_root,omitempty"`
	BasePartitionRoots      []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
	ExpectedPostRoot        string                       `json:"expected_post_root,omitempty"`
	ExpectedPostRoots       []replay.PartitionCommitment `json:"expected_post_roots,omitempty"`
	PartitionIDs            []string                     `json:"partition_ids,omitempty"`
	StatementIDs            []string                     `json:"statement_ids,omitempty"`
	DropPartitionIDs        []string                     `json:"drop_partition_ids,omitempty"`
	CandidateParts          []ByteSidePart               `json:"candidate_parts,omitempty"`
	CleanupUnsafeParts      []ByteSidePart               `json:"cleanup_unsafe_parts,omitempty"`
	InputParts              []replay.PartManifestEntry   `json:"input_parts,omitempty"`
	ReplacePartition        bool                         `json:"replace_partition,omitempty"`
	InternalDropPartition   bool                         `json:"internal_drop_partition,omitempty"`
	SkipBasePartitionAttach bool                         `json:"skip_base_partition_attach,omitempty"`
	CleanupUnsafe           bool                         `json:"cleanup_unsafe,omitempty"`
	RequireBaseRootCAS      bool                         `json:"require_base_root_cas,omitempty"`
	RequirePostRootCAS      bool                         `json:"require_post_root_cas,omitempty"`
	RequirePromotionSeq     bool                         `json:"require_promotion_seq,omitempty"`
	DropCompactTable        bool                         `json:"drop_compact_table,omitempty"`
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
		PromoteDatabase:         task.PromoteDatabase,
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
		CompactDatabase:    task.CompactDatabase,
		CompactTable:       task.CompactTable,
		BaseSafeSnapshotID: task.BaseSafeSnapshotID,
		BasePartitionRoot:  task.BasePartitionRoot,
		ExpectedPostRoot:   task.ExpectedPostRoot,
		PartitionIDs:       sortedCopy(task.PartitionIDs),
		InputParts:         sortedLeaderManifestParts(task.InputParts),
		RequireBaseRootCAS: task.RequireBaseRootCAS,
		RequirePostRootCAS: task.RequirePostRootCAS,
		DropCompactTable:   task.DropCompactTable,
	})
}

func sortedLeaderManifestParts(in []replay.PartManifestEntry) []replay.PartManifestEntry {
	if len(in) == 0 {
		return nil
	}
	out := append([]replay.PartManifestEntry(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID != out[j].TableID {
			return out[i].TableID < out[j].TableID
		}
		if out[i].PartitionID != out[j].PartitionID {
			return out[i].PartitionID < out[j].PartitionID
		}
		if out[i].PartName != out[j].PartName {
			return out[i].PartName < out[j].PartName
		}
		return out[i].PartPhysHash < out[j].PartPhysHash
	})
	return out
}

type leaderJWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type leaderJWSPayload struct {
	Purpose       string `json:"purpose"`
	CommandDigest string `json:"command_digest"`
}

const leaderPublicationPurpose = "storage-integrity-publication-v1"

func leaderSigningInput(commandDigest string) (string, error) {
	headerJSON, err := json.Marshal(leaderJWSHeader{Alg: "ES256K", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("marshal leader JWS header: %w", err)
	}
	payloadJSON, err := json.Marshal(leaderJWSPayload{Purpose: leaderPublicationPurpose, CommandDigest: commandDigest})
	if err != nil {
		return "", fmt.Errorf("marshal leader JWS payload: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON), nil
}

// Secp256k1LeaderSigner signs publication command digests with a secp256k1
// authority key (spec §9.1). The arbiter leader holds it; the mock ports the
// same construction so signatures verify across repos.
type Secp256k1LeaderSigner struct {
	priv    *ecdsa.PrivateKey
	address string
}

// NewSecp256k1LeaderSignerFromSeed builds a deterministic signer from a 32-byte
// hex seed (the seed is the raw secp256k1 scalar).
func NewSecp256k1LeaderSignerFromSeed(seedHex string) (*Secp256k1LeaderSigner, error) {
	priv, err := crypto.HexToECDSA(strings.TrimPrefix(seedHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("parse leader secp256k1 private key: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(priv.PublicKey).Hex())
	return &Secp256k1LeaderSigner{priv: priv, address: addr}, nil
}

// SignCommandDigest signs a command digest, returning compact JWS with a
// recoverable secp256k1 signature. The payload carries only the publication
// purpose and canonical command digest; the command itself stays on the task.
func (s *Secp256k1LeaderSigner) SignCommandDigest(commandDigest string) string {
	signingInput, err := leaderSigningInput(commandDigest)
	if err != nil {
		return ""
	}
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), s.priv)
	if err != nil {
		return ""
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// PublicKeyHex returns the compressed secp256k1 authority public key as hex.
func (s *Secp256k1LeaderSigner) PublicKeyHex() string {
	return "0x" + hex.EncodeToString(crypto.CompressPubkey(&s.priv.PublicKey))
}

// Address returns the lowercase, 0x-prefixed publication-authority address
// recovered from signatures this signer produces.
func (s *Secp256k1LeaderSigner) Address() string {
	if s == nil {
		return ""
	}
	return s.address
}

// LeaderSignatureVerifier verifies publication signatures against a configured
// leader authority public key. A zero verifier (no key configured) reports
// Enabled() false; callers decide whether that is allowed. Protected mode
// requires an authority allowlist and fails startup when it is empty
// (HG-P0-03).
type LeaderSignatureVerifier struct {
	allowed map[string]bool
}

// NewLeaderSignatureVerifier builds a verifier from authority addresses. For a
// migration window it also accepts compressed/uncompressed secp256k1 public
// keys and normalizes them to addresses. Empty input yields a disabled verifier.
func NewLeaderSignatureVerifier(authorities ...string) (*LeaderSignatureVerifier, error) {
	allowed := map[string]bool{}
	for _, authority := range authorities {
		for _, value := range strings.Split(authority, ",") {
			normalized, err := normalizeLeaderAuthority(value)
			if err != nil {
				return nil, err
			}
			if normalized != "" {
				allowed[normalized] = true
			}
		}
	}
	if len(allowed) == 0 {
		return &LeaderSignatureVerifier{}, nil
	}
	return &LeaderSignatureVerifier{allowed: allowed}, nil
}

func normalizeLeaderAuthority(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	if err != nil {
		return "", fmt.Errorf("decode leader authority hex: %w", err)
	}
	switch len(raw) {
	case 20:
		return "0x" + strings.ToLower(hex.EncodeToString(raw)), nil
	case 33, 65:
		pub, err := crypto.UnmarshalPubkey(raw)
		if err != nil && len(raw) == 33 {
			pub, err = crypto.DecompressPubkey(raw)
		}
		if err != nil {
			return "", fmt.Errorf("parse leader authority public key: %w", err)
		}
		return strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()), nil
	default:
		return "", fmt.Errorf("leader authority must be a 20-byte address or secp256k1 public key, got %d bytes", len(raw))
	}
}

// Enabled reports whether a leader authority key is configured.
func (v *LeaderSignatureVerifier) Enabled() bool {
	return v != nil && len(v.allowed) > 0
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
	recovered, err := recoverLeaderSignatureAddress(commandDigest, signatureHex)
	if err != nil {
		return err
	}
	if !v.allowed[strings.ToLower(recovered)] {
		return fmt.Errorf("leader signature address %s not in authority allowlist", recovered)
	}
	return nil
}

func recoverLeaderSignatureAddress(commandDigest, token string) (string, error) {
	parts := strings.Split(strings.Trim(token, "\"'"), ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid leader signature JWS: expected 3 parts")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("decode leader JWS header: %w", err)
	}
	var header leaderJWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", fmt.Errorf("parse leader JWS header: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", fmt.Errorf("unsupported leader signature algorithm %q", header.Alg)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode leader JWS payload: %w", err)
	}
	var payload leaderJWSPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", fmt.Errorf("parse leader JWS payload: %w", err)
	}
	if payload.Purpose != leaderPublicationPurpose {
		return "", fmt.Errorf("unexpected leader signature purpose %q", payload.Purpose)
	}
	if payload.CommandDigest != commandDigest {
		return "", fmt.Errorf("leader signature command digest mismatch")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("decode leader JWS signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("invalid leader signature length %d", len(sig))
	}
	if sig[64] >= 27 {
		sig[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), sig)
	if err != nil {
		return "", fmt.Errorf("recover leader signature address: %w", err)
	}
	return strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()), nil
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
