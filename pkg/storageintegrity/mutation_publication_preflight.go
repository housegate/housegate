package storageintegrity

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"housegate/housegate/pkg/replay"
)

// publicationCommandHashDomain versions the signed publication command digest so
// a future P2 profile can never silently collide with v1.
const publicationCommandHashDomain = "mutation-publication-command-v1"

// PublicationCommand is the HouseGate-core mirror of the Arbiter-issued signed
// publication command's authority-bearing fields (design section 4.8's
// RecordMutationPublicationIssued). HouseGate consumes it; it does not recompute
// the majority, quorum, or the FSM transition. BasePartitionRoots uses the same
// replay.PartitionCommitment shape as the canonical artifact so base-CAS compares
// like against like.
type PublicationCommand struct {
	ContractVersion    string
	MutationID         string
	PublicationSeq     uint64
	RequiredServingSet []string
	ArtifactCommitment string
	BasePartitionRoots []replay.PartitionCommitment
}

// SignedPublicationCommand is a PublicationCommand plus its canonical hash and a
// detached ed25519 signature over that hash, with the signer id. It parallels
// SignedMutationClaim; the payload is a publication command, not a claim.
type SignedPublicationCommand struct {
	Command     PublicationCommand
	CommandHash string
	SignerID    string
	Signature   string
}

// PreflightReject is the typed reason a publication preflight refuses to proceed.
type PreflightReject int

const (
	PreflightRejectNone PreflightReject = iota
	PreflightRejectInvalidCommand
	PreflightRejectBadSignature
	PreflightRejectSeqNotMonotonic
	PreflightRejectEmptyRequiredServingSet
	PreflightRejectArtifactCommitmentMismatch
	PreflightRejectMissingBaseRoot
	PreflightRejectBaseCASMismatch
)

func (r PreflightReject) String() string {
	switch r {
	case PreflightRejectNone:
		return "None"
	case PreflightRejectInvalidCommand:
		return "InvalidCommand"
	case PreflightRejectBadSignature:
		return "BadSignature"
	case PreflightRejectSeqNotMonotonic:
		return "SeqNotMonotonic"
	case PreflightRejectEmptyRequiredServingSet:
		return "EmptyRequiredServingSet"
	case PreflightRejectArtifactCommitmentMismatch:
		return "ArtifactCommitmentMismatch"
	case PreflightRejectMissingBaseRoot:
		return "MissingBaseRoot"
	case PreflightRejectBaseCASMismatch:
		return "BaseCASMismatch"
	default:
		return "Unknown"
	}
}

// PreflightCurrentState is the worker's local view the preflight reads: the last
// committed publication_seq (for monotonicity) and the current per-partition
// safe roots (for base-CAS). HouseGate populates it from its local
// watermark/manifest — no Arbiter call.
type PreflightCurrentState struct {
	CurrentPublicationSeq uint64
	CurrentSafeRoots      []replay.PartitionCommitment
}

// Valid checks the command carries the pinned contract version and every
// authority-bearing field.
func (c PublicationCommand) Valid() error {
	if err := ValidateContractVersion(c.ContractVersion); err != nil {
		return err
	}
	if c.MutationID == "" {
		return fmt.Errorf("publication command: missing mutation id")
	}
	if c.PublicationSeq == 0 {
		return fmt.Errorf("publication command %s: missing publication seq", c.MutationID)
	}
	if len(c.RequiredServingSet) == 0 {
		return fmt.Errorf("publication command %s: empty required serving set", c.MutationID)
	}
	if c.ArtifactCommitment == "" {
		return fmt.Errorf("publication command %s: missing artifact commitment", c.MutationID)
	}
	if len(c.BasePartitionRoots) == 0 {
		return fmt.Errorf("publication command %s: missing base partition roots", c.MutationID)
	}
	for _, r := range c.BasePartitionRoots {
		if r.TableID == "" || r.PartitionID == "" || r.Root == "" {
			return fmt.Errorf("publication command %s: incomplete base partition root %s/%s", c.MutationID, r.TableID, r.PartitionID)
		}
	}
	return nil
}

// canonicalPublicationCommand serializes a command deterministically: base roots
// sorted by (table, partition) and the serving set sorted lexically, so the hash
// is order-insensitive.
func canonicalPublicationCommand(c PublicationCommand) string {
	var b strings.Builder
	b.WriteString("mutation_id=" + c.MutationID + "\n")
	fmt.Fprintf(&b, "publication_seq=%d\n", c.PublicationSeq)
	b.WriteString("artifact_commitment=" + c.ArtifactCommitment + "\n")

	serving := append([]string(nil), c.RequiredServingSet...)
	sort.Strings(serving)
	b.WriteString("required_serving_set=\n")
	for _, s := range serving {
		b.WriteString("  " + s + "\n")
	}

	roots := append([]replay.PartitionCommitment(nil), c.BasePartitionRoots...)
	sort.Slice(roots, func(i, j int) bool {
		if roots[i].TableID != roots[j].TableID {
			return roots[i].TableID < roots[j].TableID
		}
		return roots[i].PartitionID < roots[j].PartitionID
	})
	b.WriteString("base_partition_roots=\n")
	for _, r := range roots {
		fmt.Fprintf(&b, "  %s/%s=%s\n", r.TableID, r.PartitionID, r.Root)
	}
	return b.String()
}

// PublicationCommandHash is the versioned canonical digest over the full command.
func PublicationCommandHash(cmd PublicationCommand) (string, error) {
	return replay.CanonicalDigest(publicationCommandHashDomain, canonicalPublicationCommand(cmd))
}

// SignPublicationCommand validates, hashes, and signs a publication command,
// reusing the existing MutationClaimSigner port (which signs an arbitrary hash
// string). It is a test/driver helper; the real command is Arbiter-issued.
func SignPublicationCommand(ctx context.Context, signer MutationClaimSigner, cmd PublicationCommand) (SignedPublicationCommand, error) {
	if err := cmd.Valid(); err != nil {
		return SignedPublicationCommand{}, err
	}
	h, err := PublicationCommandHash(cmd)
	if err != nil {
		return SignedPublicationCommand{}, fmt.Errorf("publication command hash: %w", err)
	}
	signerID, sig, err := signer.SignMutationClaim(ctx, h)
	if err != nil {
		return SignedPublicationCommand{}, fmt.Errorf("sign publication command: %w", err)
	}
	return SignedPublicationCommand{Command: cmd, CommandHash: h, SignerID: signerID, Signature: sig}, nil
}

// VerifyPublicationCommandSignature recomputes the command hash, checks it
// matches the signed value, and verifies the ed25519 signature. Fail closed on
// any mismatch.
func VerifyPublicationCommandSignature(sc SignedPublicationCommand, pub ed25519.PublicKey) error {
	h, err := PublicationCommandHash(sc.Command)
	if err != nil {
		return err
	}
	if h != sc.CommandHash {
		return fmt.Errorf("publication command hash mismatch")
	}
	sig, err := hex.DecodeString(sc.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(pub, []byte(h), sig) {
		return fmt.Errorf("ed25519 signature verification failed")
	}
	return nil
}

// PreflightPublication is the pure authority + base-CAS gate a retained worker
// runs BEFORE emitting any REPLACE/DROP PARTITION SQL (design section 4.8). It
// fails closed unless the signed command is valid and verifiable, its
// publication_seq is strictly greater than the current committed seq, its serving
// set is non-empty, its artifact commitment matches the canonical artifact for
// the same mutation/seq, and every bound base root matches the current
// per-partition safe root (base-CAS). It never invents the command, the majority
// artifact, or the ack.
func PreflightPublication(sc SignedPublicationCommand, art CanonicalArtifact, current PreflightCurrentState, pub ed25519.PublicKey) (PreflightReject, error) {
	cmd := sc.Command
	if err := cmd.Valid(); err != nil {
		return PreflightRejectInvalidCommand, err
	}
	if err := VerifyPublicationCommandSignature(sc, pub); err != nil {
		return PreflightRejectBadSignature, err
	}
	if cmd.PublicationSeq <= current.CurrentPublicationSeq {
		return PreflightRejectSeqNotMonotonic, fmt.Errorf("publication seq %d is not greater than current %d", cmd.PublicationSeq, current.CurrentPublicationSeq)
	}
	if len(cmd.RequiredServingSet) == 0 {
		return PreflightRejectEmptyRequiredServingSet, fmt.Errorf("empty required serving set")
	}
	// The commitment must be compared against the RIGHT artifact.
	if art.MutationID != cmd.MutationID || art.PublicationSeq != cmd.PublicationSeq {
		return PreflightRejectArtifactCommitmentMismatch, fmt.Errorf("artifact %s/%d does not match command %s/%d", art.MutationID, art.PublicationSeq, cmd.MutationID, cmd.PublicationSeq)
	}
	if cmd.ArtifactCommitment != art.ArtifactCommitment {
		return PreflightRejectArtifactCommitmentMismatch, fmt.Errorf("artifact commitment %q != command %q", art.ArtifactCommitment, cmd.ArtifactCommitment)
	}
	// Base-CAS: every bound base root must match the current safe root exactly.
	currentRoots := map[string]string{}
	for _, r := range current.CurrentSafeRoots {
		currentRoots[r.TableID+"/"+r.PartitionID] = r.Root
	}
	for _, br := range cmd.BasePartitionRoots {
		key := br.TableID + "/" + br.PartitionID
		cur, ok := currentRoots[key]
		if !ok {
			return PreflightRejectMissingBaseRoot, fmt.Errorf("base root for %s missing from current safe state", key)
		}
		if cur == "" || cur != br.Root {
			return PreflightRejectBaseCASMismatch, fmt.Errorf("base-CAS mismatch for %s: command %q, current %q", key, br.Root, cur)
		}
	}
	return PreflightRejectNone, nil
}
