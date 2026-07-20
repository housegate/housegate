package storageintegrity

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

// Domain strings version the mutation claim digests so a future P2 profile can
// never silently collide with v1 (the same convention pkg/replay uses).
const (
	mutationEqualityKeyDomain = "mutation-claim-equality-key-v1"
	mutationClaimHashDomain   = "mutation-claim-v1"
)

// SignedMutationClaim is an assembled MutationClaim (the PR08 contract type)
// plus its derived section-4.7 equality-key digest, its full-claim canonical
// hash, the signing worker id, and the detached ed25519 signature over the claim
// hash. The equality-key digest is what the Arbiter groups by; the claim hash is
// what the signature covers, so a signature attests every claim field, not just
// the equality-key subset.
type SignedMutationClaim struct {
	Claim             MutationClaim
	EqualityKeyDigest string
	ClaimHash         string
	WorkerID          string
	Signature         string
}

// MutationClaimSigner signs a mutation claim hash. The default implementation
// wraps the ed25519 signer used across the replay layer, so mutation claims are
// signed with the same non-repudiable pattern as replay receipts.
type MutationClaimSigner interface {
	SignMutationClaim(ctx context.Context, claimHash string) (workerID string, signatureHex string, err error)
}

// Ed25519ClaimSigner adapts payloadexec.Ed25519Signer to MutationClaimSigner.
type Ed25519ClaimSigner struct {
	inner *payloadexec.Ed25519Signer
}

// NewEd25519ClaimSigner constructs an ed25519 claim signer for a worker.
func NewEd25519ClaimSigner(workerID string, seed []byte) (*Ed25519ClaimSigner, error) {
	inner, err := payloadexec.NewEd25519Signer(workerID, seed)
	if err != nil {
		return nil, err
	}
	return &Ed25519ClaimSigner{inner: inner}, nil
}

// SignMutationClaim signs the claim hash, returning the worker id and hex
// signature. It reuses SignReplayReceipt, which signs an arbitrary hash string.
func (s *Ed25519ClaimSigner) SignMutationClaim(ctx context.Context, claimHash string) (string, string, error) {
	return s.inner.SignReplayReceipt(ctx, claimHash)
}

// PublicKey exposes the signer's verifying key for VerifyMutationClaimSignature.
func (s *Ed25519ClaimSigner) PublicKey() ed25519.PublicKey {
	return s.inner.PublicKey()
}

// AssembleMutationClaim binds a scratch replay result into a complete
// per-worker MutationClaim (design sections 4.6 step 7 / 4.7). It fails closed
// if the kind is not a mutation, any required field is blank/empty, or an
// affected partition lacks a post commitment — no tolerated wildcard, mirroring
// the INSERT prepared-consistency discipline. The post_state_root is derived via
// the shared content-addressed AssembleStateRoot seam so it is computed the same
// way as the INSERT path and is independently recomputable.
func AssembleMutationClaim(task MutationTask, res ScratchReplayResult, workerID string) (MutationClaim, error) {
	if !mutationKindValid(task.StatementKind) {
		return MutationClaim{}, fmt.Errorf("assemble claim: kind %q is not a mutation", task.StatementKind)
	}
	if workerID == "" {
		return MutationClaim{}, fmt.Errorf("assemble claim: missing worker id")
	}
	if res.PostStateRoot == "" {
		return MutationClaim{}, fmt.Errorf("assemble claim %s: missing post state root", task.MutationID)
	}
	if task.SchemaSnapshotID == "" || task.ExecutorProfileID == "" || task.PrevSafeSnapshotID == "" {
		return MutationClaim{}, fmt.Errorf("assemble claim %s: missing schema/executor/prev-safe identity", task.MutationID)
	}
	if len(task.BasePartitionRoots) == 0 {
		return MutationClaim{}, fmt.Errorf("assemble claim %s: missing base partition roots", task.MutationID)
	}
	if len(task.AffectedPartitions) == 0 {
		return MutationClaim{}, fmt.Errorf("assemble claim %s: no affected partitions", task.MutationID)
	}
	if len(res.PartitionDeltas) == 0 {
		return MutationClaim{}, fmt.Errorf("assemble claim %s: missing partition deltas", task.MutationID)
	}
	// Every affected partition must have a post commitment (present or empty-post
	// signalled by the executor). A missing post commitment is fail-closed.
	postByPart := map[string]bool{}
	for _, c := range res.PostPartitionCommitments {
		postByPart[c.TableID+"/"+c.PartitionID] = true
	}
	for _, ap := range task.AffectedPartitions {
		if !postByPart[ap.TableID+"/"+ap.PartitionID] {
			return MutationClaim{}, fmt.Errorf("assemble claim %s: affected partition %s/%s has no post commitment", task.MutationID, ap.TableID, ap.PartitionID)
		}
	}
	return MutationClaim{
		ContractVersion:          MutationContractVersion,
		MutationID:               task.MutationID,
		WorkerID:                 workerID,
		StatementKind:            task.StatementKind,
		PostStateRoot:            res.PostStateRoot,
		PartitionDeltas:          res.PartitionDeltas,
		PostPartitionCommitments: res.PostPartitionCommitments,
		SchemaSnapshotID:         task.SchemaSnapshotID,
		ExecutorProfileID:        task.ExecutorProfileID,
		PrevSafeSnapshotID:       task.PrevSafeSnapshotID,
		BasePartitionRoots:       task.BasePartitionRoots,
		AffectedPartitions:       task.AffectedPartitions,
		RowsBefore:               res.RowsBefore,
		RowsAfter:                res.RowsAfter,
	}, nil
}

// MutationEqualityKeyDigest is the versioned canonical digest of the section-4.7
// equality key (the 8-tuple only). Two claims that are logically equal on the
// equality-key fields — regardless of slice order or non-key fields — produce
// the same digest, which is what the Arbiter's 2/3 grouping keys on.
func MutationEqualityKeyDigest(c MutationClaim) (string, error) {
	return replay.CanonicalDigest(mutationEqualityKeyDomain, DeriveEqualityKey(c).CanonicalString())
}

// MutationClaimHash is the versioned canonical digest over the FULL claim,
// including the non-equality-key fields (mutation_id, worker_id, kind, base
// roots, rows) so the signature attests everything the equality key omits.
func MutationClaimHash(c MutationClaim) (string, error) {
	return replay.CanonicalDigest(mutationClaimHashDomain, canonicalClaim(c))
}

// canonicalClaim is a deterministic projection of a claim for hashing: it embeds
// the order-insensitive equality-key canonical string plus the remaining bound
// fields, so a change to any field changes the hash.
func canonicalClaim(c MutationClaim) string {
	return "equality_key=\n" + DeriveEqualityKey(c).CanonicalString() +
		fmt.Sprintf("mutation_id=%s\nworker_id=%s\nkind=%s\nrows_before=%d\nrows_after=%d\n",
			c.MutationID, c.WorkerID, c.StatementKind, c.RowsBefore, c.RowsAfter)
}

// SignAssembledClaim derives the equality-key digest and claim hash, signs the
// claim hash, and returns a SignedMutationClaim.
func SignAssembledClaim(ctx context.Context, signer MutationClaimSigner, c MutationClaim) (SignedMutationClaim, error) {
	if err := c.Valid(); err != nil {
		return SignedMutationClaim{}, err
	}
	ekd, err := MutationEqualityKeyDigest(c)
	if err != nil {
		return SignedMutationClaim{}, fmt.Errorf("equality key digest: %w", err)
	}
	ch, err := MutationClaimHash(c)
	if err != nil {
		return SignedMutationClaim{}, fmt.Errorf("claim hash: %w", err)
	}
	workerID, sig, err := signer.SignMutationClaim(ctx, ch)
	if err != nil {
		return SignedMutationClaim{}, fmt.Errorf("sign claim: %w", err)
	}
	return SignedMutationClaim{
		Claim:             c,
		EqualityKeyDigest: ekd,
		ClaimHash:         ch,
		WorkerID:          workerID,
		Signature:         sig,
	}, nil
}

// VerifyMutationClaimSignature recomputes the equality-key digest and claim hash
// from the signed claim, checks they match the signed values, and verifies the
// ed25519 signature over the claim hash. It fails closed on any mismatch.
func VerifyMutationClaimSignature(sc SignedMutationClaim, pub ed25519.PublicKey) error {
	ekd, err := MutationEqualityKeyDigest(sc.Claim)
	if err != nil {
		return err
	}
	if ekd != sc.EqualityKeyDigest {
		return fmt.Errorf("equality key digest mismatch")
	}
	ch, err := MutationClaimHash(sc.Claim)
	if err != nil {
		return err
	}
	if ch != sc.ClaimHash {
		return fmt.Errorf("claim hash mismatch")
	}
	sig, err := hex.DecodeString(sc.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(pub, []byte(ch), sig) {
		return fmt.Errorf("ed25519 signature verification failed")
	}
	return nil
}
