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

// emptyDropActionDigestDomain versions the signed DROP-action digest.
const emptyDropActionDigestDomain = "mutation-empty-drop-action-v1"

// SignedDropAction is the Arbiter-signed internal DROP-PARTITION command for an
// empty-partition DELETE (design section 4.8): it binds the base snapshot/root,
// the partition, a zero post commitment, a monotonic publication seq, and a
// signature. HouseGate consumes and verifies it; it never issues the authoritative
// signature (that is the absent Arbiter/leader seam).
type SignedDropAction struct {
	ContractVersion    string
	MutationID         string
	WorkerID           string
	PublicationSeq     uint64
	BaseSafeSnapshotID string
	BasePartitionRoots []replay.PartitionCommitment
	TableID            string
	PartitionID        string
	ZeroPostCommitment replay.PartitionCommitment
	Plan               PartitionInstallPlan
	ActionDigest       string
	Signature          string
}

// EmptyDropExecutor is the gated port that runs a signed DROP against a real
// worker: recompute the local current root, base-CAS, execute the internal DROP
// PARTITION, durably persist {publication_seq, ack, watermark} before returning
// an empty-readback ack. No implementation exists; see
// CompanionMutationConsensusAvailable.
type EmptyDropExecutor interface {
	ExecuteSignedDrop(ctx context.Context, a SignedDropAction) (PublicationAck, error)
}

// BuildSignedDropAction constructs an unsigned drop action: it stamps the
// contract version, builds the zero post commitment and the DROP-action install
// plan, and computes the action digest. The Signature is left blank — the
// Arbiter/leader seam signs it. It fails closed on blank ids, a zero seq, or an
// empty base-root set, and defensively copies the base roots.
func BuildSignedDropAction(mutationID, workerID string, seq uint64, baseSnap string, baseRoots []replay.PartitionCommitment, tableID, partitionID string) (SignedDropAction, error) {
	if mutationID == "" || workerID == "" || tableID == "" || partitionID == "" {
		return SignedDropAction{}, fmt.Errorf("drop action: blank id (mutation/worker/table/partition)")
	}
	if seq == 0 {
		return SignedDropAction{}, fmt.Errorf("drop action %s: missing publication seq", mutationID)
	}
	if baseSnap == "" || len(baseRoots) == 0 {
		return SignedDropAction{}, fmt.Errorf("drop action %s: missing bound base snapshot/roots", mutationID)
	}
	a := SignedDropAction{
		ContractVersion:    MutationContractVersion,
		MutationID:         mutationID,
		WorkerID:           workerID,
		PublicationSeq:     seq,
		BaseSafeSnapshotID: baseSnap,
		BasePartitionRoots: append([]replay.PartitionCommitment(nil), baseRoots...),
		TableID:            tableID,
		PartitionID:        partitionID,
		ZeroPostCommitment: replay.PartitionCommitment{TableID: tableID, PartitionID: partitionID, Root: ""},
		Plan:               PartitionInstallPlan{TableID: tableID, PartitionID: partitionID, Action: PublicationActionDropPartition},
	}
	digest, err := ComputeDropActionDigest(a)
	if err != nil {
		return SignedDropAction{}, err
	}
	a.ActionDigest = digest
	return a, nil
}

// Valid fails closed unless the action is a signed, zero-post, DROP-plan command
// with a bound base and a digest that recomputes. A DROP may execute only when
// signed.
func (a SignedDropAction) Valid() error {
	if err := ValidateContractVersion(a.ContractVersion); err != nil {
		return err
	}
	if a.MutationID == "" || a.WorkerID == "" || a.TableID == "" || a.PartitionID == "" {
		return fmt.Errorf("drop action: blank id")
	}
	if a.PublicationSeq == 0 {
		return fmt.Errorf("drop action %s: missing publication seq", a.MutationID)
	}
	if a.BaseSafeSnapshotID == "" || len(a.BasePartitionRoots) == 0 {
		return fmt.Errorf("drop action %s: missing bound base", a.MutationID)
	}
	if a.ZeroPostCommitment.Root != "" {
		return fmt.Errorf("drop action %s: post commitment must be zero for an empty DELETE", a.MutationID)
	}
	if a.ZeroPostCommitment.TableID != a.TableID || a.ZeroPostCommitment.PartitionID != a.PartitionID {
		return fmt.Errorf("drop action %s: zero post commitment partition mismatch", a.MutationID)
	}
	if a.Plan.Action != PublicationActionDropPartition {
		return fmt.Errorf("drop action %s: plan action %s is not DropPartition", a.MutationID, a.Plan.Action)
	}
	if len(a.Plan.CanonicalParts) != 0 {
		return fmt.Errorf("drop action %s: a DROP plan must carry no canonical parts", a.MutationID)
	}
	if a.Plan.TableID != a.TableID || a.Plan.PartitionID != a.PartitionID {
		return fmt.Errorf("drop action %s: plan partition mismatch", a.MutationID)
	}
	if a.ActionDigest == "" {
		return fmt.Errorf("drop action %s: missing action digest", a.MutationID)
	}
	recomputed, err := ComputeDropActionDigest(a)
	if err != nil {
		return err
	}
	if recomputed != a.ActionDigest {
		return fmt.Errorf("drop action %s: action digest mismatch", a.MutationID)
	}
	if a.Signature == "" {
		return fmt.Errorf("drop action %s: unsigned; a DROP may execute only when signed", a.MutationID)
	}
	return nil
}

// ValidateDropAction is the exported wrapper around (SignedDropAction).Valid().
func ValidateDropAction(a SignedDropAction) error { return a.Valid() }

// canonicalDropAction is a deterministic string over the action's authority
// fields (base roots sorted), excluding the digest and signature themselves.
func canonicalDropAction(a SignedDropAction) string {
	var b strings.Builder
	b.WriteString("mutation_id=" + a.MutationID + "\n")
	b.WriteString("worker_id=" + a.WorkerID + "\n")
	fmt.Fprintf(&b, "publication_seq=%d\n", a.PublicationSeq)
	b.WriteString("base_safe_snapshot_id=" + a.BaseSafeSnapshotID + "\n")
	b.WriteString("table=" + a.TableID + "\n")
	b.WriteString("partition=" + a.PartitionID + "\n")
	b.WriteString("zero_post=" + a.ZeroPostCommitment.TableID + "/" + a.ZeroPostCommitment.PartitionID + "=" + a.ZeroPostCommitment.Root + "\n")
	roots := append([]replay.PartitionCommitment(nil), a.BasePartitionRoots...)
	sort.Slice(roots, func(i, j int) bool {
		if roots[i].TableID != roots[j].TableID {
			return roots[i].TableID < roots[j].TableID
		}
		return roots[i].PartitionID < roots[j].PartitionID
	})
	b.WriteString("base_roots=\n")
	for _, r := range roots {
		fmt.Fprintf(&b, "  %s/%s=%s\n", r.TableID, r.PartitionID, r.Root)
	}
	return b.String()
}

// ComputeDropActionDigest is the versioned canonical digest the signature covers.
func ComputeDropActionDigest(a SignedDropAction) (string, error) {
	return replay.CanonicalDigest(emptyDropActionDigestDomain, canonicalDropAction(a))
}

// VerifyDropActionSignature recomputes the digest, checks it matches, and
// verifies the ed25519 signature. Fail closed on any mismatch.
func VerifyDropActionSignature(a SignedDropAction, pub ed25519.PublicKey) error {
	digest, err := ComputeDropActionDigest(a)
	if err != nil {
		return err
	}
	if digest != a.ActionDigest {
		return fmt.Errorf("drop action digest mismatch")
	}
	sig, err := hex.DecodeString(a.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(pub, []byte(digest), sig) {
		return fmt.Errorf("ed25519 signature verification failed")
	}
	return nil
}

// dropBaseRootsToContract maps replay.PartitionCommitment{Root} base roots to the
// contract PartitionCommitment{Commitment} shape PublicationAck carries, keeping
// the table/partition.
func dropBaseRootsToContract(roots []replay.PartitionCommitment) []PartitionCommitment {
	out := make([]PartitionCommitment, 0, len(roots))
	for _, r := range roots {
		out = append(out, PartitionCommitment{TableID: r.TableID, PartitionID: r.PartitionID, Commitment: r.Root})
	}
	return out
}

// BuildEmptyDropAck emits the empty-DELETE PublicationAck for a drop action: an
// empty exact-active-parts readback, empty post commitments, and a blank post
// state root (the zero-post shape PublicationAck.Valid() accepts). Applied
// reflects the base-CAS outcome; a base-CAS mismatch still yields a valid empty
// ack with Applied=false.
func BuildEmptyDropAck(a SignedDropAction, localSafeSnapshotIDAfter string, applied bool) (PublicationAck, error) {
	if err := a.Valid(); err != nil {
		return PublicationAck{}, err
	}
	if localSafeSnapshotIDAfter == "" {
		return PublicationAck{}, fmt.Errorf("drop ack %s: missing local safe snapshot id after", a.MutationID)
	}
	return PublicationAck{
		ContractVersion:          MutationContractVersion,
		MutationID:               a.MutationID,
		WorkerID:                 a.WorkerID,
		PublicationSeq:           a.PublicationSeq,
		BaseSafeSnapshotID:       a.BaseSafeSnapshotID,
		BasePartitionRoots:       dropBaseRootsToContract(a.BasePartitionRoots),
		PostPartitionCommitments: nil,
		PostStateRoot:            "",
		LocalSafeSnapshotIDAfter: localSafeSnapshotIDAfter,
		ExactActivePartsReadback: nil,
		Applied:                  applied,
	}, nil
}

// AssertEmptyDropAck verifies a drop ack is genuinely empty and consistent with
// the action: it must be valid, bound to the same mutation/worker/seq/base
// snapshot and base roots, and carry no readback parts, no post commitments, and
// a blank post state root. A non-empty readback on a DROP is corrupt.
func AssertEmptyDropAck(a SignedDropAction, ack PublicationAck) error {
	if err := ack.Valid(); err != nil {
		return err
	}
	if ack.MutationID != a.MutationID || ack.WorkerID != a.WorkerID {
		return fmt.Errorf("drop ack: mutation/worker mismatch")
	}
	if ack.PublicationSeq != a.PublicationSeq {
		return fmt.Errorf("drop ack: publication seq mismatch")
	}
	if ack.BaseSafeSnapshotID != a.BaseSafeSnapshotID {
		return fmt.Errorf("drop ack: base snapshot mismatch")
	}
	if !sameContractRoots(ack.BasePartitionRoots, dropBaseRootsToContract(a.BasePartitionRoots)) {
		return fmt.Errorf("drop ack: base roots mismatch")
	}
	if len(ack.ExactActivePartsReadback) != 0 {
		return fmt.Errorf("drop ack: a DROP ack must have an empty readback")
	}
	if len(ack.PostPartitionCommitments) != 0 || ack.PostStateRoot != "" {
		return fmt.Errorf("drop ack: a DROP ack must have a zero post-state")
	}
	return nil
}

func sameContractRoots(a, b []PartitionCommitment) bool {
	if len(a) != len(b) {
		return false
	}
	idx := map[string]string{}
	for _, r := range a {
		idx[r.TableID+"/"+r.PartitionID] = r.Commitment
	}
	for _, r := range b {
		v, ok := idx[r.TableID+"/"+r.PartitionID]
		if !ok || v != r.Commitment {
			return false
		}
	}
	return true
}

// DecideDropApplied is the pure base-CAS decision (design section 4.8): Applied
// is true only when every bound base root exactly matches the worker's recomputed
// current root, with none missing. Any mismatch or missing partition yields false.
func DecideDropApplied(actionBaseRoots []replay.PartitionCommitment, workerCurrentRoots []replay.PartitionCommitment) bool {
	current := map[string]string{}
	for _, r := range workerCurrentRoots {
		current[r.TableID+"/"+r.PartitionID] = r.Root
	}
	for _, br := range actionBaseRoots {
		cur, ok := current[br.TableID+"/"+br.PartitionID]
		if !ok || cur == "" || cur != br.Root {
			return false
		}
	}
	return true
}

// DriveEmptyDrop is the gated driver helper: it verifies the action against the
// trusted Arbiter public key, then returns a durable ack for a repeated
// (mutation_id, worker_id, publication_seq) without re-executing, else executes
// the signed drop and lets the executor persist. Signature verification runs
// before the idempotent-store lookup and the executor so a forged action (a
// recomputed digest with any non-empty signature) can never reach DROP execution
// — Valid() proves structure and self-consistency, but only VerifyDropActionSignature
// against the configured Arbiter key proves authority. It needs a real
// EmptyDropExecutor, which does not exist; see CompanionMutationConsensusAvailable.
func DriveEmptyDrop(ctx context.Context, store PublicationAckStore, exec EmptyDropExecutor, a SignedDropAction, arbiterKey ed25519.PublicKey) (PublicationAck, error) {
	if err := a.Valid(); err != nil {
		return PublicationAck{}, err
	}
	if len(arbiterKey) == 0 {
		return PublicationAck{}, fmt.Errorf("drive empty drop %s: no trusted Arbiter key configured", a.MutationID)
	}
	if err := VerifyDropActionSignature(a, arbiterKey); err != nil {
		return PublicationAck{}, fmt.Errorf("drive empty drop %s: reject unauthorized action: %w", a.MutationID, err)
	}
	if store != nil {
		if existing, ok, err := store.Get(ctx, PublicationAckKey{MutationID: a.MutationID, WorkerID: a.WorkerID, PublicationSeq: a.PublicationSeq}); err != nil {
			return PublicationAck{}, err
		} else if ok {
			return existing, nil
		}
	}
	if exec == nil {
		return PublicationAck{}, fmt.Errorf("drive empty drop: no executor wired")
	}
	return exec.ExecuteSignedDrop(ctx, a)
}
