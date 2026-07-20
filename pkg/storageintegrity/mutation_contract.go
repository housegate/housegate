package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// MutationContractVersion is the single versioned identity of the P2 mutation
// contract HouseGate consumes. Every projected mutation value stamps this string
// in its ContractVersion field, and Valid() rejects any value that carries a
// blank or different version. This is the version-projection anchor: HouseGate
// consumes ONLY this pinned, versioned shape — never an ad-hoc or legacy HTTP
// mock shape (design section 4.1, "P2 必须版本化扩展").
const MutationContractVersion = "sentio-mutation-contract-v1"

// CompanionMutationConsensusAvailable reports whether the Sentio companion
// topology exposes the P2 mutation-consensus seam this contract projects:
// SubmitMutation (ACK1 sequencing), the MutationTask fan-out, SubmitMutationClaim
// with the 2-of-3 post-state quorum, per-worker PublicationAck, and the atomic
// PublishMutationSafeCut (design sections 4.3 / 4.7 / 4.8).
//
// It is false today. The companion arbiter/arbiter-proto are INSERT-only: the
// StatementKind enum is INSERT-only ("DDL/mutation kinds arrive with P2+"), the
// FSM has no mutation lane, and there is no mutation service, message, or RPC.
// Until the companion mutation seam lands, no real MutationSubmitter /
// MutationClaimSubmitter / MutationPublicationAcker / MutationSafeCutPublisher
// implementation exists, and the end-to-end mutation-consensus path is not
// wired. This constant is the single, honest gate that flips to true when a real
// mutation adapter is implemented against the companion seam; the mutation
// contract tests read it so a red run is never mistaken for green. It is
// deliberately independent of CompanionStagedIntakeAvailable (C1): mutation
// consensus (C2) is a distinct companion capability, and coupling the two would
// let a C1-only landing wrongly un-skip mutation tests.
const CompanionMutationConsensusAvailable = false

// AffectedPartition is the partition-granular unit of bounded mutation admission
// and one entry of the equality key's affected_partitions (design sections 4.2 /
// 4.7).
type AffectedPartition struct {
	TableID     string
	PartitionID string
}

// PartitionCommitment is a per-partition LtHash root commitment. It is reused for
// both base_partition_roots and post_partition_commitments.
type PartitionCommitment struct {
	TableID     string
	PartitionID string
	Commitment  string
}

// PartitionDelta is the per-partition mutation delta bound into the equality key
// (design sections 4.6 / 4.7). It carries both the add and remove LtHash sums
// plus the rows updated/deleted, which the single-sum PartitionLtHashSum cannot
// express — so mutation uses this dedicated type rather than force-fitting the
// INSERT sum shape.
type PartitionDelta struct {
	TableID         string
	PartitionID     string
	AddLtHashSum    string
	RemoveLtHashSum string
	RowsUpdated     uint64
	RowsDeleted     uint64
}

// MutationStatementEnvelope is the HouseGate-core mirror of the frozen P2
// mutation statement envelope (design section 4.1). Unlike the INSERT
// StatementEnvelope it carries NO payload identity: a mutation writes no
// hg_unsafe part (design section 4.4), so there is nothing payload-bearing to
// bind. It pins the schema/executor profile and the frozen prev-safe snapshot
// the mutation is bound against.
type MutationStatementEnvelope struct {
	ContractVersion    string
	StatementID        string
	StatementKind      Kind
	SQL                string
	SQLHash            string
	TargetTableID      string
	SchemaSnapshotID   string
	ExecutorProfileID  string
	PrevSafeSnapshotID string
	AffectedPartitions []AffectedPartition
	Signer             string
	UserJWS            string
}

// MutationTask is the per-worker task the Arbiter fans out to each of the 3
// MutationWorkers with the same frozen base (design sections 4.4 / 4.6). It is
// consumed by HouseGate, not produced: HouseGate replays it locally.
type MutationTask struct {
	ContractVersion    string
	MutationID         string
	StatementID        string
	StatementKind      Kind
	WorkerID           string
	PrevSafeSnapshotID string
	SchemaSnapshotID   string
	ExecutorProfileID  string
	AffectedPartitions []AffectedPartition
	BasePartitionRoots []PartitionCommitment
	MaterializedSQL    string
}

// MutationClaim is the signed per-worker post-state claim (design sections 4.6 /
// 4.7). It carries the full 8-field equality key the Arbiter groups claims by;
// grouping on post_state_root alone is explicitly forbidden (section 4.7).
type MutationClaim struct {
	ContractVersion          string
	MutationID               string
	WorkerID                 string
	StatementKind            Kind
	PostStateRoot            string
	PartitionDeltas          []PartitionDelta
	PostPartitionCommitments []PartitionCommitment
	SchemaSnapshotID         string
	ExecutorProfileID        string
	PrevSafeSnapshotID       string
	BasePartitionRoots       []PartitionCommitment
	AffectedPartitions       []AffectedPartition
	RowsBefore               uint64
	RowsAfter                uint64
	Signature                string
}

// EqualityKey is the exact 8-tuple the Arbiter groups claims by (design section
// 4.7). HouseGate projects and derives it as a pure helper; it does NOT decide
// the quorum (that is an Arbiter FSM decision consumed through the ports).
type EqualityKey struct {
	PostStateRoot            string
	PartitionDeltas          []PartitionDelta
	PostPartitionCommitments []PartitionCommitment
	SchemaSnapshotID         string
	ExecutorProfileID        string
	PrevSafeSnapshotID       string
	BasePartitionRoots       []PartitionCommitment
	AffectedPartitions       []AffectedPartition
}

// PublicationAck is the signed per-worker publication acknowledgement with all
// ten bound fields (design section 4.8). For an empty-DELETE partition the post
// commitment is zero and the readback is empty; for a non-empty post it carries
// the exact active parts readback.
type PublicationAck struct {
	ContractVersion          string
	MutationID               string
	WorkerID                 string
	PublicationSeq           uint64
	BaseSafeSnapshotID       string
	BasePartitionRoots       []PartitionCommitment
	PostPartitionCommitments []PartitionCommitment
	PostStateRoot            string
	LocalSafeSnapshotIDAfter string
	ExactActivePartsReadback []CandidatePart
	Applied                  bool
}

// PublishMutationSafeCutInput is what HouseGate assembles to DRIVE the Arbiter's
// atomic safe-cut command (design section 4.8). HouseGate supplies the inputs;
// the Arbiter owns the atomic FSM transition that installs the manifest,
// advances watermarks, and releases barriers. ServingAvailabilityFloor is the
// versioned P2 profile parameter (fixed at 2 for the 3-worker profile) and is
// not a runtime-mutable config.
type PublishMutationSafeCutInput struct {
	ContractVersion             string
	MutationID                  string
	PublicationSeq              uint64
	MajorityKey                 EqualityKey
	RequiredServingSet          []string
	RetainedServingSet          []string
	ExcludedBeforeCut           []string
	CanonicalArtifactCommitment string
	ServingAvailabilityFloor    int
}

// MutationServingAvailabilityFloor is the fixed P2 v1 serving-availability floor
// (design section 4.8: 3 serving workers, floor 2). It is a versioned profile
// parameter, not a runtime-mutable config.
const MutationServingAvailabilityFloor = 2

// ValidateContractVersion rejects a blank or non-pinned contract version fail
// closed, so HouseGate never consumes an unversioned or legacy shape.
func ValidateContractVersion(version string) error {
	if version == "" {
		return fmt.Errorf("mutation contract: missing contract version (want %q)", MutationContractVersion)
	}
	if version != MutationContractVersion {
		return fmt.Errorf("mutation contract: unsupported contract version %q (want %q)", version, MutationContractVersion)
	}
	return nil
}

// mutationKindValid reports whether a kind is a supported mutation kind. Only
// UPDATE and DELETE are mutations (design section 4.2); INSERT is rejected.
func mutationKindValid(k Kind) bool {
	return k == KindUpdate || k == KindDelete
}

// Valid checks a mutation statement envelope carries the pinned contract
// version, a mutation kind, and every required bound field. A blank field is a
// mismatch, never a tolerated default.
func (e MutationStatementEnvelope) Valid() error {
	if err := ValidateContractVersion(e.ContractVersion); err != nil {
		return err
	}
	if !mutationKindValid(e.StatementKind) {
		return fmt.Errorf("mutation envelope %s: kind %q is not a mutation (want UPDATE/DELETE)", e.StatementID, e.StatementKind)
	}
	if e.StatementID == "" {
		return fmt.Errorf("mutation envelope: missing statement id")
	}
	if e.TargetTableID == "" {
		return fmt.Errorf("mutation envelope %s: missing target table id", e.StatementID)
	}
	if e.SchemaSnapshotID == "" {
		return fmt.Errorf("mutation envelope %s: missing schema snapshot id", e.StatementID)
	}
	if e.ExecutorProfileID == "" {
		return fmt.Errorf("mutation envelope %s: missing executor profile id", e.StatementID)
	}
	if e.PrevSafeSnapshotID == "" {
		return fmt.Errorf("mutation envelope %s: missing prev safe snapshot id", e.StatementID)
	}
	if len(e.AffectedPartitions) == 0 {
		return fmt.Errorf("mutation envelope %s: no affected partitions", e.StatementID)
	}
	return nil
}

// Valid checks a mutation claim carries the pinned version and the full 8-field
// equality key (design section 4.7): every equality-key field must be present,
// so HouseGate binds the complete key rather than post_state_root alone.
func (c MutationClaim) Valid() error {
	if err := ValidateContractVersion(c.ContractVersion); err != nil {
		return err
	}
	if !mutationKindValid(c.StatementKind) {
		return fmt.Errorf("mutation claim %s: kind %q is not a mutation", c.MutationID, c.StatementKind)
	}
	if c.MutationID == "" {
		return fmt.Errorf("mutation claim: missing mutation id")
	}
	if c.WorkerID == "" {
		return fmt.Errorf("mutation claim %s: missing worker id", c.MutationID)
	}
	if c.PostStateRoot == "" {
		return fmt.Errorf("mutation claim %s: missing post state root", c.MutationID)
	}
	if len(c.PartitionDeltas) == 0 {
		return fmt.Errorf("mutation claim %s: missing partition deltas", c.MutationID)
	}
	if len(c.PostPartitionCommitments) == 0 {
		return fmt.Errorf("mutation claim %s: missing post partition commitments", c.MutationID)
	}
	if c.SchemaSnapshotID == "" {
		return fmt.Errorf("mutation claim %s: missing schema snapshot id", c.MutationID)
	}
	if c.ExecutorProfileID == "" {
		return fmt.Errorf("mutation claim %s: missing executor profile id", c.MutationID)
	}
	if c.PrevSafeSnapshotID == "" {
		return fmt.Errorf("mutation claim %s: missing prev safe snapshot id", c.MutationID)
	}
	if len(c.BasePartitionRoots) == 0 {
		return fmt.Errorf("mutation claim %s: missing base partition roots", c.MutationID)
	}
	if len(c.AffectedPartitions) == 0 {
		return fmt.Errorf("mutation claim %s: missing affected partitions", c.MutationID)
	}
	return nil
}

// Valid checks a publication ack carries the pinned version and its bound fields
// (design section 4.8). A non-empty post requires post commitments and an exact
// active-parts readback; an empty-DELETE post (zero post commitment + empty
// readback) is legitimate and accepted.
func (a PublicationAck) Valid() error {
	if err := ValidateContractVersion(a.ContractVersion); err != nil {
		return err
	}
	if a.MutationID == "" {
		return fmt.Errorf("publication ack: missing mutation id")
	}
	if a.WorkerID == "" {
		return fmt.Errorf("publication ack %s: missing worker id", a.MutationID)
	}
	if a.PublicationSeq == 0 {
		return fmt.Errorf("publication ack %s: missing publication seq", a.MutationID)
	}
	if a.BaseSafeSnapshotID == "" {
		return fmt.Errorf("publication ack %s: missing base safe snapshot id", a.MutationID)
	}
	if len(a.BasePartitionRoots) == 0 {
		return fmt.Errorf("publication ack %s: missing base partition roots", a.MutationID)
	}
	if a.LocalSafeSnapshotIDAfter == "" {
		return fmt.Errorf("publication ack %s: missing local safe snapshot id after", a.MutationID)
	}
	emptyPost := a.PostStateRoot == "" && len(a.PostPartitionCommitments) == 0 && len(a.ExactActivePartsReadback) == 0
	if !emptyPost {
		if a.PostStateRoot == "" {
			return fmt.Errorf("publication ack %s: non-empty post missing post state root", a.MutationID)
		}
		if len(a.PostPartitionCommitments) == 0 {
			return fmt.Errorf("publication ack %s: non-empty post missing post partition commitments", a.MutationID)
		}
		if len(a.ExactActivePartsReadback) == 0 {
			return fmt.Errorf("publication ack %s: non-empty post missing exact active parts readback", a.MutationID)
		}
	}
	return nil
}

// DeriveEqualityKey projects exactly the 8 equality-key fields from a claim into
// an EqualityKey — proving HouseGate binds those 8 fields and nothing else.
func DeriveEqualityKey(c MutationClaim) EqualityKey {
	return EqualityKey{
		PostStateRoot:            c.PostStateRoot,
		PartitionDeltas:          c.PartitionDeltas,
		PostPartitionCommitments: c.PostPartitionCommitments,
		SchemaSnapshotID:         c.SchemaSnapshotID,
		ExecutorProfileID:        c.ExecutorProfileID,
		PrevSafeSnapshotID:       c.PrevSafeSnapshotID,
		BasePartitionRoots:       c.BasePartitionRoots,
		AffectedPartitions:       c.AffectedPartitions,
	}
}

// CanonicalString is a deterministic, order-insensitive serialization of the
// equality key: the partition-keyed slices are sorted by (TableID, PartitionID)
// before encoding, so two logically equal keys built from differently-ordered
// slices compare equal. This is what makes the 2/3 claim grouping insensitive to
// per-worker slice ordering.
func (k EqualityKey) CanonicalString() string {
	var b strings.Builder
	b.WriteString("post_state_root=" + k.PostStateRoot + "\n")
	b.WriteString("schema_snapshot_id=" + k.SchemaSnapshotID + "\n")
	b.WriteString("executor_profile_id=" + k.ExecutorProfileID + "\n")
	b.WriteString("prev_safe_snapshot_id=" + k.PrevSafeSnapshotID + "\n")

	deltas := append([]PartitionDelta(nil), k.PartitionDeltas...)
	sort.Slice(deltas, func(i, j int) bool { return deltaLess(deltas[i], deltas[j]) })
	b.WriteString("partition_deltas=\n")
	for _, d := range deltas {
		fmt.Fprintf(&b, "  %s/%s add=%s rem=%s upd=%d del=%d\n", d.TableID, d.PartitionID, d.AddLtHashSum, d.RemoveLtHashSum, d.RowsUpdated, d.RowsDeleted)
	}

	b.WriteString("post_partition_commitments=\n")
	writeCommitments(&b, k.PostPartitionCommitments)
	b.WriteString("base_partition_roots=\n")
	writeCommitments(&b, k.BasePartitionRoots)

	parts := append([]AffectedPartition(nil), k.AffectedPartitions...)
	sort.Slice(parts, func(i, j int) bool { return partLess(parts[i], parts[j]) })
	b.WriteString("affected_partitions=\n")
	for _, p := range parts {
		fmt.Fprintf(&b, "  %s/%s\n", p.TableID, p.PartitionID)
	}
	return b.String()
}

func writeCommitments(b *strings.Builder, cs []PartitionCommitment) {
	sorted := append([]PartitionCommitment(nil), cs...)
	sort.Slice(sorted, func(i, j int) bool { return commitmentLess(sorted[i], sorted[j]) })
	for _, c := range sorted {
		fmt.Fprintf(b, "  %s/%s=%s\n", c.TableID, c.PartitionID, c.Commitment)
	}
}

func deltaLess(a, b PartitionDelta) bool {
	if a.TableID != b.TableID {
		return a.TableID < b.TableID
	}
	return a.PartitionID < b.PartitionID
}

func commitmentLess(a, b PartitionCommitment) bool {
	if a.TableID != b.TableID {
		return a.TableID < b.TableID
	}
	return a.PartitionID < b.PartitionID
}

func partLess(a, b AffectedPartition) bool {
	if a.TableID != b.TableID {
		return a.TableID < b.TableID
	}
	return a.PartitionID < b.PartitionID
}

// Equal reports whether two equality keys are logically equal, order-insensitive
// across their partition-keyed slices. This is the pure comparison the Arbiter's
// 2/3 grouping relies on; HouseGate exposes it as a helper but does not itself
// decide the quorum.
func (k EqualityKey) Equal(other EqualityKey) bool {
	return k.CanonicalString() == other.CanonicalString()
}

// MutationSubmitter is the ACK1 sequencing port (design section 4.4), analogous
// to StatementSubmitter for INSERT. No implementation exists; see
// CompanionMutationConsensusAvailable.
type MutationSubmitter interface {
	SubmitMutation(ctx context.Context, env MutationStatementEnvelope) (SubmitOutcome, error)
}

// MutationClaimSubmitter is the per-worker claim-submission port (design section
// 4.6) HouseGate's mutation worker drives. No implementation exists.
type MutationClaimSubmitter interface {
	SubmitMutationClaim(ctx context.Context, claim MutationClaim) (ClaimOutcome, error)
}

// MutationPublicationAcker is the per-worker publication-ack port (design section
// 4.8). No implementation exists.
type MutationPublicationAcker interface {
	SubmitPublicationAck(ctx context.Context, ack PublicationAck) error
}

// MutationSafeCutPublisher is the atomic safe-cut driver port (design section
// 4.8): HouseGate supplies inputs, the Arbiter owns the atomic FSM transition.
// No implementation exists.
type MutationSafeCutPublisher interface {
	PublishMutationSafeCut(ctx context.Context, in PublishMutationSafeCutInput) (SubmitOutcome, error)
}
