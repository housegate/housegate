package storageintegrity

import (
	"context"
	"fmt"
	"sort"

	"housegate/housegate/pkg/replay"
)

// PublicationAction is what a retained worker does to an affected partition when
// installing the canonical post-state: REPLACE PARTITION for a non-empty
// post-state, or a signed DROP PARTITION for an empty-partition DELETE (design
// section 4.8).
type PublicationAction int

const (
	PublicationActionUnspecified PublicationAction = iota
	PublicationActionReplacePartition
	PublicationActionDropPartition
)

func (a PublicationAction) String() string {
	switch a {
	case PublicationActionReplacePartition:
		return "ReplacePartition"
	case PublicationActionDropPartition:
		return "DropPartition"
	default:
		return "Unspecified"
	}
}

// CanonicalArtifact is the HouseGate-core mirror of the ledger's recorded
// mutation-publication canonical artifact (design section 4.8's
// RecordMutationPublicationIssued): the majority claim group's post commitments
// plus the single canonical ActiveParts inventory every retained worker must
// install. ArtifactCommitment / ArtifactSource are opaque values the Arbiter
// hands HouseGate — HouseGate stores them, it does not recompute the majority or
// the commitment.
type CanonicalArtifact struct {
	MutationID               string
	PublicationSeq           uint64
	ArtifactCommitment       string
	ArtifactSource           string
	SchemaSnapshotID         string
	ExecutorProfileID        string
	PrevSafeSnapshotID       string
	AffectedPartitions       []replay.PartitionCommitment // base_partition_roots keyed by table/partition
	PostPartitionCommitments []replay.PartitionCommitment
	PostStateRoot            string
	CanonicalParts           []replay.PartManifestEntry
}

// PartitionInstallPlan is the per-(table,partition) instruction: the exact
// canonical parts to install and whether that is a REPLACE or a signed DROP.
type PartitionInstallPlan struct {
	TableID        string
	PartitionID    string
	Action         PublicationAction
	CanonicalParts []replay.PartManifestEntry
}

// CanonicalPublicationSet is the full deterministic install plan across all
// affected partitions in canonical order. It is what BuildCanonicalPublicationSet
// returns and what every retained worker must materialize identically.
type CanonicalPublicationSet struct {
	MutationID     string
	PublicationSeq uint64
	Plans          []PartitionInstallPlan
}

// BuildCanonicalPublicationSet derives the exact per-partition install plan
// SOLELY from the ledger majority artifact's canonical parts — never from any
// worker's local validation scratch (design sections 4.4 / 4.8). It fails closed
// on an incomplete artifact, a canonical part outside the affected partitions, a
// drop partition with a non-zero post commitment, or duplicate part names. A
// partition with canonical parts becomes a REPLACE; an affected partition with
// zero canonical parts and a zero post commitment becomes a signed DROP.
func BuildCanonicalPublicationSet(art CanonicalArtifact) (CanonicalPublicationSet, error) {
	if art.MutationID == "" {
		return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact: missing mutation id")
	}
	if art.PublicationSeq == 0 {
		return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: missing publication seq", art.MutationID)
	}
	if art.ArtifactCommitment == "" || art.ArtifactSource == "" {
		return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: missing commitment/source", art.MutationID)
	}
	if art.SchemaSnapshotID == "" || art.ExecutorProfileID == "" || art.PrevSafeSnapshotID == "" {
		return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: missing schema/executor/prev-safe identity", art.MutationID)
	}
	if len(art.AffectedPartitions) == 0 {
		return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: no affected partitions", art.MutationID)
	}

	// Reject a duplicate affected-partition entry before building anything: the
	// plans are built by iterating art.AffectedPartitions, so a repeated
	// (table_id, partition_id) would otherwise yield two identical plans for one
	// partition and make a retained worker run the same destructive REPLACE/DROP
	// twice, violating the one-plan-per-affected-partition invariant.
	affected := map[string]bool{}
	for _, ap := range art.AffectedPartitions {
		key := partKey(ap.TableID, ap.PartitionID)
		if affected[key] {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: duplicate affected partition %s", art.MutationID, key)
		}
		affected[key] = true
	}

	// Group canonical parts by partition, rejecting any part outside the affected
	// set and any duplicate part name.
	partsByPartition := map[string][]replay.PartManifestEntry{}
	seenNames := map[string]bool{}
	for _, p := range art.CanonicalParts {
		key := partKey(p.TableID, p.PartitionID)
		if !affected[key] {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: part %s in %s is outside the affected partitions", art.MutationID, p.PartName, key)
		}
		nameKey := key + "/" + p.PartName
		if seenNames[nameKey] {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: duplicate part name %s in %s", art.MutationID, p.PartName, key)
		}
		seenNames[nameKey] = true
		partsByPartition[key] = append(partsByPartition[key], p)
	}

	// Index the post commitments, rejecting a duplicate for one partition or a
	// commitment for a partition outside the affected set. A DROP must be an
	// explicit, present post commitment with a zero root — never merely a missing
	// entry — so a missing commitment cannot be mistaken for an empty-delete post.
	postByPartition := map[string]replay.PartitionCommitment{}
	for _, pc := range art.PostPartitionCommitments {
		key := partKey(pc.TableID, pc.PartitionID)
		if !affected[key] {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: post commitment for %s is outside the affected partitions", art.MutationID, key)
		}
		if _, dup := postByPartition[key]; dup {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: duplicate post commitment for %s", art.MutationID, key)
		}
		postByPartition[key] = pc
	}

	plans := make([]PartitionInstallPlan, 0, len(art.AffectedPartitions))
	for _, ap := range art.AffectedPartitions {
		key := partKey(ap.TableID, ap.PartitionID)
		parts := partsByPartition[key]
		// Every affected partition must carry exactly one post commitment. A
		// missing commitment is fail-closed: it must not silently become a DROP.
		post, ok := postByPartition[key]
		if !ok {
			return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: affected partition %s has no post commitment", art.MutationID, key)
		}
		plan := PartitionInstallPlan{TableID: ap.TableID, PartitionID: ap.PartitionID}
		if len(parts) > 0 {
			// A non-empty post-state: the commitment root must be non-empty.
			if post.Root == "" {
				return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: partition %s has canonical parts but a zero post commitment", art.MutationID, key)
			}
			plan.Action = PublicationActionReplacePartition
			plan.CanonicalParts = sortParts(parts)
		} else {
			// No canonical parts: this is an empty-partition DELETE only if the post
			// commitment is explicitly zero. A non-zero commitment with no parts is
			// a corrupt artifact.
			if post.Root != "" {
				return CanonicalPublicationSet{}, fmt.Errorf("canonical artifact %s: partition %s has a non-zero post commitment but no canonical parts", art.MutationID, key)
			}
			plan.Action = PublicationActionDropPartition
		}
		plans = append(plans, plan)
	}
	sortInstallPlans(plans)
	return CanonicalPublicationSet{MutationID: art.MutationID, PublicationSeq: art.PublicationSeq, Plans: plans}, nil
}

// AssertRetainedWorkersInstallSame enforces that every retained worker's
// produced/readback install set equals the single canonical set byte for byte —
// same ordered plans, same part identity for every content-addressed field. A
// worker whose logical root matches but whose part names / phys metadata differ
// fails closed: the current P2 manifest profile does not support a per-worker
// physical inventory (design section 4.8). An empty readback map or a missing
// retained worker is an error, never a silent pass.
func AssertRetainedWorkersInstallSame(canonical CanonicalPublicationSet, retainedWorkers []string, workerReadbacks map[string]CanonicalPublicationSet) error {
	if len(retainedWorkers) == 0 {
		return fmt.Errorf("canonical publication: no retained workers")
	}
	if len(workerReadbacks) == 0 {
		return fmt.Errorf("canonical publication: no worker readbacks")
	}
	for _, wid := range retainedWorkers {
		rb, ok := workerReadbacks[wid]
		if !ok {
			return fmt.Errorf("canonical publication: retained worker %s has no readback", wid)
		}
		if !canonicalPublicationSetEqual(canonical, rb) {
			return fmt.Errorf("canonical publication: retained worker %s readback differs from the canonical inventory; a logical root that matches but a physical part inventory that differs is unsupported by the current P2 manifest profile (version the profile before allowing per-worker physical inventory)", wid)
		}
	}
	return nil
}

// MutationPublicationDriver is the gated port a retained worker's publication
// executor implements: base-CAS against the bound base roots, REPLACE PARTITION
// FROM the publication shadow (or a signed DROP PARTITION), a durable local
// watermark, an exact-parts readback, and a signed PublicationAck. No real
// implementation exists; see CompanionMutationConsensusAvailable. HouseGate only
// drives it and never fabricates the Arbiter publication command or the ack
// signature.
type MutationPublicationDriver interface {
	PublishRetainedWorker(ctx context.Context, workerID string, plan CanonicalPublicationSet, baseRoots []replay.PartitionCommitment) (PublicationAck, error)
}

func partKey(table, partition string) string { return table + "/" + partition }

func sortParts(parts []replay.PartManifestEntry) []replay.PartManifestEntry {
	out := append([]replay.PartManifestEntry(nil), parts...)
	sort.Slice(out, func(i, j int) bool { return out[i].PartName < out[j].PartName })
	return out
}

func sortInstallPlans(plans []PartitionInstallPlan) {
	sort.Slice(plans, func(i, j int) bool {
		if plans[i].TableID != plans[j].TableID {
			return plans[i].TableID < plans[j].TableID
		}
		return plans[i].PartitionID < plans[j].PartitionID
	})
}

func canonicalPublicationSetEqual(a, b CanonicalPublicationSet) bool {
	if a.MutationID != b.MutationID || a.PublicationSeq != b.PublicationSeq || len(a.Plans) != len(b.Plans) {
		return false
	}
	for i := range a.Plans {
		pa, pb := a.Plans[i], b.Plans[i]
		if pa.TableID != pb.TableID || pa.PartitionID != pb.PartitionID || pa.Action != pb.Action || len(pa.CanonicalParts) != len(pb.CanonicalParts) {
			return false
		}
		for j := range pa.CanonicalParts {
			if !partEntryEqual(pa.CanonicalParts[j], pb.CanonicalParts[j]) {
				return false
			}
		}
	}
	return true
}

func partEntryEqual(a, b replay.PartManifestEntry) bool {
	if a.TableID != b.TableID || a.PartitionID != b.PartitionID || a.PartName != b.PartName ||
		a.PartPhysHash != b.PartPhysHash || a.PartRowLtHash != b.PartRowLtHash ||
		a.RowCount != b.RowCount || a.Bytes != b.Bytes || len(a.StorageRefs) != len(b.StorageRefs) {
		return false
	}
	for i := range a.StorageRefs {
		if a.StorageRefs[i] != b.StorageRefs[i] {
			return false
		}
	}
	return true
}

// clone deep-copies the artifact's slices (defensive-copy discipline).
func (a CanonicalArtifact) clone() CanonicalArtifact {
	out := a
	out.AffectedPartitions = append([]replay.PartitionCommitment(nil), a.AffectedPartitions...)
	out.PostPartitionCommitments = append([]replay.PartitionCommitment(nil), a.PostPartitionCommitments...)
	out.CanonicalParts = append([]replay.PartManifestEntry(nil), a.CanonicalParts...)
	return out
}
