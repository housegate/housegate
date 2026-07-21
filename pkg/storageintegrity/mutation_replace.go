package storageintegrity

import (
	"fmt"

	"housegate/housegate/pkg/replay"
)

const (
	// targetSafeDatabase is the safe (published) database a REPLACE targets.
	targetSafeDatabase = "hg_safe"
	// mutationPublishShadowDatabase holds the canonical publication shadow parts
	// a retained worker REPLACEs FROM (design section 4.8).
	mutationPublishShadowDatabase = "hg_mutation_publish"
)

// ReplacePartitionPlan is the exact REPLACE PARTITION instruction for one
// non-empty post-state partition: the target table/partition, the canonical
// publication shadow to replace from, the exact canonical parts, and the SQL.
// Building the plan is pure; actually running the SQL against ClickHouse is the
// gated MutationPublicationDriver.PublishRetainedWorker path, not this builder.
type ReplacePartitionPlan struct {
	MutationID     string
	PublicationSeq uint64
	TableID        string
	PartitionID    string
	ShadowDatabase string
	ShadowTable    string
	SQL            string
	CanonicalParts []replay.PartManifestEntry
}

// BuildReplacePartitionPlan turns one canonical REPLACE-action install plan into
// the exact ALTER TABLE ... REPLACE PARTITION ... FROM shadow instruction
// (design section 4.8). It fails closed on a blank mutation id, a zero
// publication seq, a non-REPLACE action (DROP and Unspecified are out of scope),
// an empty canonical part set (an empty plan is not a non-empty REPLACE), or a
// blank table/partition. The canonical parts are defensively copied and sorted;
// the input plan slice is never mutated.
func BuildReplacePartitionPlan(mutationID string, publicationSeq uint64, plan PartitionInstallPlan) (ReplacePartitionPlan, error) {
	if mutationID == "" {
		return ReplacePartitionPlan{}, fmt.Errorf("replace plan: missing mutation id")
	}
	if publicationSeq == 0 {
		return ReplacePartitionPlan{}, fmt.Errorf("replace plan %s: missing publication seq", mutationID)
	}
	if plan.Action != PublicationActionReplacePartition {
		return ReplacePartitionPlan{}, fmt.Errorf("replace plan %s: action %s is not ReplacePartition", mutationID, plan.Action)
	}
	if len(plan.CanonicalParts) == 0 {
		return ReplacePartitionPlan{}, fmt.Errorf("replace plan %s: REPLACE requires at least one canonical part", mutationID)
	}
	if plan.TableID == "" || plan.PartitionID == "" {
		return ReplacePartitionPlan{}, fmt.Errorf("replace plan %s: blank table/partition", mutationID)
	}
	shadowTable := fmt.Sprintf("%s__%d", mutationID, publicationSeq)
	sql := fmt.Sprintf(
		"ALTER TABLE %s.%s REPLACE PARTITION ID %s FROM %s.%s;",
		quoteMergeIdent(targetSafeDatabase), quoteMergeIdent(plan.TableID),
		quoteMergeString(plan.PartitionID),
		quoteMergeIdent(mutationPublishShadowDatabase), quoteMergeIdent(shadowTable),
	)
	return ReplacePartitionPlan{
		MutationID:     mutationID,
		PublicationSeq: publicationSeq,
		TableID:        plan.TableID,
		PartitionID:    plan.PartitionID,
		ShadowDatabase: mutationPublishShadowDatabase,
		ShadowTable:    shadowTable,
		SQL:            sql,
		CanonicalParts: sortParts(plan.CanonicalParts), // sortParts returns a defensive copy
	}, nil
}
