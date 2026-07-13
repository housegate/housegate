package storageintegrity

import (
	"fmt"
	"strings"
)

// ValidatePromotionPreflight rejects malformed publication commands before any
// ClickHouse side effect. protected enables the stricter SNode contract used
// when authority signatures are mandatory.
func ValidatePromotionPreflight(task PromotionTask, protected bool) error {
	if task.PromotionID == "" {
		return fmt.Errorf("promotion preflight: promotion_id is required")
	}
	if len(task.PartitionIDs) > 1 && task.ExpectedPostRoot != "" && len(task.ExpectedPostRoots) == 0 {
		return fmt.Errorf("promotion %s preflight: multi-partition promotion requires per-partition expected post roots", task.PromotionID)
	}
	if isInsertPromotionKind(task.Kind) && len(task.PartitionIDs) > 0 && len(task.CandidateParts) == 0 {
		return fmt.Errorf("promotion %s preflight: insert promotion requires candidate parts", task.PromotionID)
	}
	if isInsertPromotionKind(task.Kind) {
		if err := requirePhysicalInsertParts("candidate", task.CandidateParts); err != nil {
			return fmt.Errorf("promotion %s preflight: %w", task.PromotionID, err)
		}
		if err := requirePhysicalInsertParts("cleanup", task.CleanupUnsafeParts); err != nil {
			return fmt.Errorf("promotion %s preflight: %w", task.PromotionID, err)
		}
	}
	if !protected {
		return nil
	}
	if task.TableID == "" {
		return fmt.Errorf("promotion %s preflight: table_id is required", task.PromotionID)
	}
	if task.SafeTable == "" {
		return fmt.Errorf("promotion %s preflight: safe_table is required", task.PromotionID)
	}
	if len(task.PartitionIDs) == 0 {
		return fmt.Errorf("promotion %s preflight: partition_ids are required", task.PromotionID)
	}
	if task.LeaderSignature == "" {
		return fmt.Errorf("promotion %s preflight: leader signature is required", task.PromotionID)
	}
	if task.PromotionSeq == 0 || !task.RequirePromotionSeq {
		return fmt.Errorf("promotion %s preflight: promotion sequence is required", task.PromotionID)
	}
	if !task.RequirePostRootCAS {
		return fmt.Errorf("promotion %s preflight: post-root CAS is required", task.PromotionID)
	}
	if err := requireExpectedPostRoots(task); err != nil {
		return fmt.Errorf("promotion %s preflight: %w", task.PromotionID, err)
	}
	if strings.EqualFold(task.Kind, "mutation") {
		if task.BaseSafeSnapshotID == "" {
			return fmt.Errorf("promotion %s preflight: mutation promotion requires base_safe_snapshot_id", task.PromotionID)
		}
		if !task.RequireBaseRootCAS {
			return fmt.Errorf("promotion %s preflight: mutation promotion requires base-root CAS", task.PromotionID)
		}
		if len(task.BasePartitionRoots) == 0 && task.BasePartitionRoot == "" {
			return fmt.Errorf("promotion %s preflight: mutation promotion requires base partition roots", task.PromotionID)
		}
		if len(task.PartitionIDs) > 1 && len(task.BasePartitionRoots) == 0 {
			return fmt.Errorf("promotion %s preflight: multi-partition mutation requires per-partition base roots", task.PromotionID)
		}
		if task.InternalDropPartition {
			if len(task.DropPartitionIDs) == 0 {
				return fmt.Errorf("promotion %s preflight: internal drop requires drop partition ids", task.PromotionID)
			}
		} else if !task.ReplacePartition {
			return fmt.Errorf("promotion %s preflight: mutation promotion must replace or internally drop partitions", task.PromotionID)
		}
	}
	return nil
}

func requirePhysicalInsertParts(kind string, parts []ByteSidePart) error {
	for _, part := range parts {
		if strings.TrimSpace(part.PartName) == "" {
			return fmt.Errorf("insert promotion requires physical %s parts with part_name", kind)
		}
		if part.PartitionID == "" || part.PartitionID == NativeAllPartitionID {
			return fmt.Errorf("insert promotion requires physical %s parts with real partition_id", kind)
		}
	}
	return nil
}

func requireExpectedPostRoots(task PromotionTask) error {
	if len(task.ExpectedPostRoots) == 0 {
		if len(task.PartitionIDs) == 1 && task.ExpectedPostRoot != "" {
			return nil
		}
		return fmt.Errorf("expected post roots are required")
	}
	have := make(map[string]bool, len(task.ExpectedPostRoots))
	for _, root := range task.ExpectedPostRoots {
		if root.PartitionID == "" || root.Root == "" {
			return fmt.Errorf("expected post roots must include partition_id and root")
		}
		have[root.PartitionID] = true
	}
	for _, partitionID := range task.PartitionIDs {
		if !have[partitionID] {
			return fmt.Errorf("missing expected post root for partition %s", partitionID)
		}
	}
	return nil
}

// ValidateCompactionPreflight rejects malformed controlled-compaction commands
// before the compactor touches ClickHouse.
func ValidateCompactionPreflight(task CompactionTask, protected bool) error {
	if task.CompactionID == "" {
		return fmt.Errorf("compaction preflight: compaction_id is required")
	}
	if !protected {
		return nil
	}
	if task.TableID == "" {
		return fmt.Errorf("compaction %s preflight: table_id is required", task.CompactionID)
	}
	if task.SafeTable == "" {
		return fmt.Errorf("compaction %s preflight: safe_table is required", task.CompactionID)
	}
	if len(task.PartitionIDs) == 0 {
		return fmt.Errorf("compaction %s preflight: partition_ids are required", task.CompactionID)
	}
	if task.LeaderSignature == "" {
		return fmt.Errorf("compaction %s preflight: leader signature is required", task.CompactionID)
	}
	if task.PromotionSeq == 0 {
		return fmt.Errorf("compaction %s preflight: promotion sequence is required", task.CompactionID)
	}
	if task.CompactDatabase == "" || task.CompactTable == "" {
		return fmt.Errorf("compaction %s preflight: compact database and table are required", task.CompactionID)
	}
	if task.BaseSafeSnapshotID == "" || task.BasePartitionRoot == "" {
		return fmt.Errorf("compaction %s preflight: base snapshot and root are required", task.CompactionID)
	}
	if task.ExpectedPostRoot == "" {
		return fmt.Errorf("compaction %s preflight: expected post root is required", task.CompactionID)
	}
	if len(task.InputParts) == 0 {
		return fmt.Errorf("compaction %s preflight: input parts are required", task.CompactionID)
	}
	if !task.RequireBaseRootCAS || !task.RequirePostRootCAS {
		return fmt.Errorf("compaction %s preflight: base and post root CAS are required", task.CompactionID)
	}
	return nil
}
