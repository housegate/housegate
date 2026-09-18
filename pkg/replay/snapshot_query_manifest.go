package replay

import (
	"fmt"
	"reflect"
)

// ValidateSnapshotQueryManifest matches the entire descriptor for each selected
// read table, and requires an explicit target even at empty genesis. The caller
// must authenticate publication/network/shard and the analyzer's read-table set.
func ValidateSnapshotQueryManifest(in SnapshotQueryInput, manifest SafeSnapshotManifest) error {
	if err := ValidateSnapshotQueryInput(in); err != nil {
		return err
	}
	if in.Binding.ExecutorProfileID != manifest.ExecutorProfileID {
		return fmt.Errorf("selected manifest executor_profile_id mismatch")
	}
	p := in.Binding.ReadSnapshot
	if p.SnapshotID != manifest.SnapshotID || p.SafeBlockSeq != manifest.SafeBlockSeq || p.ManifestRoot != manifest.ManifestRoot || p.StateRoot != manifest.StateRoot || p.SchemaSnapshotID != manifest.SchemaSnapshotID || p.SchemaRoot != manifest.SchemaRoot {
		return fmt.Errorf("selected manifest pin mismatch")
	}
	tables := map[string]SnapshotReadTable{}
	for _, t := range manifest.Tables {
		if _, ok := tables[t.TableID]; ok {
			return fmt.Errorf("duplicate manifest table identity")
		}
		parts := make([]SnapshotReadPart, 0, len(t.ActiveParts))
		for _, p := range t.ActiveParts {
			parts = append(parts, queryPartProjection(p))
		}
		n, err := canonicalQueryTable(SnapshotReadTable{Database: "manifest", Table: t.TableID, TableID: t.TableID, SchemaHash: t.SchemaHash, PartitionRoots: t.PartitionRoots, ActiveParts: parts})
		if err != nil {
			return fmt.Errorf("manifest: %w", err)
		}
		tables[t.TableID] = n
	}
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	target, ok := tables[in.Binding.TargetTableID]
	if !ok {
		return fmt.Errorf("target table missing from manifest (empty genesis must be explicit)")
	}
	if target.SchemaHash != in.Binding.SchemaHash {
		return fmt.Errorf("target schema_hash mismatch")
	}
	for _, t := range in.ReadSet.Tables {
		want, ok := tables[t.TableID]
		if !ok {
			return fmt.Errorf("read table missing from manifest")
		}
		n, err := canonicalQueryTable(t)
		if err != nil {
			return err
		}
		if n.SchemaHash != want.SchemaHash || !reflect.DeepEqual(n.PartitionRoots, want.PartitionRoots) || !reflect.DeepEqual(n.ActiveParts, want.ActiveParts) {
			return fmt.Errorf("incomplete or conflicting read table manifest projection: %s", t.TableID)
		}
	}
	return nil
}
