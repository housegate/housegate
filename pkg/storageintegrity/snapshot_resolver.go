package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// validateManifestPartitionRoots checks that each partition commitment's root
// equals the additive sum of its active parts' part_row_lthash (HG-P1-04). A
// partition with no enumerated active parts is skipped (its parts are covered
// only by the manifest root, per the content-addressed data-root design). This
// is the lthash-aware semantic check that the dependency-free replay.Validate
// leaves to the storageintegrity layer.
func validateManifestPartitionRoots(m replay.SafeSnapshotManifest) error {
	for _, t := range m.Tables {
		// Which partitions actually have enumerated parts?
		partsInPartition := map[string]bool{}
		for _, p := range t.ActiveParts {
			partsInPartition[p.PartitionID] = true
		}
		for _, pr := range t.PartitionRoots {
			if !partsInPartition[pr.PartitionID] {
				continue
			}
			got := partitionRootFromActiveParts(t.ActiveParts, pr.PartitionID)
			if got != pr.Root {
				return fmt.Errorf("table %q partition %q root %s does not equal the sum of its active parts %s", t.TableID, pr.PartitionID, pr.Root, got)
			}
		}
	}
	return nil
}

type SnapshotResolver struct {
	Reader      SnapshotReader
	ActiveParts ActivePartReader
}

type SnapshotResolveRequest struct {
	SnapshotID   string
	TableID      string
	SafeTable    string
	PartitionIDs []string
	VerifyActive bool
}

type ResolvedSnapshot struct {
	Watermark   SafeWatermark
	Manifest    replay.SafeSnapshotManifest
	ActiveParts []replay.PartManifestEntry
}

func (r SnapshotResolver) Latest(ctx context.Context) (ResolvedSnapshot, error) {
	if r.Reader == nil {
		return ResolvedSnapshot{}, fmt.Errorf("snapshot reader is required")
	}
	watermark, err := r.Reader.GetSafeWatermark(ctx)
	if err != nil {
		return ResolvedSnapshot{}, fmt.Errorf("get safe watermark: %w", err)
	}
	if watermark.SnapshotID == "" {
		return ResolvedSnapshot{}, fmt.Errorf("safe watermark snapshot_id is required")
	}
	resolved, err := r.ResolveLocal(ctx, SnapshotResolveRequest{SnapshotID: watermark.SnapshotID})
	if err != nil {
		return ResolvedSnapshot{}, err
	}
	resolved.Watermark = watermark
	if watermark.StateRoot != "" && resolved.Manifest.StateRoot != watermark.StateRoot {
		return ResolvedSnapshot{}, fmt.Errorf("safe watermark state_root mismatch: manifest %s watermark %s", resolved.Manifest.StateRoot, watermark.StateRoot)
	}
	if watermark.ManifestRoot != "" && resolved.Manifest.ManifestRoot != watermark.ManifestRoot {
		return ResolvedSnapshot{}, fmt.Errorf("safe watermark manifest_root mismatch: manifest %s watermark %s", resolved.Manifest.ManifestRoot, watermark.ManifestRoot)
	}
	if watermark.SafeL3BlockSeq != 0 && resolved.Manifest.SafeL3BlockSeq != watermark.SafeL3BlockSeq {
		return ResolvedSnapshot{}, fmt.Errorf("safe watermark block mismatch: manifest %d watermark %d", resolved.Manifest.SafeL3BlockSeq, watermark.SafeL3BlockSeq)
	}
	return resolved, nil
}

func (r SnapshotResolver) ResolveLocal(ctx context.Context, req SnapshotResolveRequest) (ResolvedSnapshot, error) {
	if r.Reader == nil {
		return ResolvedSnapshot{}, fmt.Errorf("snapshot reader is required")
	}
	if req.SnapshotID == "" {
		return ResolvedSnapshot{}, fmt.Errorf("snapshot_id is required")
	}
	manifest, ok, err := r.Reader.GetSafeSnapshot(ctx, req.SnapshotID)
	if err != nil {
		return ResolvedSnapshot{}, fmt.Errorf("get safe snapshot %s: %w", req.SnapshotID, err)
	}
	if !ok {
		return ResolvedSnapshot{}, fmt.Errorf("safe snapshot %s not found", req.SnapshotID)
	}
	if err := manifest.Validate(); err != nil {
		return ResolvedSnapshot{}, fmt.Errorf("validate safe snapshot %s: %w", req.SnapshotID, err)
	}
	// HG-P1-04: semantic (lthash) validation the dependency-free replay.Validate
	// cannot do — each partition's declared root must equal the additive sum of
	// its active parts' part_row_lthash. This binds the content-addressed
	// partition commitments to the enumerated parts, so a manifest whose
	// partition root does not match its parts is rejected before use.
	if err := validateManifestPartitionRoots(manifest); err != nil {
		return ResolvedSnapshot{}, fmt.Errorf("validate safe snapshot %s partition roots: %w", req.SnapshotID, err)
	}
	if manifest.SnapshotID != req.SnapshotID {
		return ResolvedSnapshot{}, fmt.Errorf("snapshot id mismatch: got %s want %s", manifest.SnapshotID, req.SnapshotID)
	}
	resolved := ResolvedSnapshot{Manifest: manifest}
	if !req.VerifyActive {
		return resolved, nil
	}
	if req.SafeTable == "" {
		return ResolvedSnapshot{}, fmt.Errorf("safe_table is required for active part verification")
	}
	if r.ActiveParts == nil {
		return ResolvedSnapshot{}, fmt.Errorf("active part reader is required for active part verification")
	}
	tableID := req.TableID
	if tableID == "" {
		tableID = tableIDFromManifest(manifest)
	}
	if tableID == "" {
		tableID = normalizeTableID(req.SafeTable)
	}
	active, err := readActivePartsWithTableID(ctx, r.ActiveParts, req.SafeTable, req.PartitionIDs, tableID)
	if err != nil {
		return ResolvedSnapshot{}, fmt.Errorf("read local active parts: %w", err)
	}
	expected := manifestActiveParts(manifest, tableID, req.PartitionIDs)
	if !activePartsEqual(active, expected) {
		return ResolvedSnapshot{}, fmt.Errorf("active parts mismatch for snapshot %s table %s", req.SnapshotID, tableID)
	}
	resolved.ActiveParts = active
	return resolved, nil
}

func (r SnapshotResolver) VerifyLocalTable(ctx context.Context, snapshotID, tableID, safeTable string, partitionIDs []string) error {
	_, err := r.ResolveLocal(ctx, SnapshotResolveRequest{
		SnapshotID:   snapshotID,
		TableID:      tableID,
		SafeTable:    safeTable,
		PartitionIDs: append([]string(nil), partitionIDs...),
		VerifyActive: true,
	})
	return err
}

type SnapshotStoreAdapter struct {
	Resolver SnapshotResolver
}

func (a SnapshotStoreAdapter) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	resolved, err := a.Resolver.ResolveLocal(ctx, SnapshotResolveRequest{SnapshotID: snapshotID})
	if err != nil {
		return replay.SafeSnapshotManifest{}, err
	}
	return resolved.Manifest, nil
}
