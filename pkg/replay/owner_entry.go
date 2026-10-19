package replay

// OwnerEntry returns track_frontiers[O] of manifest m: the TrackFrontier whose
// IndexerID is indexerID (spec 2026-10-14 §8.4, §8.5 step 7: a member keys
// its installs and checkpoints on the owner's entry). It returns false when m
// carries no frontiers (a manifest published before the tracks activation) or
// does not list that indexer. It reads the list as it is and does not check
// its order; Validate does.
func OwnerEntry(m SafeSnapshotManifest, indexerID uint64) (TrackFrontier, bool) {
	for _, f := range m.TrackFrontiers {
		if f.IndexerID == indexerID {
			return f, true
		}
	}
	return TrackFrontier{}, false
}
