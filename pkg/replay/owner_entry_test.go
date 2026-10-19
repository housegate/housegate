package replay

import "testing"

// TestOwnerEntry pins track_frontiers[O] (spec 2026-10-14 §8.4, §8.5 step 7):
// the entry of the asked indexer, false for an indexer the manifest does not
// list and for a manifest published before the tracks activation, and indexer
// 0 is an ordinary indexer, not "absent".
func TestOwnerEntry(t *testing.T) {
	m := SafeSnapshotManifest{TrackFrontiers: []TrackFrontier{
		{IndexerID: 0, SafeBlockSeq: 40, AppliedPromotionSeq: 7},
		{IndexerID: 1, SafeBlockSeq: 41, AppliedPromotionSeq: 9},
		{IndexerID: 5, SafeBlockSeq: 0, AppliedPromotionSeq: 0},
	}}
	for _, tc := range []struct {
		indexer uint64
		want    TrackFrontier
		ok      bool
	}{
		{0, TrackFrontier{IndexerID: 0, SafeBlockSeq: 40, AppliedPromotionSeq: 7}, true},
		{1, TrackFrontier{IndexerID: 1, SafeBlockSeq: 41, AppliedPromotionSeq: 9}, true},
		{5, TrackFrontier{IndexerID: 5}, true},
		{2, TrackFrontier{}, false},
		{6, TrackFrontier{}, false},
	} {
		got, ok := OwnerEntry(m, tc.indexer)
		if ok != tc.ok || got != tc.want {
			t.Errorf("OwnerEntry(m, %d) = %+v, %v; want %+v, %v", tc.indexer, got, ok, tc.want, tc.ok)
		}
	}

	for _, empty := range []SafeSnapshotManifest{{}, {TrackFrontiers: []TrackFrontier{}}} {
		if got, ok := OwnerEntry(empty, 0); ok || got != (TrackFrontier{}) {
			t.Errorf("OwnerEntry of a manifest without frontiers = %+v, %v; want the zero entry and false", got, ok)
		}
	}

	// OwnerEntry reads the list as it is: the order is Validate's to check.
	unsorted := SafeSnapshotManifest{TrackFrontiers: []TrackFrontier{{IndexerID: 3, SafeBlockSeq: 2}, {IndexerID: 1, SafeBlockSeq: 1}}}
	if got, ok := OwnerEntry(unsorted, 1); !ok || got.SafeBlockSeq != 1 {
		t.Errorf("OwnerEntry(unsorted, 1) = %+v, %v; want the listed entry", got, ok)
	}
}
