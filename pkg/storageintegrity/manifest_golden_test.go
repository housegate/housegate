package storageintegrity

import (
	"testing"

	"housegate/housegate/pkg/replay"
)

// TestGoldenManifestVectorMatchesMock pins replay.SafeSnapshotManifest's sealing
// profile to the constants that sentio-storage-mocks' ported implementation
// asserts (mockstorage.TestManifestGoldenVector). The mock module cannot import
// this package, so this test is the tripwire: if replay's hashing profile
// changes, this test breaks first, signalling that the mock's manifest.go port
// and its golden constants must be updated in lockstep.
func TestGoldenManifestVectorMatchesMock(t *testing.T) {
	const (
		wantDataRoot     = "0x797754046a86a1b790aec5a1d0b3e051bbac1c199f2428ad5daa68480885ecea"
		wantStateRoot    = "0x0069cc761092f2b5b1c6bf3c62b861f759546963c066aa7a07c9f1ad09fcc2e2"
		wantManifestRoot = "0xaa89c69e478a357aa873b5e758173a6ebbe53fd49d63e6a9b98ef92d3b95b007"
	)

	m := replay.SafeSnapshotManifest{
		ParentSnapshotID:  "parent-1",
		SafeL3BlockSeq:      7,
		SchemaSnapshotID:  "schema-snap-1",
		SchemaRoot:        "schema-root-1",
		ExecutorProfileID: "exec-profile-1",
		Tables: []replay.TableManifest{{
			TableID:    "tenant.events",
			SchemaHash: "schema-root-1",
			PartitionRoots: []replay.PartitionCommitment{
				{TableID: "tenant.events", PartitionID: "202607", Root: "root-b"},
				{TableID: "tenant.events", PartitionID: "202606", Root: "root-a"},
			},
			ActiveParts: []replay.PartManifestEntry{
				{TableID: "tenant.events", PartitionID: "202607", PartName: "p_2", PartRowLtHash: "lt-2", RowCount: 3},
				{TableID: "tenant.events", PartitionID: "202606", PartName: "p_1", PartRowLtHash: "lt-1", RowCount: 2},
			},
		}},
	}
	sealed, err := m.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if sealed.DataRoot != wantDataRoot {
		t.Fatalf("data_root = %s, want %s — replay profile changed; update mock manifest.go + both golden vectors", sealed.DataRoot, wantDataRoot)
	}
	if sealed.StateRoot != wantStateRoot {
		t.Fatalf("state_root = %s, want %s — replay profile changed; update mock manifest.go + both golden vectors", sealed.StateRoot, wantStateRoot)
	}
	if sealed.ManifestRoot != wantManifestRoot {
		t.Fatalf("manifest_root = %s, want %s — replay profile changed; update mock manifest.go + both golden vectors", sealed.ManifestRoot, wantManifestRoot)
	}
}
