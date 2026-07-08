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
		wantDataRoot     = "0x95bf867d6246aaf6033e9811792564b502d2ec70c12f2e0640d90ac3c8aca763"
		wantStateRoot    = "0xcb7123390af5c79de0966cc45a6ad14a627802fdd125886ec41d9cdb2b3c1b2f"
		wantManifestRoot = "0x5b10e055fb8f300c78173a093e62c3d27a509920fea7c46b69e60f679cc91797"
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
