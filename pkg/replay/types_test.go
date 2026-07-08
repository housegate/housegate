package replay

import "testing"

func manifestFixture() SafeSnapshotManifest {
	return SafeSnapshotManifest{
		SafeL3BlockSeq:    3,
		SchemaSnapshotID:  "schema-genesis",
		SchemaRoot:        "0xschr",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []TableManifest{{
			TableID:    "db.t",
			SchemaHash: "0xsh",
			PartitionRoots: []PartitionCommitment{{
				TableID:     "db.t",
				PartitionID: "p0",
				Root:        "0xr0",
			}},
			ActiveParts: []PartManifestEntry{{
				TableID:       "db.t",
				PartitionID:   "p0",
				PartName:      "all_1_1_0",
				PartPhysHash:  "0xphys",
				PartRowLtHash: "0xrow",
				RowCount:      7,
				Bytes:         1234,
			}},
		}},
	}
}

func TestDataRoot_IgnoresPartIdentityFields(t *testing.T) {
	a, err := manifestFixture().Seal()
	if err != nil {
		t.Fatalf("seal a: %v", err)
	}

	mutated := manifestFixture()
	mutated.Tables[0].ActiveParts[0].PartName = "different_9_9_9"
	mutated.Tables[0].ActiveParts[0].PartPhysHash = "0xotherphys"
	mutated.Tables[0].ActiveParts[0].Bytes = 999999
	mutated.Tables[0].ActiveParts[0].StorageRefs = []string{"s3://x"}
	b, err := mutated.Seal()
	if err != nil {
		t.Fatalf("seal b: %v", err)
	}

	if a.DataRoot != b.DataRoot || a.StateRoot != b.StateRoot {
		t.Fatalf("data/state roots must ignore part identity fields: %s/%s vs %s/%s", a.DataRoot, a.StateRoot, b.DataRoot, b.StateRoot)
	}
	if a.ManifestRoot == b.ManifestRoot {
		t.Fatal("manifest root must still cover ActiveParts")
	}
}

func TestDataRoot_SensitiveToPartitionRoots(t *testing.T) {
	a, err := manifestFixture().Seal()
	if err != nil {
		t.Fatalf("seal a: %v", err)
	}

	mutated := manifestFixture()
	mutated.Tables[0].PartitionRoots[0].Root = "0xEVIL"
	b, err := mutated.Seal()
	if err != nil {
		t.Fatalf("seal b: %v", err)
	}

	if a.DataRoot == b.DataRoot {
		t.Fatal("data root must commit partition roots")
	}
}

func TestAssembleStateRoot_MatchesSeal(t *testing.T) {
	m, err := manifestFixture().Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	dataRoot, stateRoot, err := AssembleStateRoot(m.SchemaSnapshotID, m.SchemaRoot, m.ExecutorProfileID, manifestFixture().Tables)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if dataRoot != m.DataRoot || stateRoot != m.StateRoot {
		t.Fatalf("assembly must match Seal: %s/%s vs %s/%s", dataRoot, stateRoot, m.DataRoot, m.StateRoot)
	}
}
