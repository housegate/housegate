package replay

import (
	"strings"
	"testing"
)

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

// TestValidateRejectsSemanticViolations proves HG-P1-04: a manifest that is
// hash-consistent must also satisfy the structural invariants — unique
// table/partition/part keys, required part fields, and parts filed under a
// declared partition of their table. Each mutation is re-sealed so the hash
// checks pass and only the semantic check can fail.
func TestValidateRejectsSemanticViolations(t *testing.T) {
	sealValid := func() SafeSnapshotManifest {
		m, err := manifestFixture().Seal()
		if err != nil {
			t.Fatalf("seal: %v", err)
		}
		return m
	}
	if err := sealValid().Validate(); err != nil {
		t.Fatalf("well-formed manifest must validate: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*SafeSnapshotManifest)
		want   string
	}{
		{"duplicate partition commitment", func(m *SafeSnapshotManifest) {
			m.Tables[0].PartitionRoots = append(m.Tables[0].PartitionRoots, PartitionCommitment{TableID: "db.t", PartitionID: "p0", Root: "0xr0b"})
		}, "duplicate partition commitment"},
		{"empty part name", func(m *SafeSnapshotManifest) {
			m.Tables[0].ActiveParts[0].PartName = ""
		}, "empty part_name"},
		{"empty part_row_lthash", func(m *SafeSnapshotManifest) {
			m.Tables[0].ActiveParts[0].PartRowLtHash = ""
		}, "empty part_row_lthash"},
		// Physical metadata is enforced per-table only when some part carries a
		// part_phys_hash (the enriched-manifest signal). A MIXED manifest — one
		// part enriched, another missing the physical checksum — is rejected; a
		// wholly content-only manifest (no part_phys_hash anywhere) is not (see
		// TestValidateAcceptsContentOnlyManifest).
		{"mixed part_phys_hash (one enriched, one missing)", func(m *SafeSnapshotManifest) {
			m.Tables[0].ActiveParts = append(m.Tables[0].ActiveParts, PartManifestEntry{
				TableID: "db.t", PartitionID: "p0", PartName: "all_2_2_0",
				PartRowLtHash: "0xrow2", RowCount: 3, Bytes: 99, // no PartPhysHash
			})
		}, "empty part_phys_hash"},
		{"mixed bytes (one enriched, one missing bytes)", func(m *SafeSnapshotManifest) {
			m.Tables[0].ActiveParts = append(m.Tables[0].ActiveParts, PartManifestEntry{
				TableID: "db.t", PartitionID: "p0", PartName: "all_2_2_0",
				PartPhysHash: "0xphys2", PartRowLtHash: "0xrow2", RowCount: 3, // no Bytes
			})
		}, "empty bytes"},
		{"part in undeclared partition", func(m *SafeSnapshotManifest) {
			m.Tables[0].ActiveParts[0].PartitionID = "p_ghost"
		}, "no partition commitment"},
		{"duplicate active part name", func(m *SafeSnapshotManifest) {
			dup := m.Tables[0].ActiveParts[0]
			m.Tables[0].ActiveParts = append(m.Tables[0].ActiveParts, dup)
		}, "duplicate active part"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := manifestFixture()
			tc.mutate(&m)
			sealed, err := m.Seal()
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			err = sealed.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

// TestValidateAcceptsContentOnlyManifest proves the physical-metadata invariant
// is conditional: a manifest whose active parts carry only the content
// commitment (part_row_lthash) — no part_phys_hash on ANY part — is valid. Such
// manifests arise on the source-claim and mutation-touched-set read paths, which
// the proxy serves without the ClickHouse system.parts physical checksum. The
// physical fields (part_phys_hash + bytes) are required only once a producer
// enriches the manifest (any part carries a part_phys_hash); see the mixed
// cases in TestValidateRejectsSemanticViolations.
func TestValidateAcceptsContentOnlyManifest(t *testing.T) {
	m := manifestFixture()
	// Strip physical metadata from every part -> pure content-addressed manifest.
	for i := range m.Tables[0].ActiveParts {
		m.Tables[0].ActiveParts[i].PartPhysHash = ""
		m.Tables[0].ActiveParts[i].Bytes = 0
	}
	sealed, err := m.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := sealed.Validate(); err != nil {
		t.Fatalf("content-only manifest must validate, got: %v", err)
	}
}
