package storageintegrity

import (
	"testing"

	"housegate/housegate/pkg/replay"
)

func replacePlanFixture() PartitionInstallPlan {
	return PartitionInstallPlan{
		TableID:     "net1.events",
		PartitionID: "20260720",
		Action:      PublicationActionReplacePartition,
		CanonicalParts: []replay.PartManifestEntry{
			{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_2_2_0", PartRowLtHash: "lt-b", RowCount: 2, Bytes: 80},
			{TableID: "net1.events", PartitionID: "20260720", PartName: "20260720_1_1_0", PartRowLtHash: "lt-a", RowCount: 3, Bytes: 100},
		},
	}
}

func TestBuildReplacePartitionPlan_EmitsCanonicalSQL(t *testing.T) {
	rp, err := BuildReplacePartitionPlan("m-1", 3, replacePlanFixture())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	wantSQL := "ALTER TABLE `hg_safe`.`net1.events` REPLACE PARTITION ID '20260720' FROM `hg_mutation_publish`.`m-1__3`;"
	if rp.SQL != wantSQL {
		t.Fatalf("SQL:\n got %q\nwant %q", rp.SQL, wantSQL)
	}
	if rp.ShadowDatabase != "hg_mutation_publish" || rp.ShadowTable != "m-1__3" {
		t.Fatalf("shadow db/table wrong: %s.%s", rp.ShadowDatabase, rp.ShadowTable)
	}
	// Canonical parts sorted by name.
	if len(rp.CanonicalParts) != 2 || rp.CanonicalParts[0].PartName != "20260720_1_1_0" || rp.CanonicalParts[1].PartName != "20260720_2_2_0" {
		t.Fatalf("canonical parts not sorted: %+v", rp.CanonicalParts)
	}
}

func TestBuildReplacePartitionPlan_RejectsNonReplace(t *testing.T) {
	for _, action := range []PublicationAction{PublicationActionDropPartition, PublicationActionUnspecified} {
		plan := replacePlanFixture()
		plan.Action = action
		if _, err := BuildReplacePartitionPlan("m-1", 3, plan); err == nil {
			t.Fatalf("action %s must be rejected by the REPLACE builder", action)
		}
	}
}

func TestBuildReplacePartitionPlan_RejectsEmptyPlan(t *testing.T) {
	plan := replacePlanFixture()
	plan.CanonicalParts = nil
	if _, err := BuildReplacePartitionPlan("m-1", 3, plan); err == nil {
		t.Fatal("a REPLACE with no canonical parts must fail closed")
	}
}

func TestBuildReplacePartitionPlan_RejectsBlankIdentity(t *testing.T) {
	cases := []struct {
		name string
		mut  string
		seq  uint64
		tbl  string
		part string
	}{
		{"blank mutation", "", 3, "net1.events", "p1"},
		{"zero seq", "m-1", 0, "net1.events", "p1"},
		{"blank table", "m-1", 3, "", "p1"},
		{"blank partition", "m-1", 3, "net1.events", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := replacePlanFixture()
			plan.TableID = tc.tbl
			plan.PartitionID = tc.part
			if _, err := BuildReplacePartitionPlan(tc.mut, tc.seq, plan); err == nil {
				t.Fatal("blank identity must fail closed")
			}
		})
	}
}

func TestBuildReplacePartitionPlan_DoesNotMutateInput(t *testing.T) {
	plan := replacePlanFixture()
	firstName := plan.CanonicalParts[0].PartName // unsorted: "..._2_2_0"
	if _, err := BuildReplacePartitionPlan("m-1", 3, plan); err != nil {
		t.Fatalf("build: %v", err)
	}
	if plan.CanonicalParts[0].PartName != firstName {
		t.Fatal("builder must not mutate the input plan's part slice order")
	}
}

// --- Gated: real REPLACE execution needs the absent C2 seam. ---

func TestReplacePartition_RealExecutionAgainstShadow(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion REPLACE PARTITION execution seam lands")
}
