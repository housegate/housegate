package storageintegrity

import (
	"testing"

	"housegate/housegate/pkg/replay"
)

func artifactFixture() CanonicalArtifact {
	return CanonicalArtifact{
		MutationID:         "m-1",
		PublicationSeq:     1,
		ArtifactCommitment: "commit-1",
		ArtifactSource:     "ledger-majority",
		SchemaSnapshotID:   "schema-1",
		ExecutorProfileID:  "profile-1",
		PrevSafeSnapshotID: "safe-1",
		AffectedPartitions: []replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"},
			{TableID: "net1.events", PartitionID: "p2", Root: "base-p2"},
		},
		PostPartitionCommitments: []replay.PartitionCommitment{
			{TableID: "net1.events", PartitionID: "p1", Root: "post-p1"},
			{TableID: "net1.events", PartitionID: "p2", Root: ""}, // empty-DELETE
		},
		PostStateRoot: "post-root-1",
		CanonicalParts: []replay.PartManifestEntry{
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartPhysHash: "ph-a", PartRowLtHash: "lt-a", RowCount: 3, Bytes: 100},
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_2_2_0", PartPhysHash: "ph-b", PartRowLtHash: "lt-b", RowCount: 2, Bytes: 80},
			// p2 has zero parts (empty-DELETE) -> DROP.
		},
	}
}

// --- Green-today: canonical install-plan construction + sameness invariant. ---

func TestBuildCanonicalPublicationSet_ProducesExactActivePartsFromArtifact(t *testing.T) {
	set, err := BuildCanonicalPublicationSet(artifactFixture())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if set.MutationID != "m-1" || set.PublicationSeq != 1 || len(set.Plans) != 2 {
		t.Fatalf("set header/plan count wrong: %+v", set)
	}
	// p1 is a REPLACE with exactly the two canonical parts, sorted by name.
	p1 := set.Plans[0]
	if p1.PartitionID != "p1" || p1.Action != PublicationActionReplacePartition || len(p1.CanonicalParts) != 2 {
		t.Fatalf("p1 plan wrong: %+v", p1)
	}
	if p1.CanonicalParts[0].PartName != "p1_1_1_0" || p1.CanonicalParts[1].PartName != "p1_2_2_0" {
		t.Fatalf("p1 parts not canonically ordered: %+v", p1.CanonicalParts)
	}
	if p1.CanonicalParts[0].PartPhysHash != "ph-a" || p1.CanonicalParts[0].RowCount != 3 {
		t.Fatal("canonical part content must be carried exactly from the artifact")
	}
}

func TestBuildCanonicalPublicationSet_NotDerivedFromLocalScratch(t *testing.T) {
	// A "local scratch" part list with the same logical intent but different part
	// names must NOT influence the plan — it is not even an input.
	localScratch := []replay.PartManifestEntry{
		{TableID: "net1.events", PartitionID: "p1", PartName: "scratch_9_9_0", PartPhysHash: "scratch-ph", RowCount: 5},
	}
	_ = localScratch
	set, err := BuildCanonicalPublicationSet(artifactFixture())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, p := range set.Plans[0].CanonicalParts {
		if p.PartName == "scratch_9_9_0" || p.PartPhysHash == "scratch-ph" {
			t.Fatal("plan must come from the canonical artifact, never from local scratch")
		}
	}
}

func TestBuildCanonicalPublicationSet_EmptyPartitionYieldsDropAction(t *testing.T) {
	set, err := BuildCanonicalPublicationSet(artifactFixture())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	p2 := set.Plans[1]
	if p2.PartitionID != "p2" || p2.Action != PublicationActionDropPartition || len(p2.CanonicalParts) != 0 {
		t.Fatalf("empty partition must be a DROP with no parts: %+v", p2)
	}
}

func TestBuildCanonicalPublicationSet_NonEmptyPartitionYieldsReplaceAction(t *testing.T) {
	set, _ := BuildCanonicalPublicationSet(artifactFixture())
	if set.Plans[0].Action != PublicationActionReplacePartition {
		t.Fatal("a partition with canonical parts must be a REPLACE")
	}
}

func TestBuildCanonicalPublicationSet_RejectsPartOutsideAffectedPartitions(t *testing.T) {
	art := artifactFixture()
	art.CanonicalParts = append(art.CanonicalParts, replay.PartManifestEntry{TableID: "net1.events", PartitionID: "p9", PartName: "x"})
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("a part outside the affected partitions must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_RejectsDropWithNonZeroPostCommitment(t *testing.T) {
	art := artifactFixture()
	// p2 has no canonical parts but give it a non-zero post commitment.
	art.PostPartitionCommitments = []replay.PartitionCommitment{
		{TableID: "net1.events", PartitionID: "p1", Root: "post-p1"},
		{TableID: "net1.events", PartitionID: "p2", Root: "non-zero"},
	}
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("a non-zero post commitment with no canonical parts must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_RejectsIncompleteArtifact(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*CanonicalArtifact)
	}{
		{"missing mutation id", func(a *CanonicalArtifact) { a.MutationID = "" }},
		{"zero publication seq", func(a *CanonicalArtifact) { a.PublicationSeq = 0 }},
		{"empty commitment", func(a *CanonicalArtifact) { a.ArtifactCommitment = "" }},
		{"empty source", func(a *CanonicalArtifact) { a.ArtifactSource = "" }},
		{"empty schema snapshot", func(a *CanonicalArtifact) { a.SchemaSnapshotID = "" }},
		{"empty executor profile", func(a *CanonicalArtifact) { a.ExecutorProfileID = "" }},
		{"empty prev safe", func(a *CanonicalArtifact) { a.PrevSafeSnapshotID = "" }},
		{"no affected partitions", func(a *CanonicalArtifact) { a.AffectedPartitions = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			art := artifactFixture()
			tc.mutate(&art)
			if _, err := BuildCanonicalPublicationSet(art); err == nil {
				t.Fatal("incomplete artifact must fail closed")
			}
		})
	}
}

// TestBuildCanonicalPublicationSet_RejectsMissingPostCommitment pins the fix for
// the fail-open gap: an affected partition with no canonical parts AND no post
// commitment must fail closed, NOT silently become a DROP PARTITION. A DROP
// requires an explicit, present, zero-root post commitment.
// TestBuildCanonicalPublicationSet_RejectsDuplicateAffectedPartition pins that a
// malformed artifact repeating the same (table_id, partition_id) in
// AffectedPartitions fails closed, rather than producing two identical plans for
// one partition (which would make a retained worker run the same destructive
// REPLACE/DROP twice).
func TestBuildCanonicalPublicationSet_RejectsDuplicateAffectedPartition(t *testing.T) {
	art := artifactFixture()
	art.AffectedPartitions = append(art.AffectedPartitions,
		replay.PartitionCommitment{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"})
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("a duplicate affected-partition entry must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_RejectsMissingPostCommitment(t *testing.T) {
	art := artifactFixture()
	// Drop p2's post commitment entirely (p2 also has no canonical parts). Under
	// the old logic this yielded a DROP; it must now be rejected.
	art.PostPartitionCommitments = []replay.PartitionCommitment{
		{TableID: "net1.events", PartitionID: "p1", Root: "post-p1"},
	}
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("an affected partition with no post commitment must fail closed, not become a DROP")
	}
}

// TestBuildCanonicalPublicationSet_EmptyDeleteRequiresExplicitZeroCommitment
// confirms the legitimate empty-delete shape: no canonical parts WITH an
// explicit present zero-root post commitment yields DROP.
func TestBuildCanonicalPublicationSet_EmptyDeleteRequiresExplicitZeroCommitment(t *testing.T) {
	set, err := BuildCanonicalPublicationSet(artifactFixture()) // p2 has an explicit {Root:""} commitment
	if err != nil {
		t.Fatalf("explicit zero-root empty-delete must build: %v", err)
	}
	if set.Plans[1].Action != PublicationActionDropPartition {
		t.Fatal("an explicit zero-root post commitment with no parts must be a DROP")
	}
}

func TestBuildCanonicalPublicationSet_RejectsDuplicatePostCommitment(t *testing.T) {
	art := artifactFixture()
	art.PostPartitionCommitments = append(art.PostPartitionCommitments,
		replay.PartitionCommitment{TableID: "net1.events", PartitionID: "p1", Root: "dup"})
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("a duplicate post commitment for one partition must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_RejectsExtraneousPostCommitment(t *testing.T) {
	art := artifactFixture()
	art.PostPartitionCommitments = append(art.PostPartitionCommitments,
		replay.PartitionCommitment{TableID: "net1.events", PartitionID: "p9", Root: "x"})
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("a post commitment outside the affected partitions must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_RejectsDuplicatePartNames(t *testing.T) {
	art := artifactFixture()
	art.CanonicalParts = append(art.CanonicalParts, replay.PartManifestEntry{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0"})
	if _, err := BuildCanonicalPublicationSet(art); err == nil {
		t.Fatal("duplicate part names in one partition must fail closed")
	}
}

func TestBuildCanonicalPublicationSet_DeterministicOrdering(t *testing.T) {
	art := artifactFixture()
	// Shuffle affected partitions and parts.
	art.AffectedPartitions = []replay.PartitionCommitment{art.AffectedPartitions[1], art.AffectedPartitions[0]}
	art.CanonicalParts = []replay.PartManifestEntry{art.CanonicalParts[1], art.CanonicalParts[0]}
	set1, err := BuildCanonicalPublicationSet(art)
	if err != nil {
		t.Fatalf("build shuffled: %v", err)
	}
	set2, _ := BuildCanonicalPublicationSet(artifactFixture())
	if !canonicalPublicationSetEqual(set1, set2) {
		t.Fatal("shuffled input must produce an identical canonical set")
	}
}

func TestAssertRetainedWorkersInstallSame_AllMatch(t *testing.T) {
	set, _ := BuildCanonicalPublicationSet(artifactFixture())
	readbacks := map[string]CanonicalPublicationSet{"w1": set, "w2": set, "w3": set}
	if err := AssertRetainedWorkersInstallSame(set, []string{"w1", "w2", "w3"}, readbacks); err != nil {
		t.Fatalf("identical readbacks must pass: %v", err)
	}
}

func TestAssertRetainedWorkersInstallSame_DifferentPartNamesFailsClosed(t *testing.T) {
	set, _ := BuildCanonicalPublicationSet(artifactFixture())
	// w2's readback has the same shape but a different part name (logical root
	// equal, physical inventory different) — unsupported by the current profile.
	divergent, _ := BuildCanonicalPublicationSet(artifactFixture())
	divergent.Plans[0].CanonicalParts[0].PartName = "different_name"
	readbacks := map[string]CanonicalPublicationSet{"w1": set, "w2": divergent, "w3": set}
	err := AssertRetainedWorkersInstallSame(set, []string{"w1", "w2", "w3"}, readbacks)
	if err == nil {
		t.Fatal("a different physical part name must fail closed")
	}
	if !contains(err.Error(), "physical part inventory") && !contains(err.Error(), "physical inventory") {
		t.Fatalf("error must name the versioned-profile rule: %v", err)
	}
}

func TestAssertRetainedWorkersInstallSame_MissingWorkerFailsClosed(t *testing.T) {
	set, _ := BuildCanonicalPublicationSet(artifactFixture())
	readbacks := map[string]CanonicalPublicationSet{"w1": set}
	if err := AssertRetainedWorkersInstallSame(set, []string{"w1", "w2"}, readbacks); err == nil {
		t.Fatal("a missing retained worker readback must fail closed")
	}
}

func TestAssertRetainedWorkersInstallSame_EmptyReadbacksFailsClosed(t *testing.T) {
	set, _ := BuildCanonicalPublicationSet(artifactFixture())
	if err := AssertRetainedWorkersInstallSame(set, []string{"w1"}, map[string]CanonicalPublicationSet{}); err == nil {
		t.Fatal("empty readbacks must fail closed")
	}
}

func TestCanonicalArtifact_CloneIsDefensive(t *testing.T) {
	art := artifactFixture()
	c := art.clone()
	c.CanonicalParts[0].PartName = "mutated"
	c.AffectedPartitions[0].Root = "mutated"
	if art.CanonicalParts[0].PartName == "mutated" || art.AffectedPartitions[0].Root == "mutated" {
		t.Fatal("clone must not share slice backing with the original")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}

// --- Gated: real REPLACE/DROP PARTITION + signed ack need the absent C2 seam. ---

func TestPublishRetainedWorker_ReplaceFromCanonicalShadow(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion mutation-publication seam lands")
}

func TestPublishRetainedWorker_EmptyPartitionSignedDrop(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion signed DROP PARTITION command lands")
}

func TestPublishRetainedWorker_ReadbackEqualsCanonicalManifestInput(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until real readback + PublicationAck land")
}
