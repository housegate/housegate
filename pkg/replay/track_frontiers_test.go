package replay

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// Frozen cross-implementation vectors of ComputeManifestRoot with and without
// track frontiers (spec 2026-10-10 §7.3): SHA-256 over
// "housegate-replay-mvp-v0:" || "safe-snapshot-manifest" || 0x00 || the
// literal canonical JSON below. The arbiter records manifest roots in
// consensus state and every verifier recomputes them, so do not "fix" an
// expected value without a coordinated profile migration.
const (
	manifestRootWithoutTrackFrontiers = "0x56b5abc3101bb3616ebcc96a0681f60c47e1c79e6e98294c7b2f00b39ce9a5f6"
	manifestRootWithTrackFrontiers    = "0xf9f8ce7f5e5345a2ed3e0b86612bd7cd6117218145b74635b4c374b6e7352c8f"
)

// manifestPreimageWithoutTrackFrontiers is trackVectorManifest's root
// preimage: tables sorted by table_id, nil slices as null, and no
// track_frontiers member at all.
const manifestPreimageWithoutTrackFrontiers = `{"parent_snapshot_id":"0xparent","safe_block_seq":57,"state_root":"0xstate","schema_snapshot_id":"devnet2-si-v2","schema_root":"0xschema","executor_profile_id":"executor-v1","data_root":"0xdata","tables":[{"table_id":"net1.a","schema_hash":"0xha","partition_roots":[{"table_id":"net1.a","partition_id":"p1","root":"0xr1"}],"active_parts":[{"table_id":"net1.a","partition_id":"p1","part_name":"all_1_1_0","part_phys_hash":"0xphys","part_row_lthash":"0xlt","row_count":3,"bytes":512}]},{"table_id":"net1.b","schema_hash":"0xhb","partition_roots":null,"active_parts":null}]}`

// manifestPreimageWithTrackFrontiers is the same preimage with
// trackVectorFrontiers as its last member: indexer 0 and a zero
// applied_promotion_seq are written out.
const manifestPreimageWithTrackFrontiers = `{"parent_snapshot_id":"0xparent","safe_block_seq":57,"state_root":"0xstate","schema_snapshot_id":"devnet2-si-v2","schema_root":"0xschema","executor_profile_id":"executor-v1","data_root":"0xdata","tables":[{"table_id":"net1.a","schema_hash":"0xha","partition_roots":[{"table_id":"net1.a","partition_id":"p1","root":"0xr1"}],"active_parts":[{"table_id":"net1.a","partition_id":"p1","part_name":"all_1_1_0","part_phys_hash":"0xphys","part_row_lthash":"0xlt","row_count":3,"bytes":512}]},{"table_id":"net1.b","schema_hash":"0xhb","partition_roots":null,"active_parts":null}],"track_frontiers":[{"indexer_id":0,"safe_block_seq":57,"applied_promotion_seq":131},{"indexer_id":1,"safe_block_seq":56,"applied_promotion_seq":0}]}`

// trackFrontiersTail is the JSON the frontiers add after a manifest's tables.
const trackFrontiersTail = `,"track_frontiers":[{"indexer_id":0,"safe_block_seq":57,"applied_promotion_seq":131},{"indexer_id":1,"safe_block_seq":56,"applied_promotion_seq":0}]}`

// trackVectorManifest is the fixed manifest of the track-frontier root
// vectors. Its data and state roots are literal strings, so ComputeManifestRoot
// hashes exactly the values below; its tables are listed out of order and the
// preimage holds them sorted.
func trackVectorManifest() SafeSnapshotManifest {
	return SafeSnapshotManifest{
		ParentSnapshotID:  "0xparent",
		SafeBlockSeq:      57,
		StateRoot:         "0xstate",
		SchemaSnapshotID:  "devnet2-si-v2",
		SchemaRoot:        "0xschema",
		ExecutorProfileID: "executor-v1",
		DataRoot:          "0xdata",
		Tables: []TableManifest{
			{TableID: "net1.b", SchemaHash: "0xhb"},
			{
				TableID:        "net1.a",
				SchemaHash:     "0xha",
				PartitionRoots: []PartitionCommitment{{TableID: "net1.a", PartitionID: "p1", Root: "0xr1"}},
				ActiveParts: []PartManifestEntry{{
					TableID: "net1.a", PartitionID: "p1", PartName: "all_1_1_0",
					PartPhysHash: "0xphys", PartRowLtHash: "0xlt", RowCount: 3, Bytes: 512,
				}},
			},
		},
	}
}

// trackVectorFrontiers are the vectors' frontiers: indexer 0 first and a zero
// applied_promotion_seq, so both must serialize.
func trackVectorFrontiers() []TrackFrontier {
	return []TrackFrontier{
		{IndexerID: 0, SafeBlockSeq: 57, AppliedPromotionSeq: 131},
		{IndexerID: 1, SafeBlockSeq: 56, AppliedPromotionSeq: 0},
	}
}

// TestManifestRootWithoutTrackFrontiersIsUnchanged pins spec 2026-10-10 §7.3:
// a manifest without track frontiers keeps its root byte for byte under the
// unchanged safe-snapshot-manifest domain. It covers a nil and an empty list,
// an independent digest over the literal preimage (no track_frontiers member),
// and the manifest roots frozen in testdata/snapshot_query_v1.json, which
// predate track frontiers.
func TestManifestRootWithoutTrackFrontiersIsUnchanged(t *testing.T) {
	for name, frontiers := range map[string][]TrackFrontier{"nil": nil, "empty": {}} {
		m := trackVectorManifest()
		m.TrackFrontiers = frontiers
		got, err := m.ComputeManifestRoot()
		if err != nil {
			t.Fatalf("%s: ComputeManifestRoot: %v", name, err)
		}
		if got != manifestRootWithoutTrackFrontiers {
			t.Fatalf("%s: manifest root = %s, want the frozen %s", name, got, manifestRootWithoutTrackFrontiers)
		}
	}
	if got := independentQueryDigest("safe-snapshot-manifest", manifestPreimageWithoutTrackFrontiers); got != manifestRootWithoutTrackFrontiers {
		t.Fatalf("independent literal digest = %s, want %s", got, manifestRootWithoutTrackFrontiers)
	}
	const (
		fixtureGenesisRoot  = "0x38adcfceae2f13b16ae98cdc72424bf3459ebf2fa38f498487877cfc3f145980"
		fixtureManifestRoot = "0x64a57e1f2ff5ed22b6d994f46c0310f7f0a258b87da39efe711b1b52b4489f02"
	)
	fixtures := queryFixtures(t)
	if len(fixtures) == 0 || fixtures[0].Genesis.ManifestRoot != fixtureGenesisRoot || fixtures[0].Manifest.ManifestRoot != fixtureManifestRoot {
		t.Fatal("testdata/snapshot_query_v1.json no longer carries its frozen manifest roots")
	}
	for _, f := range fixtures {
		for _, m := range []SafeSnapshotManifest{f.Genesis, f.Manifest} {
			if len(m.TrackFrontiers) != 0 {
				t.Fatalf("%s: a pre-stage-2 fixture carries track frontiers", f.Name)
			}
			got, err := m.ComputeManifestRoot()
			if err != nil || got != m.ManifestRoot {
				t.Fatalf("%s: manifest root = %s (err %v), want the frozen %s", f.Name, got, err, m.ManifestRoot)
			}
		}
	}
}

// TestManifestRootWithTrackFrontiersFrozenVector pins the track-aware root
// (spec 2026-10-10 §7.3): track_frontiers is the preimage's last member, after
// tables, under the unchanged domain, and every entry writes all three fields,
// indexer 0 and zero values included. Frontiers move only the manifest root:
// the data and state roots a verifier compares stay those of the same tables
// without frontiers.
func TestManifestRootWithTrackFrontiersFrozenVector(t *testing.T) {
	m := trackVectorManifest()
	m.TrackFrontiers = trackVectorFrontiers()
	got, err := m.ComputeManifestRoot()
	if err != nil {
		t.Fatalf("ComputeManifestRoot: %v", err)
	}
	if got != manifestRootWithTrackFrontiers {
		t.Fatalf("frozen vector mismatch: got %s want %s", got, manifestRootWithTrackFrontiers)
	}
	if got := independentQueryDigest("safe-snapshot-manifest", manifestPreimageWithTrackFrontiers); got != manifestRootWithTrackFrontiers {
		t.Fatalf("independent literal digest = %s, want %s", got, manifestRootWithTrackFrontiers)
	}
	if manifestPreimageWithTrackFrontiers != strings.TrimSuffix(manifestPreimageWithoutTrackFrontiers, "}")+trackFrontiersTail {
		t.Fatal("the frontier preimage must be the frontier-free preimage with track_frontiers appended as its last member")
	}
	plain, err := trackVectorManifest().Seal()
	if err != nil {
		t.Fatalf("Seal without frontiers: %v", err)
	}
	tracked := trackVectorManifest()
	tracked.TrackFrontiers = trackVectorFrontiers()
	sealed, err := tracked.Seal()
	if err != nil {
		t.Fatalf("Seal with frontiers: %v", err)
	}
	if sealed.DataRoot != plain.DataRoot || sealed.StateRoot != plain.StateRoot {
		t.Fatalf("data/state roots moved with track frontiers: %s/%s vs %s/%s", sealed.DataRoot, sealed.StateRoot, plain.DataRoot, plain.StateRoot)
	}
	if sealed.ManifestRoot == plain.ManifestRoot {
		t.Fatal("track frontiers must enter the manifest root")
	}
	if err := sealed.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for i, mutate := range []func(*TrackFrontier){
		func(f *TrackFrontier) { f.IndexerID = 2 },
		func(f *TrackFrontier) { f.SafeBlockSeq++ },
		func(f *TrackFrontier) { f.AppliedPromotionSeq++ },
	} {
		changed := trackVectorManifest()
		changed.TrackFrontiers = trackVectorFrontiers()
		mutate(&changed.TrackFrontiers[1])
		root, err := changed.ComputeManifestRoot()
		if err != nil || root == manifestRootWithTrackFrontiers {
			t.Fatalf("mutation %d of the last entry left the manifest root at %s (err %v)", i, root, err)
		}
	}
}

// TestManifestJSONCarriesTrackFrontiersLast pins the manifest's own JSON form,
// which the arbiter's snapshot document and published-snapshot replies embed:
// without frontiers there is no track_frontiers member, so every earlier
// encoding is byte-identical; with them the member comes last and every entry
// writes all three fields.
func TestManifestJSONCarriesTrackFrontiersLast(t *testing.T) {
	plain, err := json.Marshal(trackVectorManifest())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "track_frontiers") {
		t.Fatalf("a manifest without frontiers encodes a track_frontiers member: %s", plain)
	}
	m := trackVectorManifest()
	m.TrackFrontiers = trackVectorFrontiers()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.TrimSuffix(string(plain), "}") + trackFrontiersTail; string(b) != want {
		t.Fatalf("manifest JSON = %s, want %s", b, want)
	}
	var back SafeSnapshotManifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(back.TrackFrontiers, m.TrackFrontiers) {
		t.Fatalf("decoded frontiers = %+v, want %+v", back.TrackFrontiers, m.TrackFrontiers)
	}
}

// TestValidateRefusesUnsortedOrDuplicateTrackFrontiers pins the order rule
// (spec 2026-10-10 §7.3): Validate requires track_frontiers strictly ascending
// by indexer_id. Seal does not sort them, so a sealed out-of-order list keeps
// its order and is refused rather than silently repaired; indexer 0 is a valid
// first entry.
func TestValidateRefusesUnsortedOrDuplicateTrackFrontiers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		frontiers []TrackFrontier
		wantErr   bool
	}{
		{"sorted from indexer 0", []TrackFrontier{{IndexerID: 0, SafeBlockSeq: 57}, {IndexerID: 1, SafeBlockSeq: 56}, {IndexerID: 7}}, false},
		{"single entry", []TrackFrontier{{IndexerID: 3, SafeBlockSeq: 9, AppliedPromotionSeq: 4}}, false},
		{"unsorted", []TrackFrontier{{IndexerID: 1, SafeBlockSeq: 56}, {IndexerID: 0, SafeBlockSeq: 57}}, true},
		{"duplicate", []TrackFrontier{{IndexerID: 0, SafeBlockSeq: 57}, {IndexerID: 0, SafeBlockSeq: 58}}, true},
		{"identical duplicate", []TrackFrontier{{IndexerID: 2}, {IndexerID: 2}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := manifestFixture()
			m.TrackFrontiers = append([]TrackFrontier(nil), tc.frontiers...)
			sealed, err := m.Seal()
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if !slices.Equal(sealed.TrackFrontiers, tc.frontiers) {
				t.Fatalf("Seal reordered the frontiers: %+v, want the caller's %+v", sealed.TrackFrontiers, tc.frontiers)
			}
			err = sealed.Validate()
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || err.Error() != "track_frontiers must be sorted by indexer_id and unique" {
				t.Fatalf("Validate = %v, want the track_frontiers order refusal", err)
			}
		})
	}
}

// TestSealCopiesTrackFrontiers pins that normalized() copies the frontiers:
// a sealed manifest shares no backing array with its caller's slice.
func TestSealCopiesTrackFrontiers(t *testing.T) {
	m := manifestFixture()
	m.TrackFrontiers = trackVectorFrontiers()
	sealed, err := m.Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	m.TrackFrontiers[0].SafeBlockSeq = 999
	if sealed.TrackFrontiers[0].SafeBlockSeq != 57 {
		t.Fatal("Seal aliased its caller's track frontiers")
	}
	if err := sealed.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// trackTransitionFixture is executorTransitionFixture with the given frontiers
// on the previous and the next snapshot, both resealed and re-linked.
func trackTransitionFixture(t *testing.T, prevFrontiers, nextFrontiers []TrackFrontier) (SafeSnapshotManifest, SafeSnapshotManifest, ExecutorProfileTransition) {
	t.Helper()
	prev, next, tr := executorTransitionFixture(t)
	prev.TrackFrontiers = prevFrontiers
	reseal(t, &prev)
	next.TrackFrontiers = nextFrontiers
	next.ParentSnapshotID = prev.SnapshotID
	reseal(t, &next)
	tr.PrevSnapshotID, tr.PrevStateRoot = prev.SnapshotID, prev.StateRoot
	tr.NextSnapshotID, tr.NextStateRoot, tr.NextManifestRoot = next.SnapshotID, next.StateRoot, next.ManifestRoot
	return prev, next, tr
}

// TestExecutorProfileTransitionCarriesTrackFrontiersUnchanged pins that an
// executor-profile transition, which changes no data, keeps every track
// frontier of its predecessor (Plan S2-A contract §2).
func TestExecutorProfileTransitionCarriesTrackFrontiersUnchanged(t *testing.T) {
	prev, next, tr := trackTransitionFixture(t, trackVectorFrontiers(), trackVectorFrontiers())
	if err := ValidateExecutorProfileTransition(prev, next, tr); err != nil {
		t.Fatalf("equal frontiers refused: %v", err)
	}
	advanced := trackVectorFrontiers()
	advanced[1].SafeBlockSeq++
	for name, frontiers := range map[string][]TrackFrontier{
		"advanced": advanced,
		"dropped":  nil,
		"added":    append(trackVectorFrontiers(), TrackFrontier{IndexerID: 2}),
	} {
		prev, next, tr := trackTransitionFixture(t, trackVectorFrontiers(), frontiers)
		err := ValidateExecutorProfileTransition(prev, next, tr)
		if err == nil || err.Error() != "track frontiers changed across the executor transition" {
			t.Fatalf("%s: err = %v, want the track-frontier refusal", name, err)
		}
	}
}
