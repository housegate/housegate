package replay

import "testing"

func TestExecutorProfileTransitionAcceptsCompatibleChild(t *testing.T) {
	prev, next, tr := executorTransitionFixture(t)
	if err := ValidateExecutorProfileTransition(prev, next, tr); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorProfileTransitionRefusesMutations(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*SafeSnapshotManifest, *SafeSnapshotManifest, *ExecutorProfileTransition)
	}{
		{"same-executor", func(_, next *SafeSnapshotManifest, _ *ExecutorProfileTransition) {
			next.ExecutorProfileID = "executor-e1"
			reseal(t, next)
		}},
		{"old-id", func(_, _ *SafeSnapshotManifest, tr *ExecutorProfileTransition) {
			tr.OldExecutorProfileID = "executor-other"
		}},
		{"new-id", func(_, _ *SafeSnapshotManifest, tr *ExecutorProfileTransition) {
			tr.NewExecutorProfileID = "executor-other"
		}},
		{"schema-root", func(_, next *SafeSnapshotManifest, _ *ExecutorProfileTransition) {
			next.SchemaRoot = "0xother-schema"
			reseal(t, next)
		}},
		{"data-root", func(_, next *SafeSnapshotManifest, _ *ExecutorProfileTransition) {
			next.Tables[0].PartitionRoots[0].Root = "0xother-data"
			reseal(t, next)
		}},
		{"omitted-table", func(_, next *SafeSnapshotManifest, _ *ExecutorProfileTransition) {
			next.Tables = next.Tables[:1]
			reseal(t, next)
		}},
		{"parent", func(_, next *SafeSnapshotManifest, _ *ExecutorProfileTransition) {
			next.ParentSnapshotID = "not-prev"
			reseal(t, next)
		}},
		{"target-manifest", func(_, _ *SafeSnapshotManifest, tr *ExecutorProfileTransition) {
			tr.NextManifestRoot = DigestString("wrong-manifest")
		}},
		{"unsupported-pair", func(_, next *SafeSnapshotManifest, tr *ExecutorProfileTransition) {
			tr.Activation.ExecutorProfileID = "executor-e1"
			_ = next
		}},
		{"query-disabled", func(_, _ *SafeSnapshotManifest, tr *ExecutorProfileTransition) {
			tr.Activation.Enabled = false
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prev, next, tr := executorTransitionFixture(t)
			tc.mut(&prev, &next, &tr)
			if err := ValidateExecutorProfileTransition(prev, next, tr); err == nil {
				t.Fatal("mutation accepted")
			}
		})
	}
}

func executorTransitionFixture(t *testing.T) (SafeSnapshotManifest, SafeSnapshotManifest, ExecutorProfileTransition) {
	t.Helper()
	prev, err := SafeSnapshotManifest{
		SafeBlockSeq:      4,
		SchemaSnapshotID:  "schema-1",
		SchemaRoot:        DigestString("schema"),
		ExecutorProfileID: "executor-e1",
		Tables: []TableManifest{
			{
				TableID:    "alpha",
				SchemaHash: DigestString("alpha-schema"),
				PartitionRoots: []PartitionCommitment{{
					TableID: "alpha", PartitionID: "all", Root: DigestString("alpha-part"),
				}},
				ActiveParts: []PartManifestEntry{{
					TableID: "alpha", PartitionID: "all", PartName: "all_1_1_0",
					PartPhysHash: DigestString("alpha-phys"), PartRowLtHash: DigestString("alpha-row"),
					RowCount: 3, Bytes: 30,
				}},
			},
			{
				TableID:    "beta",
				SchemaHash: DigestString("beta-schema"),
				PartitionRoots: []PartitionCommitment{{
					TableID: "beta", PartitionID: "all", Root: DigestString("beta-part"),
				}},
				ActiveParts: []PartManifestEntry{{
					TableID: "beta", PartitionID: "all", PartName: "all_1_1_0",
					PartPhysHash: DigestString("beta-phys"), PartRowLtHash: DigestString("beta-row"),
					RowCount: 5, Bytes: 50,
				}},
			},
		},
	}.Seal()
	if err != nil {
		t.Fatal(err)
	}
	next := prev
	next.ExecutorProfileID = "executor-e2"
	next.ParentSnapshotID = prev.SnapshotID
	next.SafeBlockSeq = prev.SafeBlockSeq + 1
	reseal(t, &next)
	tr := ExecutorProfileTransition{
		NetworkID:            "net-1",
		KeeperShardID:        1,
		PrevSnapshotID:       prev.SnapshotID,
		PrevStateRoot:        prev.StateRoot,
		OldExecutorProfileID: prev.ExecutorProfileID,
		NewExecutorProfileID: next.ExecutorProfileID,
		SchemaSnapshotID:     next.SchemaSnapshotID,
		SchemaRoot:           next.SchemaRoot,
		DataRoot:             next.DataRoot,
		NextSnapshotID:       next.SnapshotID,
		NextStateRoot:        next.StateRoot,
		NextManifestRoot:     next.ManifestRoot,
		Activation: ActiveQueryPolicy{
			ActivationID:       "act-1",
			NetworkID:          "net-1",
			KeeperShardID:      1,
			ActivationBlockSeq: next.SafeBlockSeq,
			ExecutorProfileID:  next.ExecutorProfileID,
			QueryProfileID:     "query-q1",
			Enabled:            true,
		},
	}
	return prev, next, tr
}

func reseal(t *testing.T, m *SafeSnapshotManifest) {
	t.Helper()
	m.SnapshotID = ""
	m.DataRoot = ""
	m.StateRoot = ""
	m.ManifestRoot = ""
	sealed, err := m.Seal()
	if err != nil {
		t.Fatal(err)
	}
	*m = sealed
}
