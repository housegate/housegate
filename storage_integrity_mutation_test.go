package housegate

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
	sicore "housegate/housegate/pkg/storageintegrity"
)

// requireCompanionMutationConsensus skips a root-package test while the exported
// companion C2 gate is off. It reads the same single honest gate the
// pkg/storageintegrity package exposes; it declares no new constant.
func requireCompanionMutationConsensus(t *testing.T) {
	t.Helper()
	if !sicore.CompanionMutationConsensusAvailable {
		t.Skip("companion C2 mutation-consensus seam absent: SubmitMutation / " +
			"SubmitMutationClaim / PublishMutationSafeCut are not exposed by the Sentio " +
			"arbiter/arbiter-proto topology; end-to-end P2 mutation runtime is blocked " +
			"until the companion seam lands (see sicore.CompanionMutationConsensusAvailable)")
	}
}

// --- fake mutation ports (test doubles, never a companion-protocol stand-in) ---

type fakeMutationSubmitter struct{}

func (fakeMutationSubmitter) SubmitMutation(context.Context, sicore.MutationStatementEnvelope) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{}, nil
}

type fakeMutationClaimSubmitter struct{}

func (fakeMutationClaimSubmitter) SubmitMutationClaim(context.Context, sicore.MutationClaim) (sicore.ClaimOutcome, error) {
	return sicore.ClaimOutcome{}, nil
}

type fakeMutationPublicationAcker struct{}

func (fakeMutationPublicationAcker) SubmitPublicationAck(context.Context, sicore.PublicationAck) error {
	return nil
}

type fakeMutationSafeCutPublisher struct{}

func (fakeMutationSafeCutPublisher) PublishMutationSafeCut(context.Context, sicore.PublishMutationSafeCutInput) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{}, nil
}

type fakeMutationPublicationDriver struct{}

func (fakeMutationPublicationDriver) PublishRetainedWorker(context.Context, string, sicore.CanonicalPublicationSet, []replay.PartitionCommitment) (sicore.PublicationAck, error) {
	return sicore.PublicationAck{}, nil
}

func testSafeReadGate(t *testing.T) *sicore.SafeReadGate {
	t.Helper()
	cut := sicore.NewSafeCutView("m", 10, map[string]uint64{"w-1": 10}, map[string]bool{"w-1": true}, 3, nil)
	g, err := sicore.NewSafeReadGate(cut)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	return &g
}

func newTestMutationRuntime(t *testing.T, workers []string) (*StorageIntegrityMutation, error) {
	t.Helper()
	return NewStorageIntegrityMutation(
		fakeMutationSubmitter{}, fakeMutationClaimSubmitter{}, fakeMutationPublicationAcker{},
		fakeMutationSafeCutPublisher{}, fakeMutationPublicationDriver{}, testSafeReadGate(t), workers,
	)
}

func TestNewStorageIntegrityMutation_HoldsPortsAndFloor(t *testing.T) {
	m, err := newTestMutationRuntime(t, []string{"w-1", "w-2", "w-3"})
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	if m.ServingAvailabilityFloor() != sicore.MutationServingAvailabilityFloor {
		t.Fatalf("floor = %d, want %d", m.ServingAvailabilityFloor(), sicore.MutationServingAvailabilityFloor)
	}
	got := m.WorkerIDs()
	if len(got) != 3 {
		t.Fatalf("worker ids = %v", got)
	}
	got[0] = "mutated"
	if m.WorkerIDs()[0] == "mutated" {
		t.Fatal("WorkerIDs must return a defensive copy")
	}
}

func TestNewStorageIntegrityMutation_RejectsNilPorts(t *testing.T) {
	gate := testSafeReadGate(t)
	workers := []string{"w-1", "w-2", "w-3"}
	cases := []struct {
		name string
		call func() (*StorageIntegrityMutation, error)
	}{
		{"nil submitter", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(nil, fakeMutationClaimSubmitter{}, fakeMutationPublicationAcker{}, fakeMutationSafeCutPublisher{}, fakeMutationPublicationDriver{}, gate, workers)
		}},
		{"nil claims", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(fakeMutationSubmitter{}, nil, fakeMutationPublicationAcker{}, fakeMutationSafeCutPublisher{}, fakeMutationPublicationDriver{}, gate, workers)
		}},
		{"nil acker", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(fakeMutationSubmitter{}, fakeMutationClaimSubmitter{}, nil, fakeMutationSafeCutPublisher{}, fakeMutationPublicationDriver{}, gate, workers)
		}},
		{"nil safeCut", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(fakeMutationSubmitter{}, fakeMutationClaimSubmitter{}, fakeMutationPublicationAcker{}, nil, fakeMutationPublicationDriver{}, gate, workers)
		}},
		{"nil publisher", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(fakeMutationSubmitter{}, fakeMutationClaimSubmitter{}, fakeMutationPublicationAcker{}, fakeMutationSafeCutPublisher{}, nil, gate, workers)
		}},
		{"nil read gate", func() (*StorageIntegrityMutation, error) {
			return NewStorageIntegrityMutation(fakeMutationSubmitter{}, fakeMutationClaimSubmitter{}, fakeMutationPublicationAcker{}, fakeMutationSafeCutPublisher{}, fakeMutationPublicationDriver{}, nil, workers)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.call(); err == nil {
				t.Fatal("nil port/gate must be a wiring error")
			}
		})
	}
}

func TestNewStorageIntegrityMutation_RequiresThreeDistinctWorkers(t *testing.T) {
	if _, err := newTestMutationRuntime(t, []string{"w-1", "w-2"}); err == nil {
		t.Fatal("fewer than 3 workers must be rejected")
	}
	if _, err := newTestMutationRuntime(t, []string{"w-1", "w-2", "w-2"}); err == nil {
		t.Fatal("duplicate worker id must be rejected")
	}
	if _, err := newTestMutationRuntime(t, []string{"w-1", "w-2", "w-3"}); err != nil {
		t.Fatalf("3 distinct workers must succeed: %v", err)
	}
}

func TestStorageIntegrityMutation_ValidatePublicationInputs_UsesPureBuilder(t *testing.T) {
	m, _ := newTestMutationRuntime(t, []string{"w-1", "w-2", "w-3"})
	good := sicore.CanonicalArtifact{
		MutationID: "m-1", PublicationSeq: 1, ArtifactCommitment: "c", ArtifactSource: "s",
		SchemaSnapshotID: "schema", ExecutorProfileID: "profile", PrevSafeSnapshotID: "safe",
		AffectedPartitions:       []replay.PartitionCommitment{{TableID: "t", PartitionID: "p1", Root: "b"}},
		PostPartitionCommitments: []replay.PartitionCommitment{{TableID: "t", PartitionID: "p1", Root: "post"}},
		CanonicalParts:           []replay.PartManifestEntry{{TableID: "t", PartitionID: "p1", PartName: "p1_1_1_0"}},
	}
	if err := m.validatePublicationInputs(good); err != nil {
		t.Fatalf("well-formed artifact must pass: %v", err)
	}
	bad := good
	bad.CanonicalParts = []replay.PartManifestEntry{{TableID: "t", PartitionID: "p9", PartName: "x"}} // outside affected
	if err := m.validatePublicationInputs(bad); err == nil {
		t.Fatal("a malformed artifact must fail closed via the pure builder")
	}
}

func TestStorageIntegrityMutation_RunMutation_FailsClosedWhenC2Absent(t *testing.T) {
	m, _ := newTestMutationRuntime(t, []string{"w-1", "w-2", "w-3"})
	err := m.RunMutation(context.Background(), sicore.MutationStatementEnvelope{})
	if err == nil {
		t.Fatal("RunMutation must fail closed while the C2 seam is absent")
	}
}

// --- Gated: the full 3-worker end-to-end flow needs the absent C2 seam. ---

func TestMutationRuntimeEndToEnd_ThreeWorkerIngressToAck3(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the full companion mutation consensus path lands")
}
