package storageintegrity

import (
	"context"
	"encoding/hex"
	"testing"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
)

// ltHex folds the given elements into an accumulator and returns its raw
// 2048-byte hex form (the shape a manifest's PartRowLtHash carries).
func ltHex(elements ...string) string {
	h := lthash.New()
	for _, e := range elements {
		h.Add([]byte(e))
	}
	return hex.EncodeToString(h.Bytes())
}

func compactionPlanFixture() CompactionPlan {
	// Two input parts whose row-lthash sum equals the single output part built
	// from the same elements (a re-layout that preserves content).
	return CompactionPlan{
		CompactionID:       "cmp-1",
		PublicationSeq:     3,
		TableID:            "net1.events",
		PartitionID:        "p1",
		BaseSafeSnapshotID: "safe-1",
		BasePartitionRoots: []replay.PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Root: "base-p1"}},
		InputParts: []replay.PartManifestEntry{
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartRowLtHash: ltHex("a", "b"), RowCount: 2, Bytes: 20},
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_2_2_0", PartRowLtHash: ltHex("c"), RowCount: 1, Bytes: 10},
		},
	}
}

func compactionOutputFixture() CompactionOutput {
	// One compacted part carrying a,b,c — same lattice sum as the two inputs.
	return CompactionOutput{
		TableID:     "net1.events",
		PartitionID: "p1",
		OutputParts: []replay.PartManifestEntry{
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_2_1", PartRowLtHash: ltHex("a", "b", "c"), RowCount: 3, Bytes: 30},
		},
	}
}

func TestVerifyCompactionEquation_PreservesRowContentSum(t *testing.T) {
	if err := VerifyCompactionEquation(compactionPlanFixture().InputParts, compactionOutputFixture().OutputParts); err != nil {
		t.Fatalf("a content-preserving compaction must satisfy the equation: %v", err)
	}
	// Order-insensitive on both sides.
	in := compactionPlanFixture().InputParts
	in[0], in[1] = in[1], in[0]
	if err := VerifyCompactionEquation(in, compactionOutputFixture().OutputParts); err != nil {
		t.Fatalf("equation must be order-insensitive: %v", err)
	}
	// Dropping a row breaks the equation.
	dropped := compactionOutputFixture().OutputParts
	dropped[0].PartRowLtHash = ltHex("a", "b") // missing c
	if err := VerifyCompactionEquation(compactionPlanFixture().InputParts, dropped); err == nil {
		t.Fatal("a compaction that drops content must fail the equation")
	}
	// Adding a row breaks the equation.
	added := compactionOutputFixture().OutputParts
	added[0].PartRowLtHash = ltHex("a", "b", "c", "d")
	if err := VerifyCompactionEquation(compactionPlanFixture().InputParts, added); err == nil {
		t.Fatal("a compaction that adds content must fail the equation")
	}
	if err := VerifyCompactionEquation(nil, compactionOutputFixture().OutputParts); err == nil {
		t.Fatal("empty input must fail closed")
	}
}

func TestBuildCompactionReplacePlan(t *testing.T) {
	plan := compactionPlanFixture()
	out := compactionOutputFixture()
	rp, err := BuildCompactionReplacePlan(plan, out)
	if err != nil {
		t.Fatalf("valid compaction must build a REPLACE plan: %v", err)
	}
	if rp.TableID != "net1.events" || rp.PartitionID != "p1" || len(rp.CanonicalParts) != 1 {
		t.Fatalf("replace plan wrong: %+v", rp)
	}
	if rp.SQL == "" {
		t.Fatal("replace plan must carry SQL")
	}

	// Equation failure blocks the plan.
	bad := compactionOutputFixture()
	bad.OutputParts[0].PartRowLtHash = ltHex("a") // content mismatch
	if _, err := BuildCompactionReplacePlan(plan, bad); err == nil {
		t.Fatal("an equation-violating output must not build a REPLACE plan")
	}
	// Cross-partition output rejected.
	wrongPart := compactionOutputFixture()
	wrongPart.PartitionID = "p2"
	if _, err := BuildCompactionReplacePlan(plan, wrongPart); err == nil {
		t.Fatal("cross-partition output must be rejected")
	}
}

func TestBuildCompactionManifestID_ContentAddressedAndDeterministic(t *testing.T) {
	plan := compactionPlanFixture()
	out := compactionOutputFixture()
	a, err := BuildCompactionManifestID(plan, out)
	if err != nil {
		t.Fatalf("manifest id: %v", err)
	}
	b, _ := BuildCompactionManifestID(plan, out)
	if a != b {
		t.Fatal("manifest id must be deterministic")
	}
	// A different output mapping changes the id.
	changed := compactionOutputFixture()
	changed.OutputParts[0].PartName = "different_1_1_0"
	c, _ := BuildCompactionManifestID(plan, changed)
	if a == c {
		t.Fatal("manifest id must be content-addressed over the output mapping")
	}
}

func TestDetectActiveSetMismatch_AndQuarantine(t *testing.T) {
	expected := []string{"p1_1_1_0", "p1_2_2_0"}
	// Match: no mismatch, order-insensitive.
	if _, ok := DetectActiveSetMismatch("w-1", "net1.events", "p1", expected, []string{"p1_2_2_0", "p1_1_1_0"}); ok {
		t.Fatal("identical sets must not be a mismatch")
	}
	// A native merge collapsed the two parts into one unexpected part.
	m, ok := DetectActiveSetMismatch("w-1", "net1.events", "p1", expected, []string{"p1_1_2_1"})
	if !ok {
		t.Fatal("a native-merged active set must be a mismatch")
	}
	d := DecideCompactionQuarantine(m)
	if !d.StopServing || !d.ExcludeFromReadSet || !d.RepairRequired {
		t.Fatalf("a mismatch must stop serving + exclude + require repair: %+v", d)
	}
	if d.Mismatch.WorkerID != "w-1" {
		t.Fatal("quarantine decision must carry the mismatch evidence")
	}
}

func TestCompactionPlanAndOutput_Valid(t *testing.T) {
	if err := compactionPlanFixture().Valid(); err != nil {
		t.Fatalf("valid plan: %v", err)
	}
	if err := compactionOutputFixture().Valid(); err != nil {
		t.Fatalf("valid output: %v", err)
	}
	bad := compactionPlanFixture()
	bad.InputParts = nil
	if err := bad.Valid(); err == nil {
		t.Fatal("empty input parts must fail")
	}
	crossPart := compactionPlanFixture()
	crossPart.InputParts[0].PartitionID = "p9"
	if err := crossPart.Valid(); err == nil {
		t.Fatal("cross-partition input must fail")
	}
}

// --- fake gated C4 ports ---

type fakeCompactionDriver struct{ out CompactionOutput }

func (f fakeCompactionDriver) ExecuteControlledCompaction(context.Context, string, CompactionPlan) (CompactionOutput, error) {
	return f.out, nil
}

type fakeCompactionPublisher struct{}

func (fakeCompactionPublisher) PublishCompaction(context.Context, string, ReplacePartitionPlan, []replay.PartitionCommitment) (PublicationAck, error) {
	return PublicationAck{}, nil
}

func TestNewCompactionWorker_RequiresPorts(t *testing.T) {
	if _, err := NewCompactionWorker(nil, fakeCompactionPublisher{}); err == nil {
		t.Fatal("nil compactor must fail")
	}
	if _, err := NewCompactionWorker(fakeCompactionDriver{}, nil); err == nil {
		t.Fatal("nil publisher must fail")
	}
	if _, err := NewCompactionWorker(fakeCompactionDriver{}, fakeCompactionPublisher{}); err != nil {
		t.Fatalf("well-formed wiring must succeed: %v", err)
	}
}

func TestCompactionWorker_RunCompaction_FailsClosedWhenC4Absent(t *testing.T) {
	w, _ := NewCompactionWorker(fakeCompactionDriver{out: compactionOutputFixture()}, fakeCompactionPublisher{})
	if _, _, err := w.RunCompaction(context.Background(), "w-1", compactionPlanFixture()); err == nil {
		t.Fatal("RunCompaction must fail closed while the C4 seam is absent")
	}
}

// --- Gated: the real shadow build + signed REPLACE publication need the absent C4 seam. ---

func TestCompactionWorker_BuildsShadowVerifiesEquationAndPublishes(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion C4 controlled-compaction publication seam lands")
}
