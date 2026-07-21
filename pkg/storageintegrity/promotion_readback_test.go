package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

func readbackMappingFixture() PromotionReadbackMapping {
	return PromotionReadbackMapping{
		PostStateRoot: "post-root-1",
		Parts: []replay.PartManifestEntry{
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartPhysHash: "ph1", PartRowLtHash: "lh1", RowCount: 5, Bytes: 50, StorageRefs: []string{"s3://a", "s3://b"}},
			{TableID: "net1.events", PartitionID: "p2", PartName: "p2_1_1_0", PartPhysHash: "ph2", PartRowLtHash: "lh2", RowCount: 7, Bytes: 70},
		},
	}
}

func TestAssertReadbackFastPathEquivalent_Identical(t *testing.T) {
	strict := readbackMappingFixture()
	fast := readbackMappingFixture()
	if err := AssertReadbackFastPathEquivalent(strict, fast); err != nil {
		t.Fatalf("identical mappings must be equivalent: %v", err)
	}
	// Order-insensitive on parts and on storage refs.
	fast.Parts[0], fast.Parts[1] = fast.Parts[1], fast.Parts[0]
	fast.Parts[1].StorageRefs = []string{"s3://b", "s3://a"} // reordered
	if err := AssertReadbackFastPathEquivalent(strict, fast); err != nil {
		t.Fatalf("order must not matter: %v", err)
	}
}

func TestAssertReadbackFastPathEquivalent_Divergence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PromotionReadbackMapping)
	}{
		{"different post root", func(m *PromotionReadbackMapping) { m.PostStateRoot = "other-root" }},
		{"blank post root", func(m *PromotionReadbackMapping) { m.PostStateRoot = "" }},
		{"missing part", func(m *PromotionReadbackMapping) { m.Parts = m.Parts[:1] }},
		{"extra part", func(m *PromotionReadbackMapping) {
			m.Parts = append(m.Parts, replay.PartManifestEntry{TableID: "net1.events", PartitionID: "p9", PartName: "x", PartPhysHash: "z", PartRowLtHash: "z"})
		}},
		{"phys hash drift", func(m *PromotionReadbackMapping) { m.Parts[0].PartPhysHash = "tampered" }},
		{"row-lthash drift", func(m *PromotionReadbackMapping) { m.Parts[0].PartRowLtHash = "tampered" }},
		{"row count drift", func(m *PromotionReadbackMapping) { m.Parts[0].RowCount = 999 }},
		{"bytes drift", func(m *PromotionReadbackMapping) { m.Parts[0].Bytes = 999 }},
		{"storage refs drift", func(m *PromotionReadbackMapping) { m.Parts[0].StorageRefs = []string{"s3://different"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			strict := readbackMappingFixture()
			fast := readbackMappingFixture()
			tc.mutate(&fast)
			if err := AssertReadbackFastPathEquivalent(strict, fast); err == nil {
				t.Fatalf("%s must not be equivalent", tc.name)
			}
		})
	}
}

// --- fake gated ports ---

type fakeSNodeReadback struct{ m PromotionReadbackMapping }

func (f fakeSNodeReadback) ExactActivePartsReadback(context.Context, string, payloadexec.TableSchema, []CandidatePart) (PromotionReadbackMapping, error) {
	return f.m, nil
}

func TestNewPromotionReadback_RequiresPorts(t *testing.T) {
	if _, err := NewPromotionReadback(nil, &fakePartScanner{}); err == nil {
		t.Fatal("nil SNode port must fail")
	}
	if _, err := NewPromotionReadback(fakeSNodeReadback{}, nil); err == nil {
		t.Fatal("nil strict scanner must fail")
	}
	if _, err := NewPromotionReadback(fakeSNodeReadback{}, &fakePartScanner{}); err != nil {
		t.Fatalf("well-formed wiring must succeed: %v", err)
	}
}

func TestPromotionReadback_FailsClosedWhenC0Absent(t *testing.T) {
	r, _ := NewPromotionReadback(fakeSNodeReadback{m: readbackMappingFixture()}, &fakePartScanner{})
	if _, err := r.Readback(context.Background(), "net1.events"); err == nil {
		t.Fatal("Readback must fail closed while the C0 SNode readback seam is absent")
	}
}

// --- Gated: the real SNode fast-path readback needs the absent C0 seam. ---

func TestPromotionReadback_FastPathMatchesStrictAgainstRealSNode(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion C0 SNode exact-parts readback seam lands")
}
