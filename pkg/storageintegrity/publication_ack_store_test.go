package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

// ackCanonicalFixture is a one-partition REPLACE canonical set plus a matching
// PublicationAck whose readback equals the canonical parts.
func ackCanonicalFixture() (CanonicalPublicationSet, PublicationAck) {
	parts := []replay.PartManifestEntry{
		{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartRowLtHash: "lt-a", RowCount: 3, Bytes: 100},
		{TableID: "net1.events", PartitionID: "p1", PartName: "p1_2_2_0", PartRowLtHash: "lt-b", RowCount: 2, Bytes: 80},
	}
	canonical := CanonicalPublicationSet{
		MutationID:     "m-1",
		PublicationSeq: 2,
		Plans: []PartitionInstallPlan{
			{TableID: "net1.events", PartitionID: "p1", Action: PublicationActionReplacePartition, CanonicalParts: parts},
		},
	}
	ack := PublicationAck{
		ContractVersion:          MutationContractVersion,
		MutationID:               "m-1",
		WorkerID:                 "w-1",
		PublicationSeq:           2,
		BaseSafeSnapshotID:       "safe-1",
		BasePartitionRoots:       []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "base-p1"}},
		PostPartitionCommitments: []PartitionCommitment{{TableID: "net1.events", PartitionID: "p1", Commitment: "post-p1"}},
		PostStateRoot:            "post-root-1",
		LocalSafeSnapshotIDAfter: "safe-2",
		ExactActivePartsReadback: []CandidatePart{
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0", PartRowLtHash: "lt-a", RowCount: 3, Bytes: 100},
			{TableID: "net1.events", PartitionID: "p1", PartName: "p1_2_2_0", PartRowLtHash: "lt-b", RowCount: 2, Bytes: 80},
		},
		Applied: true,
	}
	return canonical, ack
}

func TestMemPublicationAckStore_PutThenGet(t *testing.T) {
	canonical, ack := ackCanonicalFixture()
	store := NewMemPublicationAckStore()
	stored, err := store.Put(context.Background(), ack, canonical)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if stored.MutationID != "m-1" || !stored.Applied {
		t.Fatalf("stored ack wrong: %+v", stored)
	}
	got, ok, err := store.Get(context.Background(), PublicationAckKey{MutationID: "m-1", WorkerID: "w-1", PublicationSeq: 2})
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.PostStateRoot != "post-root-1" {
		t.Fatalf("got ack wrong: %+v", got)
	}
}

func TestMemPublicationAckStore_PutIdempotentSameKey(t *testing.T) {
	canonical, ack := ackCanonicalFixture()
	store := NewMemPublicationAckStore()
	first, err := store.Put(context.Background(), ack, canonical)
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	// A second Put with the same key but a DIVERGENT body must return the first
	// stored ack unchanged and never re-verify/overwrite.
	divergent := ack
	divergent.PostStateRoot = "tampered"
	divergent.ExactActivePartsReadback = nil // would fail verifyReadbackEqualsCanonical if re-verified
	second, err := store.Put(context.Background(), divergent, canonical)
	if err != nil {
		t.Fatalf("idempotent second put must not error: %v", err)
	}
	if second.PostStateRoot != first.PostStateRoot || len(second.ExactActivePartsReadback) != len(first.ExactActivePartsReadback) {
		t.Fatal("duplicate-key Put must return the first stored ack, never the divergent body")
	}
}

func TestMemPublicationAckStore_ReadbackEqualsCanonical_OK(t *testing.T) {
	canonical, ack := ackCanonicalFixture()
	// Shuffle the readback order; verification must be order-insensitive.
	ack.ExactActivePartsReadback = []CandidatePart{ack.ExactActivePartsReadback[1], ack.ExactActivePartsReadback[0]}
	store := NewMemPublicationAckStore()
	if _, err := store.Put(context.Background(), ack, canonical); err != nil {
		t.Fatalf("shuffled-order readback must still match: %v", err)
	}
}

func TestMemPublicationAckStore_ReadbackMismatchRejected(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PublicationAck)
	}{
		{"extra readback part", func(a *PublicationAck) {
			a.ExactActivePartsReadback = append(a.ExactActivePartsReadback, CandidatePart{TableID: "net1.events", PartitionID: "p1", PartName: "extra"})
		}},
		{"missing readback part", func(a *PublicationAck) { a.ExactActivePartsReadback = a.ExactActivePartsReadback[:1] }},
		{"row lthash mismatch", func(a *PublicationAck) { a.ExactActivePartsReadback[0].PartRowLtHash = "wrong" }},
		{"bytes mismatch", func(a *PublicationAck) { a.ExactActivePartsReadback[0].Bytes = 999 }},
		{"wrong publication seq", func(a *PublicationAck) { a.PublicationSeq = 9 }},
		{"wrong mutation id", func(a *PublicationAck) { a.MutationID = "m-other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			canonical, ack := ackCanonicalFixture()
			tc.mutate(&ack)
			store := NewMemPublicationAckStore()
			if _, err := store.Put(context.Background(), ack, canonical); err == nil {
				t.Fatal("readback mismatch must fail closed")
			}
			// Nothing persisted on a failed first Put.
			key := PublicationAckKey{MutationID: ack.MutationID, WorkerID: ack.WorkerID, PublicationSeq: ack.PublicationSeq}
			if _, ok, _ := store.Get(context.Background(), key); ok {
				t.Fatal("a failed Put must persist nothing")
			}
		})
	}
}

func TestMemPublicationAckStore_RejectsBlankKey(t *testing.T) {
	canonical, base := ackCanonicalFixture()
	cases := []struct {
		name   string
		mutate func(*PublicationAck)
	}{
		{"blank mutation", func(a *PublicationAck) { a.MutationID = "" }},
		{"blank worker", func(a *PublicationAck) { a.WorkerID = "" }},
		{"zero seq", func(a *PublicationAck) { a.PublicationSeq = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ack := base
			tc.mutate(&ack)
			store := NewMemPublicationAckStore()
			if _, err := store.Put(context.Background(), ack, canonical); err == nil {
				t.Fatal("blank key must fail closed")
			}
		})
	}
}

func TestMemPublicationAckStore_InvalidAckRejected(t *testing.T) {
	canonical, ack := ackCanonicalFixture()
	ack.BaseSafeSnapshotID = "" // fails PublicationAck.Valid()
	store := NewMemPublicationAckStore()
	if _, err := store.Put(context.Background(), ack, canonical); err == nil {
		t.Fatal("an ack failing Valid() must be rejected before persist")
	}
}

// --- Gated: durable persist via the real driver needs the absent C2 seam. ---

func TestPublicationAck_DurablePersistBeforeSendRealDriver(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the real MutationPublicationDriver ack path lands")
}
