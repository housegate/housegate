package payloadexec

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestMemSnapshotStoreRoundTrips(t *testing.T) {
	var _ replay.SnapshotStore = NewMemSnapshotStore()

	store := NewMemSnapshotStore()
	e := testExecutor()
	snap := genesis(t, e)
	store.Put(snap)

	got, err := store.GetSafeSnapshot(context.Background(), snap.SnapshotID)
	if err != nil {
		t.Fatalf("GetSafeSnapshot: %v", err)
	}
	if got.StateRoot != snap.StateRoot {
		t.Fatalf("state root = %s, want %s", got.StateRoot, snap.StateRoot)
	}
	if _, err := store.GetSafeSnapshot(context.Background(), "missing"); err == nil {
		t.Fatal("expected error for missing snapshot")
	}
}

func TestMemPayloadStoreRoundTrips(t *testing.T) {
	var _ replay.PayloadStore = NewMemPayloadStore()

	store := NewMemPayloadStore()
	store.Put("ref-1", []byte("hello"))

	got, err := store.GetPayload(context.Background(), "ref-1")
	if err != nil {
		t.Fatalf("GetPayload: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("payload = %q, want hello", got)
	}
	// returned slice must be a copy: mutating it must not corrupt the store
	got[0] = 'x'
	again, err := store.GetPayload(context.Background(), "ref-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "hello" {
		t.Fatalf("store payload was mutated through returned slice: %q", again)
	}
	if _, err := store.GetPayload(context.Background(), "missing"); err == nil {
		t.Fatal("expected error for missing payload")
	}
}
