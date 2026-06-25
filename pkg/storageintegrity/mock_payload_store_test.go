package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestMockPayloadStorePersistsPayloadByDigestRef(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("from,to,amount\nalice,bob,10\n")

	store, err := NewMockPayloadStore(root)
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	got, err := store.PutPayload(ctx, PutPayloadRequest{
		TableID:     "Transfer/0xT",
		StatementID: "stmt 1",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("PutPayload: %v", err)
	}
	if got.Hash != replay.DigestBytes(payload) {
		t.Fatalf("hash = %s, want %s", got.Hash, replay.DigestBytes(payload))
	}
	if got.Length != uint64(len(payload)) {
		t.Fatalf("length = %d, want %d", got.Length, len(payload))
	}
	if !strings.HasPrefix(got.Ref, "mockda://") {
		t.Fatalf("ref = %q, want mockda:// prefix", got.Ref)
	}

	reopened, err := NewMockPayloadStore(root)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	loaded, err := reopened.GetPayload(ctx, got.Ref)
	if err != nil {
		t.Fatalf("GetPayload: %v", err)
	}
	if string(loaded) != string(payload) {
		t.Fatalf("loaded payload = %q, want %q", loaded, payload)
	}
}

func TestMockPayloadStoreRejectsBadRefs(t *testing.T) {
	ctx := context.Background()
	store, err := NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	if _, err := store.GetPayload(ctx, "https://example.invalid/payload"); err == nil {
		t.Fatal("expected non-mock ref to be rejected")
	}
}
