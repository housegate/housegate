package storageintegrity

import (
	"context"
	"testing"
	"time"

	core "housegate/housegate/pkg/storageintegrity"
)

func TestFileOutboxPersistsAndReloadsPendingInsert(t *testing.T) {
	store, err := NewFileInsertOutbox(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileInsertOutbox: %v", err)
	}
	rec := core.InsertRecord{
		TableID:     "tenant.events",
		StatementID: "stmt-1",
		UnsafeTable: "`hg_unsafe`.`events`",
		SafeTable:   "`hg_safe`.`events`",
		ReceivedAt:  time.Unix(10, 0).UTC(),
	}
	if err := store.Put(context.Background(), rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	reloaded, err := NewFileInsertOutbox(store.Dir())
	if err != nil {
		t.Fatalf("reload NewFileInsertOutbox: %v", err)
	}
	pending, err := reloaded.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(pending) != 1 || pending[0].StatementID != "stmt-1" || pending[0].TableID != "tenant.events" {
		t.Fatalf("pending = %+v, want stmt-1", pending)
	}
}

func TestFileOutboxAckIsIdempotent(t *testing.T) {
	store, err := NewFileInsertOutbox(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileInsertOutbox: %v", err)
	}
	rec := core.InsertRecord{TableID: "tenant.events", StatementID: "stmt-1"}
	if err := store.Put(context.Background(), rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Ack(context.Background(), rec.StatementID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := store.Ack(context.Background(), rec.StatementID); err != nil {
		t.Fatalf("second Ack: %v", err)
	}
	pending, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after ack = %+v, want none", pending)
	}
}
