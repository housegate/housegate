package storageintegrity

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSnapshotQueryJournalIsIndependentAndRoundTrips(t *testing.T) {
	dir := t.TempDir()
	journal, err := NewFileSnapshotQueryJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	env := snapshotQueryEnvelopeFixture(t)
	rec := newSnapshotQueryRecordAtIntent(env)
	if err := journal.Save(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	got, found, err := journal.Load(context.Background(), rec.StatementID)
	if err != nil || !found {
		t.Fatalf("load found=%v err=%v", found, err)
	}
	if got.Version != SnapshotQueryJournalVersion || got.Stage != SnapshotQueryStageSubmitIntent || got.Envelope.UserJWS != env.UserJWS {
		t.Fatalf("record = %+v", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || filepath.Ext(entries[0].Name()) != ".json" {
		t.Fatalf("journal entries = %v", entries)
	}
}

func TestFileSnapshotQueryJournalListIgnoresUnpublishedTempFiles(t *testing.T) {
	dir := t.TempDir()
	journal, err := NewFileSnapshotQueryJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	env := snapshotQueryEnvelopeFixture(t)
	rec := newSnapshotQueryRecordAtIntent(env)
	if err := journal.Save(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp-snapshot-query-crash.json"), []byte("not a record"), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := journal.List(context.Background())
	if err != nil {
		t.Fatalf("List must ignore unpublished temp file: %v", err)
	}
	if len(records) != 1 || records[0].StatementID != rec.StatementID {
		t.Fatalf("records = %+v", records)
	}
}
