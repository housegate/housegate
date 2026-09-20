package storageintegrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// rewriteSnapshotQueryRecordFile mutates the single published record in dir as
// a foreign writer would: a corrupted stage, or a record an older binary wrote
// under an earlier version of this shape.
func rewriteSnapshotQueryRecordFile(t *testing.T, dir string, mutate func(map[string]any)) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var recordPath string
	for _, entry := range entries {
		if !entry.IsDir() && isSnapshotQueryRecordFile(entry.Name()) {
			recordPath = filepath.Join(dir, entry.Name())
		}
	}
	if recordPath == "" {
		t.Fatalf("no published record in %s", dir)
	}
	b, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatal(err)
	}
	mutate(obj)
	b, err = json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A record this binary cannot decode must be refused where it is read, naming
// what it could not accept. Reaching a recovery switch's default branch
// instead would abort every other pending statement in the same List.
func TestSnapshotQueryJournalRefusesUnknownStageAndOldVersion(t *testing.T) {
	if SnapshotQueryJournalVersion != 2 {
		t.Fatalf("SnapshotQueryJournalVersion = %d, want 2: the stage set carrying ForwardAuthorized is version 2", SnapshotQueryJournalVersion)
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name:   "unknown stage",
			mutate: func(obj map[string]any) { obj["stage"] = "Bogus" },
			want:   `stage "Bogus"`,
		},
		{
			name:   "older record version",
			mutate: func(obj map[string]any) { obj["version"] = 1 },
			want:   fmt.Sprintf("record version 1 unsupported by this binary (supports %d)", SnapshotQueryJournalVersion),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			journal, err := NewFileSnapshotQueryJournal(dir)
			if err != nil {
				t.Fatal(err)
			}
			rec := newSnapshotQueryRecordAtIntent(snapshotQueryEnvelopeFixture(t))
			if err := journal.Save(context.Background(), rec); err != nil {
				t.Fatal(err)
			}
			rewriteSnapshotQueryRecordFile(t, dir, tc.mutate)
			if _, _, err := journal.Load(context.Background(), rec.StatementID); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want one naming %s", err, tc.want)
			}
			if _, err := journal.List(context.Background()); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("List error = %v, want one naming %s", err, tc.want)
			}
		})
	}
}
