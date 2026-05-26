package checkpoint

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_MissingFile_ReturnsZero(t *testing.T) {
	cp, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("missing file should be ok: %v", err)
	}
	if !cp.LastModificationTime.IsZero() {
		t.Errorf("expected zero time, got %v", cp.LastModificationTime)
	}
}

func TestSaveLoad_Roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	want := Checkpoint{LastModificationTime: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)}
	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !got.LastModificationTime.Equal(want.LastModificationTime) {
		t.Errorf("got %v want %v", got.LastModificationTime, want.LastModificationTime)
	}
}

func TestSave_AtomicReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	first := Checkpoint{LastModificationTime: time.Unix(1000, 0).UTC()}
	if err := Save(path, first); err != nil {
		t.Fatal(err)
	}
	second := Checkpoint{LastModificationTime: time.Unix(2000, 0).UTC()}
	if err := Save(path, second); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastModificationTime.Unix() != 2000 {
		t.Errorf("expected second value, got %v", got.LastModificationTime)
	}
}
