package chimport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func makeShim(t *testing.T, body string) func() {
	t.Helper()
	dir := t.TempDir()
	shim := filepath.Join(dir, "clickhouse-client")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	prev := os.Getenv("PATH")
	os.Setenv("PATH", dir+":"+prev)
	return func() { os.Setenv("PATH", prev) }
}

func TestInsert_PipesStdinToShim(t *testing.T) {
	cleanup := makeShim(t, `cat > /tmp/damvp-shim-stdin`)
	defer cleanup()
	err := Insert(context.Background(), Config{Host: "h", Port: 9000}, "db", "t", "Parquet", []byte("PAYLOAD"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("/tmp/damvp-shim-stdin")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "PAYLOAD" {
		t.Errorf("got %q", got)
	}
}
