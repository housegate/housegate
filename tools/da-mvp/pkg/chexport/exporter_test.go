package chexport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// makeShim writes a fake "clickhouse-client" to a temp dir, prepends
// that dir to PATH, and returns a cleanup func. The shim prints the
// args it received as Parquet-y stdout so the test can assert what
// the exporter would have invoked.
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

func TestExportPart_PipesQueryToShim(t *testing.T) {
	cleanup := makeShim(t, `echo "PARQUET_BYTES"`)
	defer cleanup()
	bytes, err := ExportPart(context.Background(), Config{
		Host: "h", Port: 9000, User: "u", Password: "p",
	}, "db", "tbl", "all_1_1_0")
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes) != "PARQUET_BYTES\n" {
		t.Errorf("got %q", bytes)
	}
}

func TestExportPart_NonzeroExit_ReturnsError(t *testing.T) {
	cleanup := makeShim(t, `echo "boom" >&2; exit 1`)
	defer cleanup()
	_, err := ExportPart(context.Background(), Config{Host: "h", Port: 9000}, "db", "tbl", "p")
	if err == nil {
		t.Error("expected exit error")
	}
}
