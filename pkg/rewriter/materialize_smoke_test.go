package rewriter

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Runs the real native engine end-to-end. Skips unless POLYGLOT_SQL_FFI_PATH
// points at libpolyglot_sql_ffi.{so,dylib}.
func TestMaterialize_NativeSmoke(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; skipping native materialize smoke test")
	}
	m, err := NewMaterializer(Options{Engine: EngineNative, NativeLibraryPath: lib}, 16, "")
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	defer m.Close()
	out, err := m.Materialize(context.Background(), "SELECT now()")
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if !out.Changed || strings.Contains(strings.ToLower(out.SQL), "now(") {
		t.Fatalf("now() should have been materialized to a constant, got %q", out.SQL)
	}
}
