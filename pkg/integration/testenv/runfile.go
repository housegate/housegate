package testenv

import (
	"os"
	"path/filepath"
	"testing"
)

// runfileBinary resolves a bazel-built binary from the test's runfiles by
// its rules_go output path (<pkg>/<name>_/<name>). Used to mount test-only
// proxy/sidecar binaries (kpx, imesh) into containers. Skips when not run
// under bazel.
func runfileBinary(t *testing.T, rel string) string {
	t.Helper()
	srcdir := os.Getenv("TEST_SRCDIR")
	ws := os.Getenv("TEST_WORKSPACE")
	if srcdir == "" || ws == "" {
		t.Skip("binary requires bazel runfiles (TEST_SRCDIR/TEST_WORKSPACE); run via `bazel test`")
	}
	p := filepath.Join(srcdir, ws, rel)
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("binary not found at %s: %v\n(add the go_binary to the integration_test `data`)", p, err)
	}
	return p
}
