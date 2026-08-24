package rewriter

import (
	"testing"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// TestReadModeSettingKeyMatchesStorageIntegrity keeps the two declarations of
// SQL_x_read_mode in lockstep. pkg/storageintegrity cannot import pkg/rewriter
// (that would pull grpc + the FFI engine into a pure-data package), so the
// equality is asserted from the package that already sits above both.
func TestReadModeSettingKeyMatchesStorageIntegrity(t *testing.T) {
	if ReadModeSettingKey != sicore.ReadModeSettingKey {
		t.Fatalf("rewriter.ReadModeSettingKey = %q, storageintegrity.ReadModeSettingKey = %q", ReadModeSettingKey, sicore.ReadModeSettingKey)
	}
}
