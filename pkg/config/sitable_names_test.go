package config

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/sitable"
)

// TestSitablePhysicalNamesMatchConfig pins the D2 names pkg/sitable repeats
// because a leaf package cannot import pkg/config.
func TestSitablePhysicalNamesMatchConfig(t *testing.T) {
	want := []string{StorageIntegritySafeDatabase, StorageIntegrityUnsafeDatabase, StorageIntegrityPromoteDatabase}
	if got := sitable.ReservedDatabases(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sitable.ReservedDatabases() = %v drifted from config %v", got, want)
	}
	if sitable.SafeDatabase != StorageIntegritySafeDatabase || sitable.UnsafeDatabase != StorageIntegrityUnsafeDatabase || sitable.PromoteDatabase != StorageIntegrityPromoteDatabase {
		t.Fatalf("sitable databases %q/%q/%q drifted from config", sitable.SafeDatabase, sitable.UnsafeDatabase, sitable.PromoteDatabase)
	}
	for _, id := range []string{"db1.t", "a_b.c_d"} {
		if sitable.PhysicalTable(id) != StorageIntegrityPhysicalTable(id) {
			t.Fatalf("PhysicalTable(%q) = %q, config = %q", id, sitable.PhysicalTable(id), StorageIntegrityPhysicalTable(id))
		}
	}
}
