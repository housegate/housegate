package config

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/sitable"
)

// TestSitablePhysicalNamesMatchConfig pins the D2 names pkg/sitable repeats
// because a leaf package cannot import pkg/config, and pins by literal the
// stage-3 member databases (spec 2026-10-14 §8.1, §14), which only pkg/sitable
// names: config keeps its three constants.
func TestSitablePhysicalNamesMatchConfig(t *testing.T) {
	want := []string{StorageIntegritySafeDatabase, StorageIntegrityUnsafeDatabase, StorageIntegrityPromoteDatabase,
		"hg_member_unsafe", "hg_replica", "hg_rstage", "hg_xfer"}
	if got := sitable.ReservedDatabases(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sitable.ReservedDatabases() = %v drifted from %v", got, want)
	}
	if sitable.SafeDatabase != StorageIntegritySafeDatabase || sitable.UnsafeDatabase != StorageIntegrityUnsafeDatabase || sitable.PromoteDatabase != StorageIntegrityPromoteDatabase {
		t.Fatalf("sitable databases %q/%q/%q drifted from config", sitable.SafeDatabase, sitable.UnsafeDatabase, sitable.PromoteDatabase)
	}
	if sitable.MemberUnsafeDatabase != "hg_member_unsafe" || sitable.ReplicaDatabase != "hg_replica" ||
		sitable.ReplicaStageDatabase != "hg_rstage" || sitable.TransferDatabase != "hg_xfer" {
		t.Fatalf("sitable stage-3 databases %q/%q/%q/%q drifted", sitable.MemberUnsafeDatabase, sitable.ReplicaDatabase, sitable.ReplicaStageDatabase, sitable.TransferDatabase)
	}
	for _, id := range []string{"db1.t", "a_b.c_d"} {
		if sitable.PhysicalTable(id) != StorageIntegrityPhysicalTable(id) {
			t.Fatalf("PhysicalTable(%q) = %q, config = %q", id, sitable.PhysicalTable(id), StorageIntegrityPhysicalTable(id))
		}
	}
}
