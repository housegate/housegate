package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

// TestClickHouseSafeAuditorFastScanActiveSetMatch proves the fast path serves
// the manifest active-set check from the cache (live part names, cached
// lthash), that the vote matches when the active set agrees, and — critically —
// that the vote StateRoot still comes from the full Hasher scan (the vote
// semantics are unchanged; only the pre-check is accelerated).
func TestClickHouseSafeAuditorFastScanActiveSetMatch(t *testing.T) {
	root := rawAccumHex("safe")
	manifest := sealedOpsTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "safe_p1",
		PartPhysHash:  "phys-1",
		PartRowLtHash: root,
		RowCount:      2,
		Bytes:         128,
	}})
	// Fast scan returns the live part matching the manifest.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		// Runtime ClickHousePartInspector defaults this to the physical table id
		// (hg_safe.events). Audit must override it with task.TableID before
		// comparing against the logical manifest table id (tenant.events).
		"hg_safe.events": {{Database: "hg_safe", Table: "events", TableID: "hg_safe.events", PartitionID: "202607", PartName: "safe_p1", PartPhysHash: "phys-1", Rows: 2, Bytes: 128}},
	}}
	fold := map[string]replay.PartManifestEntry{
		"safe_p1": {TableID: "tenant.events", PartitionID: "202607", PartName: "safe_p1", RowCount: 2, PartRowLtHash: root},
	}
	fast := &CachingPartScanner{
		Inspector: insp,
		Cache:     NewInMemoryPartLtHashCache(0),
		Scanner:   &fakeNamedPartReader{byName: fold},
		Schema:    fakeSchemaColumnReader{hash: "schema-1"},
	}
	// The hasher returns the manifest state root AND records that it was called
	// (proving the vote root is still a full scan, not the cache).
	hasher := &fakeTableHasher{root: manifest.StateRoot}
	auditor := ClickHouseSafeAuditor{Hasher: hasher, FastScan: fast, WorkerID: "auditor-a"}

	vote, err := auditor.AuditSafe(context.Background(), SafeAuditTask{
		AuditID:      "audit-1",
		SnapshotID:   manifest.SnapshotID,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		StateRoot:    manifest.StateRoot,
		Manifest:     manifest,
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("AuditSafe: %v", err)
	}
	if !vote.Match || !vote.ActivePartsMatch {
		t.Fatalf("vote should match a consistent active set: %+v", vote)
	}
	if vote.StateRoot != manifest.StateRoot {
		t.Fatalf("vote StateRoot = %q, want the hasher's full-scan root %q", vote.StateRoot, manifest.StateRoot)
	}
	// The vote StateRoot must come from the full hasher scan (fast path does not
	// replace the vote).
	if hasher.calls == 0 || hasher.table != "`hg_safe`.`events`" {
		t.Fatalf("audit vote must still full-scan the safe table via the hasher (calls=%d table=%q)", hasher.calls, hasher.table)
	}
	// The fast active-set path was used (inspector consulted).
	if insp.calls == 0 {
		t.Fatalf("fast active-set path was not used")
	}
	if len(vote.ActiveParts) != 1 || vote.ActiveParts[0].PartName != "safe_p1" {
		t.Fatalf("vote active parts = %+v (must carry the live part name)", vote.ActiveParts)
	}
}

func TestClickHouseSafeAuditorFastScanActiveSetMismatchFailsVote(t *testing.T) {
	manifest := sealedOpsTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "p_good",
		PartRowLtHash: rawAccumHex("good"),
		RowCount:      2,
	}})
	// Fast scan returns a DIFFERENT part → active-set mismatch → no "pass" vote.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_safe.events": {{Database: "hg_safe", Table: "events", TableID: "tenant.events", PartitionID: "202607", PartName: "p_evil", PartPhysHash: "phys-evil", Rows: 2}},
	}}
	fold := map[string]replay.PartManifestEntry{
		"p_evil": {TableID: "tenant.events", PartitionID: "202607", PartName: "p_evil", RowCount: 2, PartRowLtHash: rawAccumHex("evil")},
	}
	fast := &CachingPartScanner{
		Inspector: insp,
		Cache:     NewInMemoryPartLtHashCache(0),
		Scanner:   &fakeNamedPartReader{byName: fold},
		Schema:    fakeSchemaColumnReader{hash: "schema-1"},
	}
	auditor := ClickHouseSafeAuditor{Hasher: &fakeTableHasher{root: manifest.StateRoot}, FastScan: fast, WorkerID: "auditor-a"}

	vote, err := auditor.AuditSafe(context.Background(), SafeAuditTask{
		AuditID:      "audit-1",
		SnapshotID:   manifest.SnapshotID,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		StateRoot:    manifest.StateRoot,
		Manifest:     manifest,
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("AuditSafe: %v", err)
	}
	if vote.Match || vote.ActivePartsMatch || !strings.Contains(vote.Error, "active parts do not match manifest") {
		t.Fatalf("vote = %+v, want active-set mismatch (no pass vote)", vote)
	}
}
