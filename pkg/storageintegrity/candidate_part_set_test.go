package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
)

// sumHex returns the additive lattice sum of the given raw-accumulator seeds as
// a single accumulator hex, so a test can declare one candidate part that
// content-equals several scanned parts.
func sumHex(t *testing.T, seeds ...string) string {
	t.Helper()
	hashes := make([]string, 0, len(seeds))
	for _, s := range seeds {
		hashes = append(hashes, rawAccumHex(s))
	}
	root, err := sumPartRowLtHashes(hashes)
	if err != nil {
		t.Fatalf("sumPartRowLtHashes: %v", err)
	}
	return root
}

// TestEnforceCandidatePartSetContentAddressedRepacking is the core HG-P0-02
// property: a source declares its candidate as a single logical part carrying
// the additive part_row_lthash of all its rows, while ClickHouse physically
// packed the same rows into two differently named parts. The content-addressed
// binding must accept this because the additive partition root is invariant to
// part packing (spec §8.1).
func TestEnforceCandidatePartSetContentAddressedRepacking(t *testing.T) {
	scanned := []ByteSidePart{
		{PartitionID: "202607", PartName: "202607_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
		{PartitionID: "202607", PartName: "202607_2_2_0", PartRowLtHash: rawAccumHex("b"), RowCount: 1},
	}
	// Source declares one logical part (no physical name) summing a+b.
	candidates := []ByteSidePart{
		{PartitionID: "202607", PartRowLtHash: sumHex(t, "a", "b"), RowCount: 2},
	}
	if err := enforceCandidatePartSet(scanned, candidates); err != nil {
		t.Fatalf("content-addressed repacking should match: %v", err)
	}
}

// TestEnforceCandidatePartSetContentRejectsExtraPart proves the exact-set guard:
// a partition that physically contains a part the source never committed (a
// second statement's rows leaking into the same partition) must be rejected.
func TestEnforceCandidatePartSetContentRejectsExtraPart(t *testing.T) {
	scanned := []ByteSidePart{
		{PartitionID: "202607", PartName: "202607_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
		{PartitionID: "202607", PartName: "202607_2_2_0", PartRowLtHash: rawAccumHex("intruder"), RowCount: 1},
	}
	candidates := []ByteSidePart{
		{PartitionID: "202607", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
	}
	err := enforceCandidatePartSet(scanned, candidates)
	if err == nil || !strings.Contains(err.Error(), "additive root differs") {
		t.Fatalf("expected additive-root mismatch, got %v", err)
	}
}

// TestEnforceCandidatePartSetContentRejectsRowCountSkew guards the
// anti-cancellation belt: LtHash sums can collide under 2^16 identical rows, so
// a matching additive root with a different row count must still be rejected.
func TestEnforceCandidatePartSetContentRejectsRowCountSkew(t *testing.T) {
	scanned := []ByteSidePart{
		{PartitionID: "202607", PartName: "202607_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 5},
	}
	candidates := []ByteSidePart{
		{PartitionID: "202607", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
	}
	err := enforceCandidatePartSet(scanned, candidates)
	if err == nil || !strings.Contains(err.Error(), "row count") {
		t.Fatalf("expected row-count mismatch, got %v", err)
	}
}

// TestEnforceCandidatePartSetContentRejectsMissingPartition proves a source
// that declares parts in a partition the scan did not surface fails closed.
func TestEnforceCandidatePartSetContentRejectsMissingPartition(t *testing.T) {
	scanned := []ByteSidePart{
		{PartitionID: "202607", PartName: "202607_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
	}
	candidates := []ByteSidePart{
		{PartitionID: "202607", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
		{PartitionID: "202608", PartRowLtHash: rawAccumHex("b"), RowCount: 1},
	}
	err := enforceCandidatePartSet(scanned, candidates)
	if err == nil || !strings.Contains(err.Error(), "partition") {
		t.Fatalf("expected partition mismatch, got %v", err)
	}
}

// TestEnforceCandidatePartSetNameKeyedStillStrict confirms the name-keyed path
// (used when candidates carry physical names) still enforces one-to-one name
// matching.
func TestEnforceCandidatePartSetNameKeyedStillStrict(t *testing.T) {
	scanned := []ByteSidePart{
		{PartitionID: "p", PartName: "real_1_1_0", PartRowLtHash: "0xa", RowCount: 1},
	}
	candidates := []ByteSidePart{
		{PartitionID: "p", PartName: "different_name", PartRowLtHash: "0xa", RowCount: 1},
	}
	if err := enforceCandidatePartSet(scanned, candidates); err == nil {
		t.Fatalf("name-keyed binding must reject a candidate whose name is not active")
	}
}

// TestByteSideScannerRejectsInsertWithoutCandidates is the Verifier-side
// fail-closed: an INSERT byte-side scan with no declared candidate parts is
// unverifiable and must be refused before any attestation (HG-P0-02).
func TestByteSideScannerRejectsInsertWithoutCandidates(t *testing.T) {
	active := &fakeActivePartReader{parts: []replay.PartManifestEntry{
		{PartitionID: "202607", PartName: "202607_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 1},
	}}
	scanner := HashingByteSideScanner{ActiveParts: active, WorkerID: "verifier-1"}
	_, err := scanner.ScanByteSide(context.Background(), ByteSideScanTask{
		ScanID: "s1", StatementID: "stmt-1", TableID: "tenant.events",
		UnsafeTable: "`hg_unsafe`.`events`", PartitionIDs: []string{"202607"},
		// no CandidateParts, Kind defaults to insert
	})
	if err == nil || !strings.Contains(err.Error(), "no candidate parts") {
		t.Fatalf("expected empty-candidate rejection, got %v", err)
	}
}

// TestNativeSourceClaimRootReconcilesWithByteSideCandidate ties the source claim
// to the byte side: the candidate part_row_lthash the plugin declares
// (claim.PartRowLtHash) is exactly the additive accumulator that, summed onto
// the genesis base, yields the source claim root. A byte-side scan recomputing
// that same accumulator therefore reconciles with the source claim.
func TestNativeSourceClaimRootReconcilesWithByteSideCandidate(t *testing.T) {
	genesis, err := NativePayloadGenesisSnapshot("tenant.events", []lthash.Column{{Name: "id", Type: "UInt64"}})
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	partRowLtHash := rawAccumHex("candidate-rows")
	root, err := NativeSourceClaimRoot(genesis, "tenant.events", partRowLtHash)
	if err != nil {
		t.Fatalf("NativeSourceClaimRoot: %v", err)
	}
	if root == "" {
		t.Fatalf("source claim root must be non-empty")
	}
	// The byte-side candidate the plugin declares carries exactly partRowLtHash;
	// a scan producing the same accumulator content-matches it.
	candidate := []ByteSidePart{{PartitionID: NativeAllPartitionID, PartRowLtHash: partRowLtHash, RowCount: 2}}
	scanned := []ByteSidePart{{PartitionID: NativeAllPartitionID, PartName: "all_1_1_0", PartRowLtHash: partRowLtHash, RowCount: 2}}
	if err := enforceCandidatePartSet(scanned, candidate); err != nil {
		t.Fatalf("declared candidate must reconcile with byte-side scan: %v", err)
	}
}
