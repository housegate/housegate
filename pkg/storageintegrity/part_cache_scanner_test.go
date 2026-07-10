package storageintegrity

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"housegate/housegate/pkg/replay"
)

// --- fakes -----------------------------------------------------------------

type fakePartInspector struct {
	parts map[string][]PartDescriptor // key: db.table
	calls int
}

func (f *fakePartInspector) ActiveParts(_ context.Context, database, table string, _ []string) ([]PartDescriptor, error) {
	f.calls++
	return f.parts[database+"."+table], nil
}

type fakeNamedPartReader struct {
	// byName maps a physical part name to the entry it folds to.
	byName map[string]replay.PartManifestEntry
	// calls records each ReadNamedParts invocation's requested names.
	calls [][]string
	err   error
	// skipUnknown returns an empty result (rather than erroring) for a name not
	// in byName, so a test can exercise the "live part disappeared from scan"
	// fail-closed branch.
	skipUnknown bool
}

func (f *fakeNamedPartReader) ReadNamedParts(_ context.Context, _ string, partNames []string) ([]replay.PartManifestEntry, error) {
	f.calls = append(f.calls, append([]string(nil), partNames...))
	if f.err != nil {
		return nil, f.err
	}
	out := make([]replay.PartManifestEntry, 0, len(partNames))
	for _, n := range partNames {
		e, ok := f.byName[n]
		if !ok {
			if f.skipUnknown {
				continue
			}
			return nil, fmt.Errorf("fake: unknown part %q", n)
		}
		out = append(out, e)
	}
	return out, nil
}

type fakeSchemaColumnReader struct{ hash string }

func (f fakeSchemaColumnReader) SchemaHash(_ context.Context, _, _, _ string) (string, error) {
	return f.hash, nil
}

func descr(part, phys string, rows uint64) PartDescriptor {
	return PartDescriptor{
		Database: "hg_unsafe", Table: "events", TableID: "hg_unsafe.events",
		PartitionID: "p1", PartName: part, PartPhysHash: phys, Rows: rows, Bytes: rows * 40,
	}
}

func entry(part string, rows uint64, seed string) replay.PartManifestEntry {
	return replay.PartManifestEntry{
		TableID: "hg_unsafe.events", PartitionID: "p1", PartName: part,
		RowCount: rows, PartRowLtHash: rawAccumHex(seed),
	}
}

// --- tests -----------------------------------------------------------------

func TestCachingPartScannerMissThenHit(t *testing.T) {
	ctx := context.Background()
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3), descr("all_2_2_0", "phys-b", 2)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{
		"all_1_1_0": entry("all_1_1_0", 3, "a"),
		"all_2_2_0": entry("all_2_2_0", 2, "b"),
	}}
	cache := NewInMemoryPartLtHashCache(0)
	scanner := CachingPartScanner{Inspector: insp, Cache: cache, Scanner: reader, Schema: fakeSchemaColumnReader{hash: "schema-1"}, Source: "byte_side_scan"}

	// First call: all miss → one scan of both parts, backfills cache.
	got1, err := scanner.ScanParts(ctx, "hg_unsafe", "events", []string{"p1"})
	if err != nil {
		t.Fatalf("first ScanParts: %v", err)
	}
	if len(reader.calls) != 1 || len(reader.calls[0]) != 2 {
		t.Fatalf("first call should scan both parts once, got calls=%v", reader.calls)
	}
	assertEntries(t, got1, map[string]string{"all_1_1_0": rawAccumHex("a"), "all_2_2_0": rawAccumHex("b")})

	// Second call: both hit → NO further scan; byte-identical result.
	got2, err := scanner.ScanParts(ctx, "hg_unsafe", "events", []string{"p1"})
	if err != nil {
		t.Fatalf("second ScanParts: %v", err)
	}
	if len(reader.calls) != 1 {
		t.Fatalf("second call must not scan any rows, got calls=%v", reader.calls)
	}
	if !equalEntries(got1, got2) {
		t.Fatalf("cache-hit result diverged from miss result:\n miss=%+v\n hit=%+v", got1, got2)
	}
}

func TestCachingPartScannerPartialHit(t *testing.T) {
	ctx := context.Background()
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{
		"all_1_1_0": entry("all_1_1_0", 3, "a"),
		"all_9_9_0": entry("all_9_9_0", 5, "z"),
	}}
	cache := NewInMemoryPartLtHashCache(0)
	scanner := CachingPartScanner{Inspector: insp, Cache: cache, Scanner: reader, Schema: fakeSchemaColumnReader{hash: "schema-1"}}

	// Warm the cache with part a.
	if _, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil); err != nil {
		t.Fatalf("warm: %v", err)
	}
	// Now a NEW part appears (a still cached). Only the new part must be scanned.
	insp.parts["hg_unsafe.events"] = []PartDescriptor{descr("all_1_1_0", "phys-a", 3), descr("all_9_9_0", "phys-z", 5)}
	reader.calls = nil
	got, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil)
	if err != nil {
		t.Fatalf("partial: %v", err)
	}
	if len(reader.calls) != 1 || len(reader.calls[0]) != 1 || reader.calls[0][0] != "all_9_9_0" {
		t.Fatalf("only the new part should be scanned, got calls=%v", reader.calls)
	}
	assertEntries(t, got, map[string]string{"all_1_1_0": rawAccumHex("a"), "all_9_9_0": rawAccumHex("z")})
}

func TestCachingPartScannerEmptyPhysBypassesCache(t *testing.T) {
	ctx := context.Background()
	// Part with empty phys hash must always be scanned (guard #5) and never cached.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{"all_1_1_0": entry("all_1_1_0", 3, "a")}}
	cache := NewInMemoryPartLtHashCache(0)
	scanner := CachingPartScanner{Inspector: insp, Cache: cache, Scanner: reader}

	for i := 0; i < 2; i++ {
		if _, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil); err != nil {
			t.Fatalf("scan %d: %v", i, err)
		}
	}
	// Both calls must scan (no caching for empty phys).
	if len(reader.calls) != 2 {
		t.Fatalf("empty-phys part must be re-scanned every call, got calls=%v", reader.calls)
	}
}

func TestCachingPartScannerRowCountMismatchFailsClosed(t *testing.T) {
	ctx := context.Background()
	// The fold reports a different row count than system.parts.rows → fail closed
	// (guard #4 anti-cancellation).
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{
		"all_1_1_0": entry("all_1_1_0", 2, "a"), // fold says 2, system.parts says 3
	}}
	scanner := CachingPartScanner{Inspector: insp, Cache: NewInMemoryPartLtHashCache(0), Scanner: reader}
	if _, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil); err == nil {
		t.Fatalf("expected fail-closed on row-count mismatch")
	}
}

func TestCachingPartScannerDisappearedPartFailsClosed(t *testing.T) {
	ctx := context.Background()
	// A live part that the scan does not return (merged away mid-scan) must fail
	// closed rather than emit an incomplete active set.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	// The reader returns an empty set for the requested part (skipUnknown), so the
	// scanner sees a live part with neither a cache hit nor a scan result.
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{}, skipUnknown: true}
	scanner := CachingPartScanner{Inspector: insp, Cache: NewInMemoryPartLtHashCache(0), Scanner: reader}
	if _, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil); err == nil {
		t.Fatalf("expected fail-closed when a live part is missing from the scan")
	}
}

func TestCachingPartScannerNoCacheStillScans(t *testing.T) {
	ctx := context.Background()
	// Cache == nil: every part is scanned, result still correct.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{"all_1_1_0": entry("all_1_1_0", 3, "a")}}
	scanner := CachingPartScanner{Inspector: insp, Scanner: reader}
	got, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil)
	if err != nil {
		t.Fatalf("no-cache scan: %v", err)
	}
	assertEntries(t, got, map[string]string{"all_1_1_0": rawAccumHex("a")})
}

func TestCachingPartScannerNoSchemaDisablesCache(t *testing.T) {
	ctx := context.Background()
	// Without a Schema resolver the cache key is incomplete, so every part is
	// re-scanned (correct, just not cached). This documents the contract.
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{"all_1_1_0": entry("all_1_1_0", 3, "a")}}
	scanner := CachingPartScanner{Inspector: insp, Cache: NewInMemoryPartLtHashCache(0), Scanner: reader} // no Schema
	for i := 0; i < 2; i++ {
		if _, err := scanner.ScanParts(ctx, "hg_unsafe", "events", nil); err != nil {
			t.Fatalf("scan %d: %v", i, err)
		}
	}
	if len(reader.calls) != 2 {
		t.Fatalf("no-schema scanner should re-scan every call, got calls=%v", reader.calls)
	}
}

func TestByteSideScannerPrefersFastScanAndFallsBack(t *testing.T) {
	ctx := context.Background()
	insp := &fakePartInspector{parts: map[string][]PartDescriptor{
		"hg_unsafe.events": {descr("all_1_1_0", "phys-a", 3)},
	}}
	reader := &fakeNamedPartReader{byName: map[string]replay.PartManifestEntry{"all_1_1_0": entry("all_1_1_0", 3, "a")}}
	fast := &CachingPartScanner{Inspector: insp, Cache: NewInMemoryPartLtHashCache(0), Scanner: reader}

	// Qualified table → fast path used (inspector consulted).
	scanner := HashingByteSideScanner{FastScan: fast, WorkerID: "w1"}
	res, err := scanner.ScanByteSide(ctx, ByteSideScanTask{ScanID: "s1", UnsafeTable: "`hg_unsafe`.`events`", PartitionIDs: []string{"p1"},
		CandidateParts: []ByteSidePart{{PartitionID: "p1", PartName: "all_1_1_0", PartRowLtHash: rawAccumHex("a"), RowCount: 3}}})
	if err != nil {
		t.Fatalf("fast ScanByteSide: %v", err)
	}
	if insp.calls != 1 || len(res.Parts) != 1 || res.Parts[0].PartName != "all_1_1_0" {
		t.Fatalf("fast path not used as expected: insp.calls=%d parts=%+v", insp.calls, res.Parts)
	}
	if res.PartSetHash == "" {
		t.Fatalf("PartSetHash must still be populated")
	}

	// Non-qualified table with a Hasher fallback → fast path skipped, hasher used.
	insp.calls = 0
	fallbackScanner := HashingByteSideScanner{FastScan: fast, Hasher: &fakeTableHasherForBSS{}, WorkerID: "w1"}
	if _, err := fallbackScanner.ScanByteSide(ctx, ByteSideScanTask{ScanID: "s2", UnsafeTable: "singleword",
		CandidateParts: []ByteSidePart{{PartitionID: "p1", PartName: "hash-scan-p1", PartRowLtHash: rawAccumHex("h"), RowCount: 1}}}); err != nil {
		t.Fatalf("fallback ScanByteSide: %v", err)
	}
	if insp.calls != 0 {
		t.Fatalf("fast path must be skipped for a non-qualified table, insp.calls=%d", insp.calls)
	}
}

type fakeTableHasherForBSS struct{}

func (fakeTableHasherForBSS) HashTable(_ context.Context, _ string, _ []string) (TableHash, error) {
	return TableHash{Parts: []ByteSidePart{{PartitionID: "p1", PartName: "hash-scan-p1", RowCount: 1, PartRowLtHash: rawAccumHex("h")}}}, nil
}

// --- helpers ---------------------------------------------------------------

func assertEntries(t *testing.T, got []replay.PartManifestEntry, wantLtHash map[string]string) {
	t.Helper()
	if len(got) != len(wantLtHash) {
		t.Fatalf("expected %d entries, got %d: %+v", len(wantLtHash), len(got), got)
	}
	for _, e := range got {
		want, ok := wantLtHash[e.PartName]
		if !ok {
			t.Fatalf("unexpected part %q", e.PartName)
		}
		if e.PartRowLtHash != want {
			t.Fatalf("part %q lthash = %s, want %s", e.PartName, e.PartRowLtHash, want)
		}
	}
}

func equalEntries(a, b []replay.PartManifestEntry) bool {
	return reflect.DeepEqual(a, b)
}
