package storageintegrity

import (
	"context"
	"fmt"
	"testing"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

func cacheEntryFixture(phys string) replay.PartManifestEntry {
	return replay.PartManifestEntry{
		TableID: "net1.events", PartitionID: "p1", PartName: "p1_1_1_0",
		PartPhysHash: phys, PartRowLtHash: "lh", RowCount: 5, Bytes: 50,
	}
}

func schemaFixture() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: "net1.events",
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}},
	}
}

func keyFixture() PartCacheKey {
	return PartCacheKey{
		RowHashVersion: RowHashProfileVersion, TableID: "net1.events",
		SchemaHash: "sh", PartPhysHash: "phys-1", RowCount: 5, Bytes: 50,
	}
}

func TestPartCacheKey_Valid(t *testing.T) {
	if err := keyFixture().Valid(); err != nil {
		t.Fatalf("full key must validate: %v", err)
	}
	for _, drop := range []func(*PartCacheKey){
		func(k *PartCacheKey) { k.RowHashVersion = "" },
		func(k *PartCacheKey) { k.TableID = "" },
		func(k *PartCacheKey) { k.SchemaHash = "" },
		func(k *PartCacheKey) { k.PartPhysHash = "" },
	} {
		k := keyFixture()
		drop(&k)
		if err := k.Valid(); err == nil {
			t.Fatal("a key missing a binding field must be invalid")
		}
	}
}

func TestPartLtHashCache_GetPutAndAllFieldsBindHit(t *testing.T) {
	c := NewPartLtHashCache(100)
	key := keyFixture()
	res := PartLtHashResult{PartName: "p1_1_1_0", RowLtHash: "lh", RowCount: 5}
	if _, ok := c.Get(key); ok {
		t.Fatal("empty cache must miss")
	}
	if err := c.Put(key, res); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok := c.Get(key)
	if !ok || got != res {
		t.Fatalf("exact key must hit with the stored result: %+v ok=%v", got, ok)
	}
	// Any single field changing forces a miss.
	for _, mutate := range []func(*PartCacheKey){
		func(k *PartCacheKey) { k.RowHashVersion = "other-profile" },
		func(k *PartCacheKey) { k.TableID = "net1.other" },
		func(k *PartCacheKey) { k.SchemaHash = "different" },
		func(k *PartCacheKey) { k.PartPhysHash = "phys-2" },
		func(k *PartCacheKey) { k.RowCount = 6 },
		func(k *PartCacheKey) { k.Bytes = 51 },
	} {
		miss := keyFixture()
		mutate(&miss)
		if _, ok := c.Get(miss); ok {
			t.Fatal("a key with any field changed must miss")
		}
	}
	// An invalid key never stores.
	bad := keyFixture()
	bad.PartPhysHash = ""
	if err := c.Put(bad, res); err == nil {
		t.Fatal("an invalid (unanchored) key must be rejected by Put")
	}
}

func TestPartLtHashCache_LRUEviction(t *testing.T) {
	c := NewPartLtHashCache(2)
	mk := func(phys string) PartCacheKey { k := keyFixture(); k.PartPhysHash = phys; return k }
	_ = c.Put(mk("a"), PartLtHashResult{PartName: "a"})
	_ = c.Put(mk("b"), PartLtHashResult{PartName: "b"})
	// Touch "a" so "b" becomes the LRU.
	if _, ok := c.Get(mk("a")); !ok {
		t.Fatal("a must be present")
	}
	_ = c.Put(mk("c"), PartLtHashResult{PartName: "c"})
	if c.Len() != 2 {
		t.Fatalf("cache must stay bounded at 2, got %d", c.Len())
	}
	if _, ok := c.Get(mk("b")); ok {
		t.Fatal("LRU entry b must have been evicted")
	}
	if _, ok := c.Get(mk("a")); !ok {
		t.Fatal("recently-used a must survive")
	}
	if _, ok := c.Get(mk("c")); !ok {
		t.Fatal("newest c must be present")
	}
}

// fakePartScanner counts real scans and returns a fixed per-part result.
type fakePartScanner struct {
	calls   int
	results map[string]PartLtHashResult
	err     error
}

func (f *fakePartScanner) ScanPart(_ context.Context, entry replay.PartManifestEntry, _ payloadexec.TableSchema) (PartLtHashResult, error) {
	f.calls++
	if f.err != nil {
		return PartLtHashResult{}, f.err
	}
	if r, ok := f.results[entry.PartName]; ok {
		return r, nil
	}
	return PartLtHashResult{PartName: entry.PartName, RowLtHash: "scanned-" + entry.PartName, RowCount: entry.RowCount}, nil
}

func TestCachingPartScanner_HitReturnsExactlyWhatScanWouldAndSkipsRescan(t *testing.T) {
	inner := &fakePartScanner{}
	cache := NewPartLtHashCache(100)
	insp := NewPartInspector("net1")
	cs, err := NewCachingPartScanner(inner, cache, insp)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	entry := cacheEntryFixture("phys-1")

	// First call: miss => real scan.
	first, err := cs.ScanPart(context.Background(), entry, schemaFixture())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("first scan must hit the inner scanner, calls=%d", inner.calls)
	}
	// Second call for the SAME part: hit => no rescan, identical result.
	second, err := cs.ScanPart(context.Background(), entry, schemaFixture())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("cached hit must not rescan, calls=%d", inner.calls)
	}
	if first != second {
		t.Fatal("a cache hit must return exactly what the real scan produced")
	}
	hits, misses := cs.Stats()
	if hits != 1 || misses != 1 {
		t.Fatalf("hits=%d misses=%d, want 1/1", hits, misses)
	}
}

func TestCachingPartScanner_PartWithoutPhysHashAlwaysRescans(t *testing.T) {
	inner := &fakePartScanner{}
	cs, _ := NewCachingPartScanner(inner, NewPartLtHashCache(100), NewPartInspector("net1"))
	entry := cacheEntryFixture("") // no physical hash => un-keyable
	if _, err := cs.ScanPart(context.Background(), entry, schemaFixture()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if _, err := cs.ScanPart(context.Background(), entry, schemaFixture()); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if inner.calls != 2 {
		t.Fatalf("an un-keyable part must always be really scanned, calls=%d", inner.calls)
	}
}

func TestCachingPartScanner_NilCacheIsPassThrough(t *testing.T) {
	inner := &fakePartScanner{}
	cs, _ := NewCachingPartScanner(inner, nil, NewPartInspector("net1"))
	entry := cacheEntryFixture("phys-1")
	_, _ = cs.ScanPart(context.Background(), entry, schemaFixture())
	_, _ = cs.ScanPart(context.Background(), entry, schemaFixture())
	if inner.calls != 2 {
		t.Fatalf("a nil cache must pass through to the inner scanner every time, calls=%d", inner.calls)
	}
}

func TestCachingPartScanner_InconsistentHitDiscardedAndRescanned(t *testing.T) {
	// A phys-hash collision: the cache holds a result whose part name differs from
	// the observed entry. The caching scanner must discard the hit and rescan.
	inner := &fakePartScanner{results: map[string]PartLtHashResult{
		"p1_1_1_0": {PartName: "p1_1_1_0", RowLtHash: "correct", RowCount: 5},
	}}
	cache := NewPartLtHashCache(100)
	insp := NewPartInspector("net1")
	cs, _ := NewCachingPartScanner(inner, cache, insp)
	entry := cacheEntryFixture("phys-collide")

	// Poison the cache: same key, but a result for a DIFFERENT part name.
	key, _ := insp.KeyFor(entry, schemaFixture())
	_ = cache.Put(key, PartLtHashResult{PartName: "other_part", RowLtHash: "stale", RowCount: 5})

	got, err := cs.ScanPart(context.Background(), entry, schemaFixture())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("an inconsistent hit must be discarded and rescanned, calls=%d", inner.calls)
	}
	if got.RowLtHash != "correct" {
		t.Fatalf("must return the real scan result, not the poisoned cache: %+v", got)
	}
}

func TestCachingPartScanner_RequiresInnerAndInspector(t *testing.T) {
	if _, err := NewCachingPartScanner(nil, NewPartLtHashCache(1), NewPartInspector("n")); err == nil {
		t.Fatal("nil inner scanner must fail")
	}
	if _, err := NewCachingPartScanner(&fakePartScanner{}, NewPartLtHashCache(1), nil); err == nil {
		t.Fatal("nil inspector must fail")
	}
}

func TestPartInspector_KeyForRejectsNoPhysHash(t *testing.T) {
	insp := NewPartInspector("net1")
	if _, err := insp.KeyFor(cacheEntryFixture(""), schemaFixture()); err == nil {
		t.Fatal("a part with no physical hash must not be keyable")
	}
	k, err := insp.KeyFor(cacheEntryFixture("phys-1"), schemaFixture())
	if err != nil {
		t.Fatalf("keyable part: %v", err)
	}
	if k.RowHashVersion != RowHashProfileVersion || k.PartPhysHash != "phys-1" {
		t.Fatalf("key binding wrong: %+v", k)
	}
	// Scanner-scan equivalence sanity: the inner scanner's error propagates.
	inner := &fakePartScanner{err: fmt.Errorf("scan failed")}
	cs, _ := NewCachingPartScanner(inner, NewPartLtHashCache(1), insp)
	if _, err := cs.ScanPart(context.Background(), cacheEntryFixture("phys-1"), schemaFixture()); err == nil {
		t.Fatal("a real-scan error must propagate")
	}
}
