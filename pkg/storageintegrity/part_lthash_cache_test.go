package storageintegrity

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"housegate/housegate/pkg/lthash"
)

// rawAccumHex returns a distinct, valid raw 2048-byte accumulator hex for the
// given seed so tests can store cacheable values without a full row fold.
func rawAccumHex(seed string) string {
	acc := lthash.New()
	acc.Add([]byte("part-lthash-cache-test\x00" + seed))
	return lthashAccumulatorHex(acc)
}

func testKey(phys string) PartLtHashKey {
	return PartLtHashKey{
		RowHashVersion: lthash.RowHashVersion(),
		TableID:        "hg_safe.events",
		SchemaHash:     "schema-1",
		PartPhysHash:   phys,
	}
}

func TestPartLtHashCachePutGetHit(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(0)
	key := testKey("phys-a")
	want := PartLtHashEntry{Key: key, PartitionID: "p1", PartName: "all_1_1_0", RowCount: 3, PartRowLtHash: rawAccumHex("a"), Source: "byte_side_scan"}
	if err := c.Put(ctx, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := c.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get hit: ok=%v err=%v", ok, err)
	}
	if got.PartRowLtHash != want.PartRowLtHash || got.RowCount != want.RowCount {
		t.Fatalf("Get returned %+v, want lthash/rows of %+v", got, want)
	}
}

func TestPartLtHashCacheMiss(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(0)
	if _, ok, err := c.Get(ctx, testKey("absent")); ok || err != nil {
		t.Fatalf("expected clean miss, got ok=%v err=%v", ok, err)
	}
}

func TestPartLtHashCacheIncompleteKeyIsMissNotError(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(0)
	// Empty PartPhysHash must never hit (guard #5): it would collide unrelated
	// parts. Get returns a clean miss; Put refuses.
	empty := PartLtHashKey{RowHashVersion: lthash.RowHashVersion(), TableID: "hg_safe.events", SchemaHash: "s"}
	if _, ok, err := c.Get(ctx, empty); ok || err != nil {
		t.Fatalf("empty-phys Get: expected clean miss, got ok=%v err=%v", ok, err)
	}
	if err := c.Put(ctx, PartLtHashEntry{Key: empty, PartRowLtHash: rawAccumHex("x")}); err == nil {
		t.Fatalf("expected Put with incomplete key to error")
	}
}

func TestPartLtHashCacheRejectsNonAccumulatorHash(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(0)
	// Guard #6: a digest fallback (not a raw accumulator) must never enter the
	// cache, or a later arithmetic CAS would diverge from a readback.
	err := c.Put(ctx, PartLtHashEntry{Key: testKey("phys-b"), PartRowLtHash: "0xdeadbeef"})
	if err == nil {
		t.Fatalf("expected Put to reject a non-accumulator PartRowLtHash")
	}
	if _, ok, _ := c.Get(ctx, testKey("phys-b")); ok {
		t.Fatalf("rejected entry must not be retrievable")
	}
}

func TestPartLtHashCacheInvalidate(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(0)
	keyA := testKey("phys-a")
	keyOther := PartLtHashKey{RowHashVersion: lthash.RowHashVersion(), TableID: "hg_safe.other", SchemaHash: "s", PartPhysHash: "phys-c"}
	mustPut(t, c, keyA, "a")
	mustPut(t, c, keyOther, "c")

	if err := c.InvalidateTable(ctx, "hg_safe.events"); err != nil {
		t.Fatalf("InvalidateTable: %v", err)
	}
	if _, ok, _ := c.Get(ctx, keyA); ok {
		t.Fatalf("InvalidateTable should have evicted hg_safe.events entry")
	}
	if _, ok, _ := c.Get(ctx, keyOther); !ok {
		t.Fatalf("InvalidateTable must not evict a different table's entry")
	}

	if err := c.Delete(ctx, keyOther); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := c.Get(ctx, keyOther); ok {
		t.Fatalf("Delete should have removed the entry")
	}

	mustPut(t, c, keyA, "a")
	if err := c.InvalidateAll(ctx); err != nil {
		t.Fatalf("InvalidateAll: %v", err)
	}
	if _, ok, _ := c.Get(ctx, keyA); ok {
		t.Fatalf("InvalidateAll should have emptied the cache")
	}
}

func TestPartLtHashCacheLRUBound(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(2)
	k1, k2, k3 := testKey("p1"), testKey("p2"), testKey("p3")
	mustPut(t, c, k1, "1")
	mustPut(t, c, k2, "2")
	// Touch k1 so k2 becomes least-recently-used.
	if _, ok, _ := c.Get(ctx, k1); !ok {
		t.Fatalf("k1 should still be present")
	}
	mustPut(t, c, k3, "3") // evicts k2 (LRU), not k1
	if _, ok, _ := c.Get(ctx, k2); ok {
		t.Fatalf("k2 should have been evicted as least-recently-used")
	}
	if _, ok, _ := c.Get(ctx, k1); !ok {
		t.Fatalf("k1 should have survived (recently used)")
	}
	if _, ok, _ := c.Get(ctx, k3); !ok {
		t.Fatalf("k3 should be present")
	}
}

func TestPartLtHashCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	c := NewInMemoryPartLtHashCache(64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				phys := "phys-" + strconv.Itoa((g*200+i)%50)
				key := testKey(phys)
				_ = c.Put(ctx, PartLtHashEntry{Key: key, PartRowLtHash: rawAccumHex(phys)})
				_, _, _ = c.Get(ctx, key)
			}
		}(g)
	}
	wg.Wait()
}

func TestPartSchemaHashMatchesNative(t *testing.T) {
	// PartSchemaHash must be exactly nativeSchemaHash so the cache key never
	// diverges from the row-fold's schema identity.
	cols := []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "v", Type: "String"}}
	if PartSchemaHash("hg_safe.events", cols) != nativeSchemaHash("hg_safe.events", cols) {
		t.Fatalf("PartSchemaHash diverged from nativeSchemaHash")
	}
}

func TestRowHashVersionBindsBothProfiles(t *testing.T) {
	// The version token must change if either profile changes.
	v := lthash.RowHashVersion()
	if wantSub := lthash.CanonicalRowProfile(); !strings.Contains(v, wantSub) {
		t.Fatalf("RowHashVersion %q must contain row profile %q", v, wantSub)
	}
	if wantSub := lthash.AccumulatorProfile(); !strings.Contains(v, wantSub) {
		t.Fatalf("RowHashVersion %q must contain accumulator profile %q", v, wantSub)
	}
}

func mustPut(t *testing.T, c PartLtHashCache, key PartLtHashKey, seed string) {
	t.Helper()
	if err := c.Put(context.Background(), PartLtHashEntry{Key: key, PartRowLtHash: rawAccumHex(seed)}); err != nil {
		t.Fatalf("Put(%s): %v", seed, err)
	}
}
