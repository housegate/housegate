package storageintegrity

import (
	"container/list"
	"fmt"
	"sync"
)

// RowHashProfileVersion names the row-hash profile the PartLtHash cache key binds
// to. It mirrors the pkg/lthash canonical row profile domain
// ("housegate-row-mvp-v0") and MUST be bumped in lockstep with it: the cache key
// includes this string, so a profile change forces every prior entry to miss
// rather than serve a stale LtHash computed under the old profile. It is a
// local-performance discriminator, never a wire-committed value.
const RowHashProfileVersion = "housegate-row-mvp-v0"

// PartCacheKey binds ALL of the identity a cached LtHash depends on (design §5.1
// step 5 / §7 part_lthash_cache): the row-hash profile, the table, the schema
// hash, the part's physical hash, and the part's row count and byte size. A hit
// requires every field to match — any drift (a re-materialized part, a schema
// change, a profile bump) changes the key and forces a real scan. The physical
// hash is the primary content anchor; rows/bytes and schema/profile are
// belt-and-suspenders so a phys-hash collision or profile drift can never serve a
// wrong LtHash.
type PartCacheKey struct {
	RowHashVersion string
	TableID        string
	SchemaHash     string
	PartPhysHash   string
	RowCount       uint64
	Bytes          uint64
}

// Valid fails closed on a key missing any binding field. A part with no physical
// hash cannot be safely cached (nothing anchors the content), so it is rejected.
func (k PartCacheKey) Valid() error {
	if k.RowHashVersion == "" || k.TableID == "" || k.SchemaHash == "" || k.PartPhysHash == "" {
		return fmt.Errorf("part cache key: missing profile/table/schema/phys-hash binding")
	}
	return nil
}

// PartLtHashResult is the cached inspection result: the LtHash a real scan would
// have produced for the part, plus the part name and row count it was computed
// over.
type PartLtHashResult struct {
	PartName  string
	RowLtHash string
	RowCount  uint64
}

// PartLtHashCache is a bounded in-memory LRU cache from PartCacheKey to
// PartLtHashResult. It is a discardable local performance layer (design §5.1):
// dropping it only forces real scans, never a wrong answer. Safe for concurrent
// use.
type PartLtHashCache struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List // front = most recently used
	entries    map[PartCacheKey]*list.Element
}

type cacheEntry struct {
	key   PartCacheKey
	value PartLtHashResult
}

// NewPartLtHashCache constructs a cache bounded to maxEntries (<= 0 means
// unbounded). The bound is an LRU eviction cap, not a correctness knob — a full
// cache still returns correct misses.
func NewPartLtHashCache(maxEntries int) *PartLtHashCache {
	return &PartLtHashCache{
		maxEntries: maxEntries,
		ll:         list.New(),
		entries:    map[PartCacheKey]*list.Element{},
	}
}

// Get returns the cached result for a key, moving it to most-recently-used. A
// miss (or an invalid key) returns ok=false.
func (c *PartLtHashCache) Get(key PartCacheKey) (PartLtHashResult, bool) {
	if key.Valid() != nil {
		return PartLtHashResult{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return PartLtHashResult{}, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).value, true
}

// Put inserts or refreshes a key's result, evicting the least-recently-used entry
// if the cache is at capacity. An invalid key is rejected (fail-closed: the cache
// never stores an unanchored entry).
func (c *PartLtHashCache) Put(key PartCacheKey, value PartLtHashResult) error {
	if err := key.Valid(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		el.Value.(*cacheEntry).value = value
		c.ll.MoveToFront(el)
		return nil
	}
	el := c.ll.PushFront(&cacheEntry{key: key, value: value})
	c.entries[key] = el
	if c.maxEntries > 0 && c.ll.Len() > c.maxEntries {
		c.evictOldest()
	}
	return nil
}

func (c *PartLtHashCache) evictOldest() {
	el := c.ll.Back()
	if el == nil {
		return
	}
	c.ll.Remove(el)
	delete(c.entries, el.Value.(*cacheEntry).key)
}

// Len returns the live entry count (observability/test accessor).
func (c *PartLtHashCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
