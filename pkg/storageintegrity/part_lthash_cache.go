package storageintegrity

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"

	"housegate/housegate/pkg/lthash"
)

// PartLtHashKey identifies a cached part_row_lthash by CONTENT, not by name.
//
// Every field must equal the exact input the row fold used to produce the
// cached PartRowLtHash, or a cache hit would return bytes computed under a
// different profile/table/schema/part:
//
//   - RowHashVersion binds both the row-encoding profile and the accumulator
//     profile (lthash.RowHashVersion()). A bump to either invalidates every
//     entry transparently.
//   - TableID is the exact string passed to lthash.RowHash as the table domain
//     — i.e. normalizeTableID(physicalTable). It is deliberately the PHYSICAL
//     table identity (scratch/shadow/compact/safe/unsafe differ), not the
//     logical arbiter table id, because the physical name frames every row
//     element (see lthash.EncodeRow).
//   - SchemaHash is derived from the live column (name,type) set via the shared
//     schema hash (see PartSchemaHash), because column type participates in the
//     row element bytes.
//   - PartPhysHash is the ClickHouse system.parts hash_of_all_files: it is the
//     content address of the part's on-disk bytes. Two parts with identical
//     bytes legitimately share a PartRowLtHash — that is intended
//     content-addressing, not a collision. An empty PartPhysHash is never a
//     valid key (the caller must bypass the cache and fold rows instead).
type PartLtHashKey struct {
	RowHashVersion string
	TableID        string
	SchemaHash     string
	PartPhysHash   string
}

func (k PartLtHashKey) valid() bool {
	return k.RowHashVersion != "" && k.TableID != "" && k.SchemaHash != "" && k.PartPhysHash != ""
}

// PartLtHashEntry is one cached part's row-content commitment plus observational
// metadata. Only PartRowLtHash and RowCount are load-bearing for a cache hit;
// the PartitionID/PartName/Bytes fields are retained for observability and MUST
// NOT be returned to a consumer in place of the live system.parts values (a
// stale part name would break active-set equality and the byte-side part-set
// hash). Callers resolve name/partition/rows/bytes from the fresh metadata query
// and take only PartRowLtHash from the cache.
type PartLtHashEntry struct {
	Key           PartLtHashKey
	PartitionID   string
	PartName      string
	RowCount      uint64
	Bytes         uint64
	PartRowLtHash string
	// Source records which reader last observed the part (byte_side_scan |
	// safe_audit | mutation_base | promotion_readback), for debugging only.
	Source string
}

// PartLtHashCache stores per-part row-LtHash commitments keyed by physical part
// content. It is a local, discardable, rebuildable acceleration structure: a
// miss is always safe (recompute by scanning), and because the key is
// content-addressed a stale entry can only ever be served for a part whose
// bytes are byte-identical, so staleness is self-limiting.
type PartLtHashCache interface {
	Get(ctx context.Context, key PartLtHashKey) (PartLtHashEntry, bool, error)
	Put(ctx context.Context, entry PartLtHashEntry) error
	Delete(ctx context.Context, key PartLtHashKey) error
	// InvalidateTable drops every entry for the given physical TableID.
	InvalidateTable(ctx context.Context, tableID string) error
	// InvalidateAll drops every entry.
	InvalidateAll(ctx context.Context) error
}

// inMemoryPartLtHashCache is a bounded (LRU) in-memory PartLtHashCache. It is
// safe for concurrent use: the shared ActivePartReader is fanned to several
// background workers, so Get/Put race by construction.
type inMemoryPartLtHashCache struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List // front = most recently used
	items      map[PartLtHashKey]*list.Element
}

type lruItem struct {
	key   PartLtHashKey
	entry PartLtHashEntry
}

const defaultPartLtHashCacheMaxEntries = 1_000_000

// NewInMemoryPartLtHashCache returns a bounded LRU cache. maxEntries <= 0 uses
// the default bound; the cache never grows past the bound (least-recently-used
// entries are evicted on Put).
func NewInMemoryPartLtHashCache(maxEntries int) PartLtHashCache {
	if maxEntries <= 0 {
		maxEntries = defaultPartLtHashCacheMaxEntries
	}
	return &inMemoryPartLtHashCache{
		maxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[PartLtHashKey]*list.Element),
	}
}

func (c *inMemoryPartLtHashCache) Get(_ context.Context, key PartLtHashKey) (PartLtHashEntry, bool, error) {
	if !key.valid() {
		// An incomplete key (notably an empty PartPhysHash) must never hit: it
		// would collide unrelated parts. Treat as a miss, never an error, so the
		// caller falls back to a full scan.
		return PartLtHashEntry{}, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return PartLtHashEntry{}, false, nil
	}
	c.ll.MoveToFront(el)
	return el.Value.(*lruItem).entry, true, nil
}

func (c *inMemoryPartLtHashCache) Put(_ context.Context, entry PartLtHashEntry) error {
	if !entry.Key.valid() {
		return fmt.Errorf("part lthash cache: refusing to cache entry with incomplete key %+v", entry.Key)
	}
	// Guard: only a raw 2048-byte lattice accumulator hex is additive and thus
	// summable into a partition root. A digest fallback (e.g. from a malformed
	// upstream value) must never enter the cache, or a later arithmetic CAS
	// would diverge from a readback. Validate by round-tripping through the
	// accumulator decoder.
	if _, err := lthashAccumulatorFromHex(entry.PartRowLtHash); err != nil {
		return fmt.Errorf("part lthash cache: PartRowLtHash is not a raw accumulator: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[entry.Key]; ok {
		el.Value.(*lruItem).entry = entry
		c.ll.MoveToFront(el)
		return nil
	}
	el := c.ll.PushFront(&lruItem{key: entry.Key, entry: entry})
	c.items[entry.Key] = el
	for c.ll.Len() > c.maxEntries {
		c.evictOldestLocked()
	}
	return nil
}

func (c *inMemoryPartLtHashCache) Delete(_ context.Context, key PartLtHashKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeLocked(el)
	}
	return nil
}

func (c *inMemoryPartLtHashCache) InvalidateTable(_ context.Context, tableID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		if el.Value.(*lruItem).key.TableID == tableID {
			c.removeLocked(el)
		}
		el = next
	}
	return nil
}

func (c *inMemoryPartLtHashCache) InvalidateAll(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.items = make(map[PartLtHashKey]*list.Element)
	return nil
}

func (c *inMemoryPartLtHashCache) evictOldestLocked() {
	if el := c.ll.Back(); el != nil {
		c.removeLocked(el)
	}
}

func (c *inMemoryPartLtHashCache) removeLocked(el *list.Element) {
	c.ll.Remove(el)
	delete(c.items, el.Value.(*lruItem).key)
}

// PartSchemaHash is the shared schema-hash used in a PartLtHashKey. It reuses
// the same derivation the native replay executor and genesis manifest use
// (nativeSchemaHash), so the cache key changes exactly when the row element
// bytes would (column name/type changes), and never diverges from a home-rolled
// variant. cols must be the live column (name,type) set actually folded.
func PartSchemaHash(tableID string, cols []lthash.Column) string {
	return nativeSchemaHash(tableID, cols)
}

// normalizeCachePhysHash trims a system.parts hash_of_all_files into the form
// stored in a key. An empty result signals "no usable content address" and the
// caller must bypass the cache entirely.
func normalizeCachePhysHash(raw string) string {
	return strings.TrimSpace(raw)
}
