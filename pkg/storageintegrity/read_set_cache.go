package storageintegrity

import (
	"sync"
	"time"
)

// Clock is the injected monotonic clock seam the read-set decision cache reads
// time through. It exists so the cache never calls time.Now() directly, which
// keeps its TTL logic deterministically testable.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock wrapping time.Now().
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// ReadSetCacheKey scopes one cached read decision to a (table, requested
// snapshot, worker, read mode) tuple (design §5.2 last bullet): a decision cached
// for one worker/snapshot/mode must never be reused for another.
type ReadSetCacheKey struct {
	TableID           string
	RequestedSnapshot uint64
	WorkerID          string
	ReadMode          string
}

type readSetCacheEntry struct {
	decision GateDecision
	expires  time.Time
}

// ReadSetDecisionCache is a TTL cache in front of one committed SafeReadGate. A
// hit returns a previously computed GateDecision; a miss, an expired entry, or an
// invalidated entry recomputes via the gate. It is fail-safe: it never serves a
// stale allow — every path that could change eligibility (a new safe cut,
// quarantine, watermark lag, active-set mismatch, or TTL expiry) drops the
// affected entries, and on any doubt it recomputes rather than serving cached.
// Safe for concurrent use.
type ReadSetDecisionCache struct {
	mu      sync.Mutex
	gate    SafeReadGate
	ttl     time.Duration
	clock   Clock
	entries map[ReadSetCacheKey]readSetCacheEntry
	// gen is bumped by every gate-changing or invalidating operation. A Decide
	// miss captures the generation it computed under and only publishes its result
	// if the generation is still current, so a concurrent InstallCut/Invalidate
	// can never be shadowed by a stale in-flight Allow.
	gen uint64
	// afterCompute, when non-nil, is invoked inside Decide after the generation is
	// captured and the gate recompute finishes, but before the store re-locks. It
	// is a test-only seam for deterministically landing an invalidation in the
	// guarded window; it is always nil in production.
	afterCompute func()
}

// NewReadSetDecisionCache constructs the cache over a committed SafeReadGate with
// a TTL and an injected clock (a nil clock defaults to SystemClock). A
// non-positive TTL disables caching (every Decide recomputes), which is still
// correct — the cache is a pure optimization.
func NewReadSetDecisionCache(gate SafeReadGate, ttl time.Duration, clock Clock) *ReadSetDecisionCache {
	if clock == nil {
		clock = SystemClock{}
	}
	return &ReadSetDecisionCache{
		gate:    gate,
		ttl:     ttl,
		clock:   clock,
		entries: map[ReadSetCacheKey]readSetCacheEntry{},
	}
}

// Decide returns the read decision for a key: a live cached entry when present
// and unexpired, else a fresh gate.MayServe recompute that is then cached (when
// the TTL is positive). The gate is consulted with the key's worker and snapshot,
// so the cached decision is always the gate's own answer — the cache never
// invents a verdict.
func (c *ReadSetDecisionCache) Decide(key ReadSetCacheKey) GateDecision {
	now := c.clock.Now()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.decision
	}
	// Capture the gate and the generation this recompute is bound to under the
	// lock. MayServe is a pure method on a value copy, so running it outside the
	// lock cannot race with a concurrent InstallCut that reassigns c.gate.
	gate := c.gate
	gen := c.gen
	c.mu.Unlock()

	// Miss / expired: recompute against the captured gate.
	d := gate.MayServe(key.WorkerID, key.RequestedSnapshot)

	if c.afterCompute != nil {
		c.afterCompute()
	}

	if c.ttl > 0 {
		c.mu.Lock()
		// Only publish if no invalidation / cut install happened while we computed.
		// Otherwise this decision is stale (it reflects the old gate) and storing it
		// would shadow the invalidation for a full TTL — a fail-safe violation.
		if c.gen == gen {
			c.entries[key] = readSetCacheEntry{decision: d, expires: now.Add(c.ttl)}
		}
		c.mu.Unlock()
	}
	return d
}

// InstallCut rebinds the cache to a new committed safe cut and flushes ALL
// entries. A new manifest/watermark/read-set is a global eligibility change, so
// every cached decision is dropped fail-safe (design §5.2: invalidate on new
// manifest / quarantine / watermark lag / active-set mismatch). Fail-closed on an
// invalid cut: the old gate is kept and the error returned, so the cache never
// binds to a malformed cut.
func (c *ReadSetDecisionCache) InstallCut(cut SafeCutView) error {
	gate, err := NewSafeReadGate(cut)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gate = gate
	c.entries = map[ReadSetCacheKey]readSetCacheEntry{}
	c.gen++
	return nil
}

// Invalidate drops a single cached decision (targeted eviction).
func (c *ReadSetDecisionCache) Invalidate(key ReadSetCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	c.gen++
}

// InvalidateWorker drops every cached decision for one worker — the eviction hook
// for a quarantine or active-set mismatch surfaced by PR20/PR21/PR22, which
// affects a worker across all tables/snapshots/modes.
func (c *ReadSetDecisionCache) InvalidateWorker(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if k.WorkerID == workerID {
			delete(c.entries, k)
		}
	}
	c.gen++
}

// InvalidateAll flushes the whole cache — the coarse hook for any event whose
// blast radius is uncertain.
func (c *ReadSetDecisionCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = map[ReadSetCacheKey]readSetCacheEntry{}
	c.gen++
}

// Len returns the live entry count (observability/test accessor). Expired-but-
// not-yet-evicted entries are counted; they are dropped lazily on the next
// Decide for their key.
func (c *ReadSetDecisionCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
