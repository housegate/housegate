package storageintegrity

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// nowFunc is the clock used by the read-set cache; overridable in tests.
var nowFunc = time.Now

// CachingReadSetGate is a local TTL cache in front of a ReadSetGate
// (CheckSafeRead). A normal safe SELECT no longer makes a control-plane round
// trip on every query: an identical read within the TTL reuses the arbiter's
// prior verdict. It caches the arbiter's decision only — it never re-derives
// read-set membership (that stays authoritative on the arbiter).
//
// Correctness bounds:
//   - The key includes the requested snapshot id and the exact table set, so a
//     different snapshot/table request is a distinct entry.
//   - The TTL bounds how long a stale verdict can be served after the read set
//     changes; on a new SafeSnapshotManifest, on local quarantine, or on any
//     event that could flip membership, the caller invalidates explicitly.
//   - A short TTL keeps the gate responsive; the cache is a latency optimization
//     over a fail-closed gate, never a relaxation of it.
type CachingReadSetGate struct {
	inner ReadSetGate
	ttl   time.Duration

	mu    sync.Mutex
	items map[string]cachedReadDecision
	// gen is bumped by Invalidate; entries stamped with an older gen are ignored.
	gen uint64
}

type cachedReadDecision struct {
	decision SafeReadDecision
	expires  time.Time
	gen      uint64
}

// NewCachingReadSetGate wraps inner with a TTL decision cache. A non-positive
// ttl disables caching (every call passes through), so the wrapper is always
// safe to install.
func NewCachingReadSetGate(inner ReadSetGate, ttl time.Duration) *CachingReadSetGate {
	return &CachingReadSetGate{
		inner: inner,
		ttl:   ttl,
		items: make(map[string]cachedReadDecision),
	}
}

func (g *CachingReadSetGate) CheckSafeRead(ctx context.Context, req SafeReadRequest) (SafeReadDecision, error) {
	if g == nil || g.inner == nil {
		return SafeReadDecision{}, nil
	}
	if g.ttl <= 0 {
		return g.inner.CheckSafeRead(ctx, req)
	}
	if req.SnapshotID == "" {
		return g.inner.CheckSafeRead(ctx, req)
	}
	key := readSetCacheKey(req)
	now := nowFunc()

	g.mu.Lock()
	if entry, ok := g.items[key]; ok && entry.gen == g.gen && now.Before(entry.expires) {
		g.mu.Unlock()
		return entry.decision, nil
	}
	gen := g.gen
	g.mu.Unlock()

	decision, err := g.inner.CheckSafeRead(ctx, req)
	if err != nil {
		return SafeReadDecision{}, err
	}

	g.mu.Lock()
	// Only store under the generation observed before the call; if Invalidate
	// ran concurrently (gen advanced), drop this result rather than cache a
	// verdict that predates the invalidation.
	if gen == g.gen {
		g.items[key] = cachedReadDecision{decision: decision, expires: now.Add(g.ttl), gen: gen}
	}
	g.mu.Unlock()
	return decision, nil
}

// Invalidate drops every cached verdict. Call it when a new SafeSnapshotManifest
// is observed, when this node is quarantined, or on any event that could change
// read-set membership, so the next read re-checks with the arbiter.
func (g *CachingReadSetGate) Invalidate() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.gen++
	g.items = make(map[string]cachedReadDecision)
	g.mu.Unlock()
}

// readSetCacheKey builds a stable key from the node id, the sorted table set,
// and the requested snapshot id. Table order is normalized so the same logical
// read hits regardless of the order tables were listed.
func readSetCacheKey(req SafeReadRequest) string {
	tables := append([]string(nil), req.TableIDs...)
	sort.Strings(tables)
	var b strings.Builder
	b.WriteString(req.NodeID)
	b.WriteByte(0)
	b.WriteString(req.SnapshotID)
	b.WriteByte(0)
	b.WriteString(strings.Join(tables, ","))
	return b.String()
}
