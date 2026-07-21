package storageintegrity

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a deterministic, manually-advanced Clock.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func readGateFixture(t *testing.T) SafeReadGate {
	t.Helper()
	cut := NewSafeCutView("m-1", 10,
		map[string]uint64{"w-1": 10, "w-2": 10},
		map[string]bool{"w-1": true, "w-2": true},
		3, nil)
	g, err := NewSafeReadGate(cut)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	return g
}

func readKey(worker string, snap uint64) ReadSetCacheKey {
	return ReadSetCacheKey{TableID: "net1.events", RequestedSnapshot: snap, WorkerID: worker, ReadMode: "safe"}
}

func TestReadSetDecisionCache_HitReturnsGateDecision(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, clk)

	// First Decide computes via the gate (w-1 in read set, watermark covers => allowed).
	d1 := c.Decide(readKey("w-1", 5))
	if !d1.Allowed {
		t.Fatalf("w-1 must be allowed: %+v", d1)
	}
	if c.Len() != 1 {
		t.Fatalf("decision must be cached, len=%d", c.Len())
	}
	// A worker outside the read set is denied and cached with the reason.
	d2 := c.Decide(readKey("w-9", 5))
	if d2.Allowed || d2.Reason != GateDenyNotInReadSet {
		t.Fatalf("w-9 must be denied not-in-read-set: %+v", d2)
	}
}

func TestReadSetDecisionCache_ExpiryRecomputes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, clk)
	c.Decide(readKey("w-1", 5))
	// Within TTL: still a hit (no recompute needed; entry present).
	clk.advance(4 * time.Second)
	if c.Len() != 1 {
		t.Fatal("entry should still be live before TTL")
	}
	// Past TTL: the entry is stale; a Decide recomputes and refreshes it.
	clk.advance(2 * time.Second) // now 6s > 5s TTL
	d := c.Decide(readKey("w-1", 5))
	if !d.Allowed {
		t.Fatalf("recompute must still allow w-1: %+v", d)
	}
}

func TestReadSetDecisionCache_ZeroTTLNeverCaches(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 0, clk)
	c.Decide(readKey("w-1", 5))
	if c.Len() != 0 {
		t.Fatalf("a non-positive TTL must not cache, len=%d", c.Len())
	}
}

func TestReadSetDecisionCache_InstallCutFlushesAll(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, clk)
	c.Decide(readKey("w-1", 5))
	c.Decide(readKey("w-2", 5))
	if c.Len() != 2 {
		t.Fatalf("two decisions cached, len=%d", c.Len())
	}

	// A new safe cut quarantines w-2. InstallCut must flush every entry.
	newCut := NewSafeCutView("m-2", 12,
		map[string]uint64{"w-1": 12, "w-2": 12},
		map[string]bool{"w-1": true, "w-2": true},
		4, map[string]bool{"w-2": true})
	if err := c.InstallCut(newCut); err != nil {
		t.Fatalf("install cut: %v", err)
	}
	if c.Len() != 0 {
		t.Fatalf("a new cut must flush all entries, len=%d", c.Len())
	}
	// After the new cut, w-2 is now quarantined — the recomputed decision reflects
	// it (no stale allow served).
	d := c.Decide(readKey("w-2", 5))
	if d.Allowed || d.Reason != GateDenyQuarantined {
		t.Fatalf("w-2 must now be denied quarantined: %+v", d)
	}
}

func TestReadSetDecisionCache_InstallCutRejectsInvalidAndKeepsOldGate(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, clk)
	c.Decide(readKey("w-1", 5))
	// An invalid cut (blank manifest) must be rejected and the old gate kept.
	if err := c.InstallCut(SafeCutView{}); err == nil {
		t.Fatal("an invalid cut must be rejected")
	}
	// The old gate still serves.
	if d := c.Decide(readKey("w-1", 5)); !d.Allowed {
		t.Fatalf("old gate must still serve after a rejected cut: %+v", d)
	}
}

func TestReadSetDecisionCache_TargetedInvalidation(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, clk)
	c.Decide(readKey("w-1", 5))
	c.Decide(readKey("w-2", 5))

	c.Invalidate(readKey("w-1", 5))
	if c.Len() != 1 {
		t.Fatalf("targeted invalidate must drop one entry, len=%d", c.Len())
	}

	// InvalidateWorker drops every entry for a worker across snapshots/modes.
	c.Decide(readKey("w-2", 6))
	c.Decide(ReadSetCacheKey{TableID: "net1.other", RequestedSnapshot: 5, WorkerID: "w-2", ReadMode: "safe"})
	c.InvalidateWorker("w-2")
	for k := range c.entries {
		if k.WorkerID == "w-2" {
			t.Fatal("InvalidateWorker must drop all w-2 entries")
		}
	}

	c.InvalidateAll()
	if c.Len() != 0 {
		t.Fatalf("InvalidateAll must flush, len=%d", c.Len())
	}
}

func TestReadSetDecisionCache_NilClockDefaultsToSystem(t *testing.T) {
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, nil)
	if d := c.Decide(readKey("w-1", 5)); !d.Allowed {
		t.Fatalf("nil clock must still decide correctly: %+v", d)
	}
}

// TestReadSetDecisionCache_InvalidationDuringInflightDecideNotShadowed proves the
// generation guard: if an invalidation lands while a Decide miss is computing
// (modelled via the afterCompute seam, which fires after the generation is
// captured and the gate recompute finishes but before the store), the in-flight
// (now stale) decision must NOT be stored — otherwise it would shadow the
// invalidation for a full TTL, violating the fail-safe guarantee.
func TestReadSetDecisionCache_InvalidationDuringInflightDecideNotShadowed(t *testing.T) {
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, &fakeClock{t: time.Unix(1000, 0)})
	fired := false
	c.afterCompute = func() {
		if !fired { // one-shot so we don't recurse via the invalidation's own path
			fired = true
			c.InvalidateAll() // a concurrent invalidation lands mid-flight
		}
	}

	d := c.Decide(readKey("w-1", 5))
	if !d.Allowed {
		t.Fatalf("the returned decision is still the gate's answer: %+v", d)
	}
	// The stale in-flight decision must not have been published: the invalidation
	// bumped the generation after this Decide captured it, so the store is skipped.
	if c.Len() != 0 {
		t.Fatalf("a decision computed across an invalidation must not be cached, len=%d", c.Len())
	}

	// A fresh Decide (no concurrent invalidation) does cache, confirming the guard
	// only suppresses the stale store, not all stores.
	c.afterCompute = nil
	_ = c.Decide(readKey("w-1", 5))
	if c.Len() != 1 {
		t.Fatalf("a clean Decide must cache, len=%d", c.Len())
	}
}

// TestReadSetDecisionCache_ConcurrentDecideAndInstallCutRaceFree exercises the
// gate read/write under -race: many Decide goroutines racing repeated InstallCut.
func TestReadSetDecisionCache_ConcurrentDecideAndInstallCutRaceFree(t *testing.T) {
	c := NewReadSetDecisionCache(readGateFixture(t), 5*time.Second, &fakeClock{t: time.Unix(1000, 0)})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.Decide(readKey("w-1", 5))
				c.Decide(readKey("w-2", 5))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cut := NewSafeCutView("m-x", 12,
					map[string]uint64{"w-1": 12, "w-2": 12},
					map[string]bool{"w-1": true, "w-2": true},
					uint64(4+n), map[string]bool{"w-2": true})
				_ = c.InstallCut(cut)
				c.InvalidateWorker("w-1")
			}
		}(i)
	}
	wg.Wait()
}
