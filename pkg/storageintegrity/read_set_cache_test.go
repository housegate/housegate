package storageintegrity

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeReadSetGate struct {
	mu       sync.Mutex
	calls    int
	decision SafeReadDecision
	err      error
}

func (f *fakeReadSetGate) CheckSafeRead(_ context.Context, _ SafeReadRequest) (SafeReadDecision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.decision, f.err
}

func (f *fakeReadSetGate) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func withFrozenClock(t *testing.T) *time.Time {
	t.Helper()
	base := time.Unix(1_700_000_000, 0)
	cur := base
	orig := nowFunc
	nowFunc = func() time.Time { return cur }
	t.Cleanup(func() { nowFunc = orig })
	return &cur
}

func TestCachingReadSetGateServesWithinTTL(t *testing.T) {
	ctx := context.Background()
	clock := withFrozenClock(t)
	inner := &fakeReadSetGate{decision: SafeReadDecision{Active: true, SnapshotID: "snap-1"}}
	gate := NewCachingReadSetGate(inner, time.Second)
	req := SafeReadRequest{NodeID: "n1", SnapshotID: "snap-1", TableIDs: []string{"t.a", "t.b"}}

	if _, err := gate.CheckSafeRead(ctx, req); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second identical read within TTL is served from cache (no extra call).
	if _, err := gate.CheckSafeRead(ctx, req); err != nil {
		t.Fatalf("second: %v", err)
	}
	if inner.count() != 1 {
		t.Fatalf("expected 1 underlying call within TTL, got %d", inner.count())
	}
	// Table order does not matter (normalized key).
	reqReordered := SafeReadRequest{NodeID: "n1", SnapshotID: "snap-1", TableIDs: []string{"t.b", "t.a"}}
	if _, err := gate.CheckSafeRead(ctx, reqReordered); err != nil {
		t.Fatalf("reordered: %v", err)
	}
	if inner.count() != 1 {
		t.Fatalf("reordered table set must hit the same cache entry, got %d calls", inner.count())
	}
	// After the TTL elapses the gate re-checks.
	*clock = clock.Add(2 * time.Second)
	if _, err := gate.CheckSafeRead(ctx, req); err != nil {
		t.Fatalf("post-ttl: %v", err)
	}
	if inner.count() != 2 {
		t.Fatalf("expected a re-check after TTL, got %d calls", inner.count())
	}
}

func TestCachingReadSetGateDistinctKeys(t *testing.T) {
	ctx := context.Background()
	withFrozenClock(t)
	inner := &fakeReadSetGate{decision: SafeReadDecision{Active: true}}
	gate := NewCachingReadSetGate(inner, time.Minute)

	// Different snapshot, node, and table set are all distinct cache entries.
	_, _ = gate.CheckSafeRead(ctx, SafeReadRequest{NodeID: "n1", SnapshotID: "s1", TableIDs: []string{"t.a"}})
	_, _ = gate.CheckSafeRead(ctx, SafeReadRequest{NodeID: "n1", SnapshotID: "s2", TableIDs: []string{"t.a"}})
	_, _ = gate.CheckSafeRead(ctx, SafeReadRequest{NodeID: "n2", SnapshotID: "s1", TableIDs: []string{"t.a"}})
	_, _ = gate.CheckSafeRead(ctx, SafeReadRequest{NodeID: "n1", SnapshotID: "s1", TableIDs: []string{"t.b"}})
	if inner.count() != 4 {
		t.Fatalf("expected 4 distinct underlying calls, got %d", inner.count())
	}
}

func TestCachingReadSetGateBypassesWhenSnapshotIDEmpty(t *testing.T) {
	ctx := context.Background()
	withFrozenClock(t)
	inner := &fakeReadSetGate{decision: SafeReadDecision{Active: true, SnapshotID: "snap-1"}}
	gate := NewCachingReadSetGate(inner, time.Minute)
	req := SafeReadRequest{NodeID: "n1", TableIDs: []string{"t.a"}}

	if _, err := gate.CheckSafeRead(ctx, req); err != nil {
		t.Fatalf("first CheckSafeRead: %v", err)
	}
	if _, err := gate.CheckSafeRead(ctx, req); err != nil {
		t.Fatalf("second CheckSafeRead: %v", err)
	}
	if inner.count() != 2 {
		t.Fatalf("empty snapshot id must not be cached, got %d calls", inner.count())
	}
}

func TestCachingReadSetGateInvalidateForcesRecheck(t *testing.T) {
	ctx := context.Background()
	withFrozenClock(t)
	inner := &fakeReadSetGate{decision: SafeReadDecision{Active: true}}
	gate := NewCachingReadSetGate(inner, time.Minute)
	req := SafeReadRequest{NodeID: "n1", SnapshotID: "s1", TableIDs: []string{"t.a"}}

	_, _ = gate.CheckSafeRead(ctx, req)
	_, _ = gate.CheckSafeRead(ctx, req) // cached
	if inner.count() != 1 {
		t.Fatalf("expected 1 call before invalidate, got %d", inner.count())
	}
	// A new manifest / quarantine invalidates → next read re-checks.
	gate.Invalidate()
	_, _ = gate.CheckSafeRead(ctx, req)
	if inner.count() != 2 {
		t.Fatalf("expected a re-check after Invalidate, got %d", inner.count())
	}
}

func TestCachingReadSetGateZeroTTLPassesThrough(t *testing.T) {
	ctx := context.Background()
	inner := &fakeReadSetGate{decision: SafeReadDecision{Active: true}}
	gate := NewCachingReadSetGate(inner, 0) // disabled
	req := SafeReadRequest{NodeID: "n1", SnapshotID: "s1", TableIDs: []string{"t.a"}}
	for i := 0; i < 3; i++ {
		if _, err := gate.CheckSafeRead(ctx, req); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if inner.count() != 3 {
		t.Fatalf("zero TTL must pass through every call, got %d", inner.count())
	}
}
