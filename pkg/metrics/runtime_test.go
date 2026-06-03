package metrics

import "testing"

// CollectRuntime must report a live, non-zero goroutine count: the test
// binary itself is running goroutines, so the value is always > 0.
func TestCollectRuntimeGoroutinesPositive(t *testing.T) {
	m := CollectRuntime()
	if m.Goroutines <= 0 {
		t.Fatalf("Goroutines = %d, want > 0", m.Goroutines)
	}
}

// HeapAllocBytes is the bytes of allocated heap objects. A running Go process
// always has live heap, so this must be > 0.
func TestCollectRuntimeHeapAllocPositive(t *testing.T) {
	m := CollectRuntime()
	if m.HeapAllocBytes == 0 {
		t.Fatalf("HeapAllocBytes = 0, want > 0")
	}
}

// GCPauseSeconds is a cumulative duration; it can legitimately be 0 if no GC
// has run yet, but it must never be negative.
func TestCollectRuntimeGCPauseNonNegative(t *testing.T) {
	m := CollectRuntime()
	if m.GCPauseSeconds < 0 {
		t.Fatalf("GCPauseSeconds = %v, want >= 0", m.GCPauseSeconds)
	}
}

// CollectRuntime is a pure function: a second call must keep producing
// well-formed values (goroutines still positive) rather than depending on
// hidden first-call state.
func TestCollectRuntimeRepeatable(t *testing.T) {
	_ = CollectRuntime()
	m := CollectRuntime()
	if m.Goroutines <= 0 {
		t.Fatalf("second call Goroutines = %d, want > 0", m.Goroutines)
	}
}
