package metrics

import (
	"math"
	"testing"
)

// floatEq compares two float64 values within a small tolerance so that the
// OS-CPU conversion (microseconds → seconds, etc.) does not fail on the last
// representable bit.
func floatEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// TestMapCHMetricsCuratedKeys feeds the three source maps with the curated
// keys the poller reads from system.metrics / system.events /
// system.asynchronous_metrics and asserts every CHReplicaMetrics field is
// populated from the right source.
func TestMapCHMetricsCuratedKeys(t *testing.T) {
	metrics := map[string]float64{
		"MemoryTracking":       1024,
		"PartsActive":          7,
		"ReplicasMaxQueueSize": 3,
	}
	events := map[string]float64{
		"Query":       100,
		"InsertQuery": 25,
	}
	async := map[string]float64{
		"OSUserTimeNormalized": 12.5,
	}

	got := mapCHMetrics("ch-1:9000", metrics, events, async)

	if got.Replica != "ch-1:9000" {
		t.Errorf("Replica = %q, want %q", got.Replica, "ch-1:9000")
	}
	if !got.Reachable {
		t.Error("Reachable = false, want true (mapper only runs after a successful scrape)")
	}
	if got.MemoryTrackingBytes != 1024 {
		t.Errorf("MemoryTrackingBytes = %d, want 1024", got.MemoryTrackingBytes)
	}
	if got.QueryTotal != 100 {
		t.Errorf("QueryTotal = %d, want 100", got.QueryTotal)
	}
	if got.InsertTotal != 25 {
		t.Errorf("InsertTotal = %d, want 25", got.InsertTotal)
	}
	if got.PartsActive != 7 {
		t.Errorf("PartsActive = %d, want 7", got.PartsActive)
	}
	if got.ReplicationQueue != 3 {
		t.Errorf("ReplicationQueue = %d, want 3", got.ReplicationQueue)
	}
	if !floatEq(got.OSCPUSeconds, 12.5) {
		t.Errorf("OSCPUSeconds = %v, want 12.5", got.OSCPUSeconds)
	}
}

// TestMapCHMetricsMissingKeysZero asserts that when none of the curated keys
// are present, every value field defaults to 0 while Replica/Reachable are
// still set. A missing key must never panic and must never carry over a stale
// value.
func TestMapCHMetricsMissingKeysZero(t *testing.T) {
	got := mapCHMetrics("ch-2:9000", map[string]float64{}, map[string]float64{}, map[string]float64{})

	if got.Replica != "ch-2:9000" {
		t.Errorf("Replica = %q, want %q", got.Replica, "ch-2:9000")
	}
	if !got.Reachable {
		t.Error("Reachable = false, want true")
	}
	if got.MemoryTrackingBytes != 0 {
		t.Errorf("MemoryTrackingBytes = %d, want 0", got.MemoryTrackingBytes)
	}
	if got.QueryTotal != 0 {
		t.Errorf("QueryTotal = %d, want 0", got.QueryTotal)
	}
	if got.InsertTotal != 0 {
		t.Errorf("InsertTotal = %d, want 0", got.InsertTotal)
	}
	if got.PartsActive != 0 {
		t.Errorf("PartsActive = %d, want 0", got.PartsActive)
	}
	if got.ReplicationQueue != 0 {
		t.Errorf("ReplicationQueue = %d, want 0", got.ReplicationQueue)
	}
	if got.OSCPUSeconds != 0 {
		t.Errorf("OSCPUSeconds = %v, want 0", got.OSCPUSeconds)
	}
}

// TestMapCHMetricsNilMaps asserts the mapper tolerates nil source maps (a
// reachable replica that returned an empty result set, or a defensive caller).
// Reading from a nil map in Go is a zero-value read, not a panic; this locks
// that contract in.
func TestMapCHMetricsNilMaps(t *testing.T) {
	got := mapCHMetrics("ch-3:9000", nil, nil, nil)

	if got.Replica != "ch-3:9000" {
		t.Errorf("Replica = %q, want %q", got.Replica, "ch-3:9000")
	}
	if !got.Reachable {
		t.Error("Reachable = false, want true")
	}
	if got.QueryTotal != 0 || got.MemoryTrackingBytes != 0 || got.OSCPUSeconds != 0 {
		t.Errorf("nil maps should yield zero fields, got %+v", got)
	}
}

// TestMapCHMetricsPartsActiveFallback asserts the PartsActive field is filled
// from the NumberOfActiveParts asynchronous-metric key when the PartsActive
// system.metrics key is absent. ClickHouse exposes the active-parts count
// under different names across the two tables and versions; the mapper must
// resolve either.
func TestMapCHMetricsPartsActiveFallback(t *testing.T) {
	got := mapCHMetrics(
		"ch-4:9000",
		map[string]float64{}, // no PartsActive in system.metrics
		map[string]float64{},
		map[string]float64{"NumberOfActiveParts": 42},
	)
	if got.PartsActive != 42 {
		t.Errorf("PartsActive (fallback) = %d, want 42", got.PartsActive)
	}
}

// TestMapCHMetricsOSCPUFallback asserts OSCPUSeconds falls back to the
// micro-seconds virtual-time key (converted to seconds) when the normalized
// key is absent.
func TestMapCHMetricsOSCPUFallback(t *testing.T) {
	got := mapCHMetrics(
		"ch-5:9000",
		map[string]float64{},
		map[string]float64{},
		// 2_500_000 µs = 2.5 s
		map[string]float64{"OSCPUVirtualTimeMicroseconds": 2_500_000},
	)
	if !floatEq(got.OSCPUSeconds, 2.5) {
		t.Errorf("OSCPUSeconds (µs fallback) = %v, want 2.5", got.OSCPUSeconds)
	}
}

// TestMapCHMetricsPrimaryKeyWins asserts that when both a primary and a
// fallback key are present, the primary (preferred) key is used. This guards
// against a future reorder accidentally preferring the fallback.
func TestMapCHMetricsPrimaryKeyWins(t *testing.T) {
	got := mapCHMetrics(
		"ch-6:9000",
		map[string]float64{"PartsActive": 5},
		map[string]float64{},
		map[string]float64{
			"NumberOfActiveParts":  99,
			"OSUserTimeNormalized": 1.0,
			// virtual-time key would yield 1000s if (wrongly) preferred
			"OSCPUVirtualTimeMicroseconds": 1_000_000_000,
		},
	)
	if got.PartsActive != 5 {
		t.Errorf("PartsActive = %d, want 5 (primary system.metrics key must win)", got.PartsActive)
	}
	if !floatEq(got.OSCPUSeconds, 1.0) {
		t.Errorf("OSCPUSeconds = %v, want 1.0 (normalized key must win over µs fallback)", got.OSCPUSeconds)
	}
}

// TestMapCHMetricsNegativeClampedToZero asserts a defensively-handled negative
// reading (which uint64 conversion would otherwise wrap to a huge number) is
// clamped to 0 for the unsigned counter fields. asynchronous_metrics values
// are float64 and a transient negative is not impossible.
func TestMapCHMetricsNegativeClampedToZero(t *testing.T) {
	got := mapCHMetrics(
		"ch-7:9000",
		map[string]float64{"MemoryTracking": -1},
		map[string]float64{"Query": -5},
		map[string]float64{},
	)
	if got.MemoryTrackingBytes != 0 {
		t.Errorf("MemoryTrackingBytes = %d, want 0 (negative clamped)", got.MemoryTrackingBytes)
	}
	if got.QueryTotal != 0 {
		t.Errorf("QueryTotal = %d, want 0 (negative clamped)", got.QueryTotal)
	}
}
