package metrics

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// makeSnapshot builds a snapshot whose fields are all derived from a single
// seed. Every internally-consistent snapshot satisfies seed == int64 of every
// numeric field that is keyed off it, so a reader that observes a torn write
// (some fields from seed A, some from seed B) can detect the inconsistency.
func makeSnapshot(seed int64) *Snapshot {
	u := uint64(seed)
	return &Snapshot{
		CapturedAt: time.Unix(seed, 0),
		CH: []CHReplicaMetrics{
			{
				Replica:             "replica-0",
				Reachable:           true,
				QueryTotal:          u,
				InsertTotal:         u,
				MemoryTrackingBytes: u,
				PartsActive:         u,
				ReplicationQueue:    u,
				OSCPUSeconds:        float64(seed),
			},
		},
		Host: HostMetrics{
			CPUPercent:        float64(seed),
			MemAvailableBytes: u,
			MemTotalBytes:     u,
			DiskReadBytes:     map[string]uint64{"disk0": u},
			DiskWriteBytes:    map[string]uint64{"disk0": u},
			NetRxBytes:        u,
			NetTxBytes:        u,
		},
		Runtime: RuntimeMetrics{
			Goroutines:     int(seed),
			HeapAllocBytes: u,
			GCPauseSeconds: float64(seed),
		},
		LastSuccess: map[string]time.Time{"ch": time.Unix(seed, 0)},
	}
}

// consistent reports whether every seed-derived field of s agrees on the same
// seed. A partially-written (torn) snapshot would fail this check.
func consistent(s *Snapshot) bool {
	seed := s.CapturedAt.Unix()
	u := uint64(seed)
	if len(s.CH) != 1 {
		return false
	}
	ch := s.CH[0]
	if ch.QueryTotal != u || ch.InsertTotal != u || ch.MemoryTrackingBytes != u ||
		ch.PartsActive != u || ch.ReplicationQueue != u || ch.OSCPUSeconds != float64(seed) {
		return false
	}
	if s.Host.MemAvailableBytes != u || s.Host.MemTotalBytes != u ||
		s.Host.NetRxBytes != u || s.Host.NetTxBytes != u ||
		s.Host.CPUPercent != float64(seed) {
		return false
	}
	if s.Host.DiskReadBytes["disk0"] != u || s.Host.DiskWriteBytes["disk0"] != u {
		return false
	}
	if s.Runtime.Goroutines != int(seed) || s.Runtime.HeapAllocBytes != u ||
		s.Runtime.GCPauseSeconds != float64(seed) {
		return false
	}
	if s.LastSuccess["ch"].Unix() != seed {
		return false
	}
	return true
}

// TestStoreLoadNilBeforePublish asserts Load returns nil (and does not panic)
// before any Store call.
func TestStoreLoadNilBeforePublish(t *testing.T) {
	var s Store
	if got := s.Load(); got != nil {
		t.Fatalf("Load() before Store() = %v, want nil", got)
	}
}

// TestStoreLoadRoundTrip asserts Store then Load returns the exact pointer that
// was published, with its values intact.
func TestStoreLoadRoundTrip(t *testing.T) {
	var s Store
	snap := makeSnapshot(42)
	s.Store(snap)

	got := s.Load()
	if got != snap {
		t.Fatalf("Load() = %p, want the stored pointer %p", got, snap)
	}
	if !consistent(got) {
		t.Fatalf("round-tripped snapshot is not internally consistent")
	}
	if got.CapturedAt.Unix() != 42 {
		t.Fatalf("CapturedAt = %d, want 42", got.CapturedAt.Unix())
	}

	// A second publish replaces the previous snapshot.
	snap2 := makeSnapshot(99)
	s.Store(snap2)
	if got2 := s.Load(); got2 != snap2 {
		t.Fatalf("after second Store, Load() = %p, want %p", got2, snap2)
	}
}

// TestStoreConcurrentLoadStore runs N writers and M readers against one Store
// under the race detector. It asserts no data race (enforced by -race) and that
// a reader never observes a torn/partial snapshot: every snapshot a reader
// loads must be internally consistent. Run with --@rules_go//go/config:race.
func TestStoreConcurrentLoadStore(t *testing.T) {
	var s Store
	// Seed an initial snapshot so readers that start before the first writer
	// publishes still have a consistent value to check (and we also exercise
	// the nil path below).
	s.Store(makeSnapshot(0))

	const (
		writers    = 8
		readers    = 8
		writesEach = 2000
		readsEach  = 2000
	)

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for w := 0; w < writers; w++ {
		go func(base int64) {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				// Use a non-zero, writer-disjoint seed so different writers
				// publish distinct-but-consistent snapshots.
				s.Store(makeSnapshot(base*int64(writesEach) + int64(i) + 1))
				runtime.Gosched()
			}
		}(int64(w))
	}

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < readsEach; i++ {
				snap := s.Load()
				if snap == nil {
					// Never expected here because we pre-published, but Load
					// returning nil must never panic the reader.
					continue
				}
				if !consistent(snap) {
					t.Errorf("reader observed a torn snapshot at CapturedAt=%d", snap.CapturedAt.Unix())
					return
				}
				runtime.Gosched()
			}
		}()
	}

	wg.Wait()
}
