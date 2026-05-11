package log

import (
	"sync"
	"sync/atomic"
	"time"
)

// resolveLazyInPlace replaces each func() string / func() any in v with its
// evaluated result. The caller must own v (variadic-allocated slices qualify).
//
// Lazy values are only resolved by emit, which means they fire AFTER the
// level gate — invocations at a filtered level never invoke the function.
//
// slog.LogValuer values are left as-is; slog resolves them lazily inside the
// handler, which we also only reach when the level is enabled.
func resolveLazyInPlace(v []any) {
	for i, x := range v {
		switch f := x.(type) {
		case func() string:
			v[i] = f()
		case func() any:
			v[i] = f()
		}
	}
}

// Sampling state for *EveryN and *Every. Keyed by a caller-supplied id so
// dedup is explicit — runtime.Caller-based keys would couple call-site
// stability to logging behaviour, which is brittle.
var (
	countMu      sync.Mutex
	countSampler = map[string]*int64{}

	timeMu      sync.Mutex
	timeSampler = map[string]*int64{} // value = unix-nanos of last emission
)

// everyN reports true on the 1st, (n+1)th, (2n+1)th, ... calls for id.
// n <= 0 always returns true.
func everyN(id string, n int64) bool {
	if n <= 1 {
		return true
	}
	p := loadOrInitCounter(id)
	v := atomic.AddInt64(p, 1)
	return ((v - 1) % n) == 0
}

func loadOrInitCounter(id string) *int64 {
	countMu.Lock()
	p, ok := countSampler[id]
	if !ok {
		var zero int64
		p = &zero
		countSampler[id] = p
	}
	countMu.Unlock()
	return p
}

// every reports true if at least d has elapsed since the last true return
// for id (or this is the first call). d <= 0 always returns true.
func every(id string, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	p := loadOrInitTimestamp(id)
	now := time.Now().UnixNano()
	for {
		last := atomic.LoadInt64(p)
		if now-last < int64(d) {
			return false
		}
		if atomic.CompareAndSwapInt64(p, last, now) {
			return true
		}
	}
}

func loadOrInitTimestamp(id string) *int64 {
	timeMu.Lock()
	p, ok := timeSampler[id]
	if !ok {
		var zero int64
		p = &zero
		timeSampler[id] = p
	}
	timeMu.Unlock()
	return p
}

// resetSamplersForTest clears all sampling state. Test-only.
func resetSamplersForTest() {
	countMu.Lock()
	countSampler = map[string]*int64{}
	countMu.Unlock()
	timeMu.Lock()
	timeSampler = map[string]*int64{}
	timeMu.Unlock()
}
