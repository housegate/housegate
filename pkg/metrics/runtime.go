package metrics

import "runtime"

// CollectRuntime captures the current Go-runtime metrics for this housegate
// process and returns them as a RuntimeMetrics. It is a pure function: it reads
// process-global runtime state and allocates nothing observable to the caller
// beyond the returned value, so it is safe to call from any goroutine and on
// every collection cycle.
//
// HeapAllocBytes and GCPauseSeconds come from runtime.ReadMemStats. That call
// performs a brief stop-the-world to obtain a consistent snapshot, which is
// acceptable at the once-per-cycle cadence the collector uses; callers should
// not invoke it in a hot loop. PauseTotalNs is a cumulative counter of all
// stop-the-world GC pauses since process start; it is reported here in seconds
// to match the exporter's counter units.
func CollectRuntime() RuntimeMetrics {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return RuntimeMetrics{
		Goroutines:     runtime.NumGoroutine(),
		HeapAllocBytes: ms.HeapAlloc,
		// PauseTotalNs is nanoseconds; convert to seconds for the exporter.
		GCPauseSeconds: float64(ms.PauseTotalNs) / 1e9,
	}
}
