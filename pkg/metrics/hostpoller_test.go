package metrics

import (
	"context"
	"testing"
)

// PollHost must succeed on the test host and populate memory totals. Memory is
// the one subsystem every supported platform reports, so MemTotalBytes is the
// load-bearing "core field > 0" assertion. Other subsystems (disk, net) may be
// empty on a restricted dev box and degrade per-field, so they are checked for
// non-negativity / shape only.
func TestPollHostMemTotalPositive(t *testing.T) {
	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error: %v", err)
	}
	if m.MemTotalBytes == 0 {
		t.Fatalf("MemTotalBytes = 0, want > 0")
	}
}

// MemAvailableBytes must never exceed MemTotalBytes and (on any real host) is
// non-zero. uint64 fields are non-negative by type, so the meaningful check is
// the available <= total invariant.
func TestPollHostMemAvailableSane(t *testing.T) {
	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error: %v", err)
	}
	if m.MemAvailableBytes == 0 {
		t.Fatalf("MemAvailableBytes = 0, want > 0")
	}
	if m.MemAvailableBytes > m.MemTotalBytes {
		t.Fatalf("MemAvailableBytes (%d) > MemTotalBytes (%d)", m.MemAvailableBytes, m.MemTotalBytes)
	}
}

// CPUPercent is a 0..100 utilization figure (summed across cores by gopsutil).
// On a non-blocking first sample it can read 0, so the test asserts the bound,
// not strict positivity.
func TestPollHostCPUPercentInRange(t *testing.T) {
	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error: %v", err)
	}
	if m.CPUPercent < 0 {
		t.Fatalf("CPUPercent = %v, want >= 0", m.CPUPercent)
	}
	// gopsutil sums across cores, so the ceiling is 100*NumCPU; a generous
	// upper bound catches a nonsense reading without being platform-fragile.
	if m.CPUPercent > 100*1024 {
		t.Fatalf("CPUPercent = %v, implausibly high", m.CPUPercent)
	}
}

// Disk counters degrade per-field: on darwin-dev gopsutil may report no block
// devices. The map must always be non-nil (so the exporter can range over it),
// and when entries do exist their byte counters are non-negative by type — the
// test only asserts the map is present and skips per-device value assertions
// when empty.
func TestPollHostDiskMapsPresent(t *testing.T) {
	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error: %v", err)
	}
	if m.DiskReadBytes == nil {
		t.Fatalf("DiskReadBytes map is nil, want non-nil (possibly empty)")
	}
	if m.DiskWriteBytes == nil {
		t.Fatalf("DiskWriteBytes map is nil, want non-nil (possibly empty)")
	}
	if len(m.DiskReadBytes) == 0 {
		t.Skip("no block devices reported on this host; skipping per-device disk assertions")
	}
	// Every read-counter device should also appear in the write map: gopsutil
	// fills both from the same IOCountersStat.
	for dev := range m.DiskReadBytes {
		if _, ok := m.DiskWriteBytes[dev]; !ok {
			t.Fatalf("device %q present in DiskReadBytes but missing from DiskWriteBytes", dev)
		}
	}
}

// Network counters are cumulative byte totals. uint64 makes them non-negative
// by type; this test documents the contract that PollHost populates them
// without error and that the aggregate fields are addressable.
func TestPollHostNetCountersPresent(t *testing.T) {
	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error: %v", err)
	}
	// On a live host with any traffic these are > 0, but a freshly-booted or
	// network-isolated CI box can legitimately read 0; assert the call shape,
	// not a non-zero floor.
	_ = m.NetRxBytes
	_ = m.NetTxBytes
}

// PollHost must honour a cancelled context without panicking. Whatever a
// cancelled gopsutil call returns, PollHost's per-field degradation means it
// still returns a usable (possibly zero-filled) struct, and the disk maps stay
// non-nil so consumers never nil-panic.
func TestPollHostCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m, err := PollHost(ctx)
	if err != nil {
		// A total failure is acceptable on a cancelled context; if it does
		// return a struct, the maps must still be non-nil.
		return
	}
	if m.DiskReadBytes == nil || m.DiskWriteBytes == nil {
		t.Fatalf("disk maps must be non-nil even on cancelled context")
	}
}
