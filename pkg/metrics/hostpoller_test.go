package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
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

// errBoom is the canned failure injected through the gopsutil seam. A real
// host cannot be coerced into failing exactly one subsystem on demand, so the
// degradation and total-failure contracts are only reachable via the package
// vars cpuPercentFn / memVirtualFn / diskIOFn / netIOFn.
var errBoom = errors.New("boom")

// Known-good fakes for each subsystem. The per-subsystem error tests install
// these for the three subsystems that should keep working, so the assertion is
// deterministic instead of depending on the host actually satisfying cpu/mem/
// disk/net while one injected fake fails.
func okCPU(context.Context, time.Duration, bool) ([]float64, error) {
	return []float64{12.5}, nil
}
func okMem(context.Context) (*mem.VirtualMemoryStat, error) {
	return &mem.VirtualMemoryStat{Total: 2048, Available: 1024}, nil
}
func okDisk(context.Context, ...string) (map[string]disk.IOCountersStat, error) {
	return map[string]disk.IOCountersStat{"sda": {ReadBytes: 100, WriteBytes: 200}}, nil
}
func okNet(context.Context, bool) ([]net.IOCountersStat, error) {
	return []net.IOCountersStat{{BytesRecv: 7, BytesSent: 9}}, nil
}

// installAllOKFns points all four seams at the known-good fakes and returns a
// restore closure. Each error-branch test starts from this fully-working
// baseline, then overrides exactly one seam with a failing fake, so the test
// isolates a single degradation branch and proves PollHost still returns nil
// because the other three subsystems succeeded.
func installAllOKFns() func() {
	origCPU, origMem, origDisk, origNet := cpuPercentFn, memVirtualFn, diskIOFn, netIOFn
	cpuPercentFn, memVirtualFn, diskIOFn, netIOFn = okCPU, okMem, okDisk, okNet
	return func() {
		cpuPercentFn, memVirtualFn, diskIOFn, netIOFn = origCPU, origMem, origDisk, origNet
	}
}

// A failing CPU subsystem must degrade to CPUPercent == 0 while the other three
// subsystems keep PollHost's error nil (anyOK stays true). This covers the
// cpu log.Warnw branch in hostpoller.go.
func TestPollHostCPUDegrades(t *testing.T) {
	defer installAllOKFns()()
	cpuPercentFn = func(context.Context, time.Duration, bool) ([]float64, error) {
		return nil, errBoom
	}

	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error on cpu-only failure: %v", err)
	}
	if m.CPUPercent != 0 {
		t.Fatalf("CPUPercent = %v, want 0 when cpu subsystem fails", m.CPUPercent)
	}
	// The surviving subsystems must still have populated their fields.
	if m.MemTotalBytes != 2048 {
		t.Fatalf("MemTotalBytes = %d, want 2048 (mem subsystem should still work)", m.MemTotalBytes)
	}
}

// A failing memory subsystem must leave MemTotalBytes / MemAvailableBytes at
// zero while PollHost stays nil. Covers the mem log.Warnw branch.
func TestPollHostMemDegrades(t *testing.T) {
	defer installAllOKFns()()
	memVirtualFn = func(context.Context) (*mem.VirtualMemoryStat, error) {
		return nil, errBoom
	}

	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error on mem-only failure: %v", err)
	}
	if m.MemTotalBytes != 0 || m.MemAvailableBytes != 0 {
		t.Fatalf("mem fields = (%d, %d), want (0, 0) when mem subsystem fails",
			m.MemTotalBytes, m.MemAvailableBytes)
	}
	if m.CPUPercent != 12.5 {
		t.Fatalf("CPUPercent = %v, want 12.5 (cpu subsystem should still work)", m.CPUPercent)
	}
}

// A failing disk subsystem must leave the disk maps non-nil but empty while
// PollHost stays nil. Covers the disk log.Warnw branch and the empty-map
// contract under degradation.
func TestPollHostDiskDegrades(t *testing.T) {
	defer installAllOKFns()()
	diskIOFn = func(context.Context, ...string) (map[string]disk.IOCountersStat, error) {
		return nil, errBoom
	}

	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error on disk-only failure: %v", err)
	}
	if m.DiskReadBytes == nil || m.DiskWriteBytes == nil {
		t.Fatalf("disk maps must stay non-nil when disk subsystem fails")
	}
	if len(m.DiskReadBytes) != 0 || len(m.DiskWriteBytes) != 0 {
		t.Fatalf("disk maps = (%d, %d entries), want empty when disk subsystem fails",
			len(m.DiskReadBytes), len(m.DiskWriteBytes))
	}
}

// A failing network subsystem must leave NetRxBytes / NetTxBytes at zero while
// PollHost stays nil. Covers the net log.Warnw branch.
func TestPollHostNetDegrades(t *testing.T) {
	defer installAllOKFns()()
	netIOFn = func(context.Context, bool) ([]net.IOCountersStat, error) {
		return nil, errBoom
	}

	m, err := PollHost(context.Background())
	if err != nil {
		t.Fatalf("PollHost returned error on net-only failure: %v", err)
	}
	if m.NetRxBytes != 0 || m.NetTxBytes != 0 {
		t.Fatalf("net fields = (%d, %d), want (0, 0) when net subsystem fails",
			m.NetRxBytes, m.NetTxBytes)
	}
}

// When every subsystem fails, anyOK stays false: PollHost must return a non-nil
// error, and — critically — the returned HostMetrics must still carry non-nil
// (empty) disk maps so a caller that ignores the error never nil-panics ranging
// over them. Covers the `if !anyOK { return ... }` total-failure branch.
func TestPollHostTotalFailure(t *testing.T) {
	defer installAllOKFns()()
	cpuPercentFn = func(context.Context, time.Duration, bool) ([]float64, error) {
		return nil, errBoom
	}
	memVirtualFn = func(context.Context) (*mem.VirtualMemoryStat, error) {
		return nil, errBoom
	}
	diskIOFn = func(context.Context, ...string) (map[string]disk.IOCountersStat, error) {
		return nil, errBoom
	}
	netIOFn = func(context.Context, bool) ([]net.IOCountersStat, error) {
		return nil, errBoom
	}

	m, err := PollHost(context.Background())
	if err == nil {
		t.Fatalf("PollHost returned nil error when all subsystems failed, want non-nil")
	}
	if m.DiskReadBytes == nil || m.DiskWriteBytes == nil {
		t.Fatalf("disk maps must be non-nil even on total failure (no nil-panic contract)")
	}
}
