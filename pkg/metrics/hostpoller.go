package metrics

import (
	"context"
	"fmt"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"

	"housegate/housegate/pkg/log"
)

// gopsutil entry points are indirected through package variables so tests can
// substitute fakes that exercise the per-field degradation and total-failure
// paths (a real darwin/linux host cannot be made to fail one subsystem on
// demand). Production code never reassigns these; they default to the real
// gopsutil functions and the public PollHost behaviour is unchanged.
var (
	cpuPercentFn  = cpu.PercentWithContext
	memVirtualFn  = mem.VirtualMemoryWithContext
	diskIOFn      = disk.IOCountersWithContext
	netIOFn       = net.IOCountersWithContext
)

// PollHost collects the host machine's CPU, memory, disk, and network metrics
// via gopsutil and returns them as a HostMetrics.
//
// Per-field degradation is the contract: each subsystem (cpu, mem, disk, net)
// is polled independently, and a failure in one subsystem logs a single warning
// via log.Warnw and leaves that subsystem's field at its zero value rather than
// failing the whole poll. This matters because the field set gopsutil can
// satisfy differs across platforms — a linux-prod box reports every block
// device while a restricted darwin-dev box may report none — and a partial host
// view is still useful to the exporter.
//
// PollHost returns an error only on total failure: when no subsystem produced
// any data. In that case the returned HostMetrics still has non-nil (empty)
// disk maps so a caller that ignores the error never nil-panics ranging over
// them.
//
// CPUPercent is sampled non-blocking (interval 0): gopsutil compares against
// the utilization captured on the previous PercentWithContext call, so the very
// first poll in a process can legitimately read 0. The value is summed across
// cores (percpu=false), matching the HostMetrics.CPUPercent doc.
func PollHost(ctx context.Context) (HostMetrics, error) {
	m := HostMetrics{
		// Always non-nil so consumers can range without a nil check, even when
		// the disk subsystem reports nothing or fails.
		DiskReadBytes:  map[string]uint64{},
		DiskWriteBytes: map[string]uint64{},
	}

	// Track per-subsystem success so we can distinguish "partial, degrade the
	// field" from "total failure, return an error".
	var anyOK bool

	// CPU: single summed utilization percentage, non-blocking sample.
	if pcts, err := cpuPercentFn(ctx, 0, false); err != nil {
		log.Warnw("host poller: cpu percent unavailable, leaving field zero", "err", err)
	} else if len(pcts) > 0 {
		m.CPUPercent = pcts[0]
		anyOK = true
	}

	// Memory: total and available bytes.
	if vm, err := memVirtualFn(ctx); err != nil {
		log.Warnw("host poller: virtual memory unavailable, leaving fields zero", "err", err)
	} else if vm != nil {
		m.MemTotalBytes = vm.Total
		m.MemAvailableBytes = vm.Available
		anyOK = true
	}

	// Disk: per-device cumulative read/write byte counters. The returned map is
	// keyed by device name, which is exactly the key shape HostMetrics wants.
	if counters, err := diskIOFn(ctx); err != nil {
		log.Warnw("host poller: disk io counters unavailable, leaving maps empty", "err", err)
	} else {
		for dev, c := range counters {
			m.DiskReadBytes[dev] = c.ReadBytes
			m.DiskWriteBytes[dev] = c.WriteBytes
		}
		// A successful call that simply reports no devices (common on
		// darwin-dev) still counts as the subsystem working.
		anyOK = true
	}

	// Network: aggregate rx/tx across all interfaces (pernic=false yields a
	// single rolled-up IOCountersStat).
	if stats, err := netIOFn(ctx, false); err != nil {
		log.Warnw("host poller: net io counters unavailable, leaving fields zero", "err", err)
	} else if len(stats) > 0 {
		m.NetRxBytes = stats[0].BytesRecv
		m.NetTxBytes = stats[0].BytesSent
		anyOK = true
	}

	if !anyOK {
		return m, fmt.Errorf("host poller: all subsystems failed")
	}
	return m, nil
}
