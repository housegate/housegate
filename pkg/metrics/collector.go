package metrics

import (
	"context"
	"sync"
	"time"

	"housegate/housegate/pkg/log"
)

// defaultCollectInterval is used when NewCollector is given a non-positive
// interval.
const defaultCollectInterval = 15 * time.Second

// chReplicaPoller is the per-replica ClickHouse poll surface the Collector
// orchestrates. *CHPoller satisfies it; tests substitute fakes.
type chReplicaPoller interface {
	Poll(ctx context.Context) (CHReplicaMetrics, error)
}

// Collector is the background goroutine that periodically polls the ClickHouse
// replicas, the host, and the Go runtime, assembles one immutable Snapshot per
// cycle, and atomically publishes it to the Store every consumer reads.
//
// Collection is fail-open and panic-isolated: a failing or panicking source
// degrades only its own portion of the snapshot, never the others and never the
// process. The poll functions are struct fields so tests can inject fakes and a
// fixed clock.
type Collector struct {
	store     *Store
	ch        []chReplicaPoller
	hostFn    func(context.Context) (HostMetrics, error)
	runtimeFn func() RuntimeMetrics
	nowFn     func() time.Time
	interval  time.Duration
}

// NewCollector builds a Collector that polls the given CH replicas plus the
// local host and Go runtime every interval (a non-positive interval falls back
// to defaultCollectInterval).
func NewCollector(store *Store, ch []*CHPoller, interval time.Duration) *Collector {
	pollers := make([]chReplicaPoller, len(ch))
	for i, p := range ch {
		pollers[i] = p
	}
	if interval <= 0 {
		interval = defaultCollectInterval
	}
	return &Collector{
		store:     store,
		ch:        pollers,
		hostFn:    PollHost,
		runtimeFn: CollectRuntime,
		nowFn:     time.Now,
		interval:  interval,
	}
}

// Start runs the collection loop until ctx is cancelled. It mirrors the
// HealthChecker idiom (pkg/cluster/health.go): an initial pass, then a ticker
// loop, with a top-level recover so a bug in collection never takes down the
// process that hosts it.
func (c *Collector) Start(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("metrics collector Start panic recovered, marking collector down", "panic", r)
			// Surface the dead collector to scrapers: publishing a nil snapshot
			// makes the exporter emit collector_up=0 rather than leaving the last
			// snapshot in place (which would falsely keep reading collector_up=1).
			c.store.Store(nil)
		}
	}()

	c.collectOnce(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collectOnce(ctx)
		}
	}
}

// collectOnce polls every source concurrently, assembles a Snapshot, and
// publishes it. Each poll runs in its own goroutine with its own recover, so a
// panic in one source is contained. Every goroutine writes only its own slot or
// variable; the WaitGroup barrier establishes happens-before before the results
// are read, so no additional locking is needed.
func (c *Collector) collectOnce(ctx context.Context) {
	var wg sync.WaitGroup

	chOut := make([]CHReplicaMetrics, len(c.ch))
	for i, p := range c.ch {
		wg.Add(1)
		go func(i int, p chReplicaPoller) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Errorw("metrics collector ch poll panic recovered", "panic", r)
				}
			}()
			// Poll is fail-open: on error it returns the replica label with
			// Reachable=false, which is exactly what we want to store.
			m, _ := p.Poll(ctx)
			chOut[i] = m
		}(i, p)
	}

	// host/hostOK (and runtime below) are closed over by their goroutines and
	// read by the parent only after wg.Wait() — that barrier is the
	// happens-before edge that makes these single-writer closures race-free. Do
	// not remove the barrier (e.g. for per-source timeouts) without moving each
	// result into its own goroutine-local storage first.
	var (
		host   HostMetrics
		hostOK bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Errorw("metrics collector host poll panic recovered", "panic", r)
			}
		}()
		h, err := c.hostFn(ctx)
		host = h
		hostOK = err == nil
	}()

	var runtime RuntimeMetrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Errorw("metrics collector runtime poll panic recovered", "panic", r)
			}
		}()
		runtime = c.runtimeFn()
	}()

	wg.Wait()

	now := c.nowFn()

	// LastSuccess carries forward the previous cycle's freshness, advancing only
	// the sources that succeeded this tick. CollectRuntime cannot fail, so
	// "runtime" always advances.
	last := make(map[string]time.Time)
	if prev := c.store.Load(); prev != nil {
		for k, v := range prev.LastSuccess {
			last[k] = v
		}
	}
	if hostOK {
		last["host"] = now
	}
	last["runtime"] = now
	for _, m := range chOut {
		if m.Reachable {
			last["ch:"+m.Replica] = now
		}
	}

	c.store.Store(&Snapshot{
		CapturedAt:  now,
		CH:          chOut,
		Host:        host,
		Runtime:     runtime,
		LastSuccess: last,
	})
}
