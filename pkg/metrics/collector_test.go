package metrics

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeCHPoller is a chReplicaPoller test double. It mirrors the real
// CHPoller.Poll fail-open contract: on error it still returns the replica label
// with Reachable=false.
type fakeCHPoller struct {
	replica string
	result  CHReplicaMetrics
	err     error
	panics  bool
}

func (f *fakeCHPoller) Poll(ctx context.Context) (CHReplicaMetrics, error) {
	if f.panics {
		panic("ch poller boom")
	}
	if f.err != nil {
		return CHReplicaMetrics{Replica: f.replica, Reachable: false}, f.err
	}
	return f.result, nil
}

// newTestCollector builds a Collector with injected pollers and a fixed clock,
// bypassing NewCollector so tests need no real CH/host.
func newTestCollector(store *Store, ch []chReplicaPoller, host func(context.Context) (HostMetrics, error), rt func() RuntimeMetrics, now time.Time) *Collector {
	return &Collector{
		store:     store,
		ch:        ch,
		hostFn:    host,
		runtimeFn: rt,
		nowFn:     func() time.Time { return now },
		interval:  time.Hour,
	}
}

func okHost(h HostMetrics) func(context.Context) (HostMetrics, error) {
	return func(context.Context) (HostMetrics, error) { return h, nil }
}

func TestCollectOnceAssemblesSnapshot(t *testing.T) {
	store := &Store{}
	now := time.Unix(1000, 0)
	ch := []chReplicaPoller{
		&fakeCHPoller{replica: "a:9000", result: CHReplicaMetrics{Replica: "a:9000", Reachable: true, MemoryTrackingBytes: 10}},
		&fakeCHPoller{replica: "b:9000", err: errors.New("down")},
	}
	c := newTestCollector(store, ch, okHost(HostMetrics{CPUPercent: 5, MemTotalBytes: 100}), func() RuntimeMetrics { return RuntimeMetrics{Goroutines: 7} }, now)

	c.collectOnce(context.Background())

	snap := store.Load()
	if snap == nil {
		t.Fatal("no snapshot published")
	}
	if !snap.CapturedAt.Equal(now) {
		t.Errorf("CapturedAt = %v, want %v", snap.CapturedAt, now)
	}
	if len(snap.CH) != 2 {
		t.Fatalf("CH len = %d, want 2", len(snap.CH))
	}
	var a, b CHReplicaMetrics
	for _, m := range snap.CH {
		switch m.Replica {
		case "a:9000":
			a = m
		case "b:9000":
			b = m
		}
	}
	if !a.Reachable || a.MemoryTrackingBytes != 10 {
		t.Errorf("replica a = %+v", a)
	}
	if b.Reachable {
		t.Errorf("replica b should be unreachable: %+v", b)
	}
	if snap.Host.CPUPercent != 5 || snap.Runtime.Goroutines != 7 {
		t.Errorf("host/runtime not populated: host=%+v runtime=%+v", snap.Host, snap.Runtime)
	}
	if !snap.LastSuccess["host"].Equal(now) || !snap.LastSuccess["runtime"].Equal(now) {
		t.Errorf("host/runtime LastSuccess not advanced: %v", snap.LastSuccess)
	}
	if !snap.LastSuccess["ch:a:9000"].Equal(now) {
		t.Error("reachable replica LastSuccess not advanced")
	}
	if _, ok := snap.LastSuccess["ch:b:9000"]; ok {
		t.Error("unreachable replica must not advance LastSuccess")
	}
}

func TestCollectOnceHostError(t *testing.T) {
	store := &Store{}
	now := time.Unix(2000, 0)
	host := func(context.Context) (HostMetrics, error) { return HostMetrics{}, errors.New("proc fail") }
	c := newTestCollector(store, nil, host, func() RuntimeMetrics { return RuntimeMetrics{Goroutines: 3} }, now)

	c.collectOnce(context.Background())

	snap := store.Load()
	if snap == nil {
		t.Fatal("no snapshot published despite host error (fail-open)")
	}
	if snap.Runtime.Goroutines != 3 {
		t.Error("runtime not populated despite host error")
	}
	if _, ok := snap.LastSuccess["host"]; ok {
		t.Error("host LastSuccess must not advance on host error")
	}
	if !snap.LastSuccess["runtime"].Equal(now) {
		t.Error("runtime LastSuccess should advance")
	}
}

func TestCollectOnceLastSuccessCarryForward(t *testing.T) {
	store := &Store{}
	oldT := time.Unix(500, 0)
	store.Store(&Snapshot{LastSuccess: map[string]time.Time{"host": oldT}})

	// Tick 1: host fails — old host time must carry forward.
	now1 := time.Unix(3000, 0)
	failHost := func(context.Context) (HostMetrics, error) { return HostMetrics{}, errors.New("fail") }
	newTestCollector(store, nil, failHost, func() RuntimeMetrics { return RuntimeMetrics{} }, now1).collectOnce(context.Background())
	if got := store.Load().LastSuccess["host"]; !got.Equal(oldT) {
		t.Errorf("host LastSuccess = %v, want carried-forward %v", got, oldT)
	}

	// Tick 2: host succeeds — advances.
	now2 := time.Unix(4000, 0)
	newTestCollector(store, nil, okHost(HostMetrics{}), func() RuntimeMetrics { return RuntimeMetrics{} }, now2).collectOnce(context.Background())
	if got := store.Load().LastSuccess["host"]; !got.Equal(now2) {
		t.Errorf("host LastSuccess = %v, want advanced %v", got, now2)
	}
}

func TestCollectOncePanicRecovered(t *testing.T) {
	store := &Store{}
	now := time.Unix(5000, 0)
	ch := []chReplicaPoller{{}, &fakeCHPoller{replica: "x:9000", panics: true}}[1:]
	c := newTestCollector(store, ch, okHost(HostMetrics{MemTotalBytes: 1}), func() RuntimeMetrics { return RuntimeMetrics{Goroutines: 1} }, now)

	// Must not crash the test process.
	c.collectOnce(context.Background())

	snap := store.Load()
	if snap == nil {
		t.Fatal("snapshot should still publish after a poller panic")
	}
	if snap.Host.MemTotalBytes != 1 {
		t.Error("host should still be collected when a CH poller panics")
	}
}

func TestStartPublishesThenStops(t *testing.T) {
	store := &Store{}
	c := &Collector{
		store:     store,
		hostFn:    okHost(HostMetrics{MemTotalBytes: 1}),
		runtimeFn: func() RuntimeMetrics { return RuntimeMetrics{Goroutines: 1} },
		nowFn:     time.Now,
		interval:  10 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Start(ctx); close(done) }()

	deadline := time.After(2 * time.Second)
	for store.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("no initial snapshot from Start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}

func TestNewCollectorWiresDefaults(t *testing.T) {
	store := &Store{}
	c := NewCollector(store, nil, 0)
	if c.interval != defaultCollectInterval {
		t.Errorf("interval = %v, want default %v", c.interval, defaultCollectInterval)
	}
	if c.hostFn == nil || c.runtimeFn == nil || c.nowFn == nil {
		t.Error("NewCollector did not wire default funcs")
	}

	p, err := NewCHPoller("127.0.0.1:9000", "db", "u", "pw", time.Second)
	if err != nil {
		t.Fatalf("NewCHPoller: %v", err)
	}
	defer func() { _ = p.Close() }()
	c2 := NewCollector(store, []*CHPoller{p}, time.Minute)
	if len(c2.ch) != 1 {
		t.Errorf("NewCollector wrapped %d CH pollers, want 1", len(c2.ch))
	}
}
