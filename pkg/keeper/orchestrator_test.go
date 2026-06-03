package keeper

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

// fakeReconfigurer captures IncrementalReconfig calls so tests can assert
// the orchestrator built the right add/remove strings in the right order.
type fakeReconfigurer struct {
	mu     sync.Mutex
	calls  []reconfigCall
	closed bool
}

type reconfigCall struct {
	joining []string
	leaving []string
}

func (f *fakeReconfigurer) IncrementalReconfig(joining, leaving []string, _ int64) (*zk.Stat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := reconfigCall{
		joining: append([]string(nil), joining...),
		leaving: append([]string(nil), leaving...),
	}
	f.calls = append(f.calls, cp)
	return &zk.Stat{}, nil
}
func (f *fakeReconfigurer) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeReconfigurer) snapshot() ([]reconfigCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reconfigCall(nil), f.calls...), f.closed
}

func TestOrchestrator_NoOpWhenConverged(t *testing.T) {
	ctx := context.Background()
	a := newFakeKeeper(t, StateLeader)
	b := newFakeKeeper(t, StateFollower)
	tr := NewTracker(TrackerConfig{Members: []string{a.addr(), b.addr()}, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)

	fake := &fakeReconfigurer{}
	o, err := NewOrchestrator(OrchestratorConfig{
		Shard:          "default",
		Expected:       func() []string { return []string{a.addr(), b.addr()} },
		CurrentMembers: func(context.Context) []string { return []string{a.addr(), b.addr()} },
		Tracker:        tr,
		Dial:           func(context.Context, []string) (Reconfigurer, error) { return fake, nil },
		ServerIDFor:    func(string) (int, bool) { return 1, true },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := o.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if calls, _ := fake.snapshot(); len(calls) != 0 {
		t.Fatalf("converged Reconcile must not issue reconfig calls, got %d", len(calls))
	}
	if o.Reconfigs() != 0 {
		t.Errorf("Reconfigs counter = %d, want 0", o.Reconfigs())
	}
}

func TestOrchestrator_AddThenRemove(t *testing.T) {
	ctx := context.Background()
	a := newFakeKeeper(t, StateLeader)
	b := newFakeKeeper(t, StateFollower)
	c := newFakeKeeper(t, StateFollower) // will join
	tr := NewTracker(TrackerConfig{Members: []string{a.addr(), b.addr()}, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)

	idMap := map[string]int{a.addr(): 1, b.addr(): 2, c.addr(): 3}

	fake := &fakeReconfigurer{}
	o, err := NewOrchestrator(OrchestratorConfig{
		Shard:    "default",
		Expected: func() []string { return []string{a.addr(), c.addr()} }, // drop b, add c
		// "Current" is the quorum's view of itself: [a, b]. Diff vs
		// Expected → add c, remove b.
		CurrentMembers: func(context.Context) []string { return []string{a.addr(), b.addr()} },
		Tracker:        tr,
		Dial:           func(context.Context, []string) (Reconfigurer, error) { return fake, nil },
		ServerIDFor:    func(m string) (int, bool) { id, ok := idMap[m]; return id, ok },
		LgifTimeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Simulate the new member coming online during the lgif wait: after
	// 100ms, register c in the tracker so the next ProbeOnce sees it
	// alive. (Until then it's just an unknown address.)
	go func() {
		time.Sleep(100 * time.Millisecond)
		tr.SetMembers([]string{a.addr(), b.addr(), c.addr()})
	}()

	if err := o.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	calls, closed := fake.snapshot()
	if !closed {
		t.Error("conn.Close() was not called after reconcile")
	}
	// Two calls: first the join (one member), then the remove. Architecture
	// §4 ordering: add before remove.
	if len(calls) != 2 {
		t.Fatalf("Reconfig calls = %d, want 2 (add then remove); calls=%+v", len(calls), calls)
	}
	if len(calls[0].joining) != 1 || len(calls[0].leaving) != 0 {
		t.Fatalf("first call must be add-only, got %+v", calls[0])
	}
	wantJoin := "server.3=" + hostOf(c.addr()) + ":9234;participant;1"
	if calls[0].joining[0] != wantJoin {
		t.Errorf("first add joining = %q, want %q", calls[0].joining[0], wantJoin)
	}
	if len(calls[1].joining) != 0 || len(calls[1].leaving) != 1 || calls[1].leaving[0] != "2" {
		t.Fatalf("second call must be remove-only of id 2, got %+v", calls[1])
	}
	if o.Adds() != 1 || o.Removes() != 1 {
		t.Errorf("counters: adds=%d removes=%d, want 1/1", o.Adds(), o.Removes())
	}
}

func TestOrchestrator_NoLiveQuorumErrors(t *testing.T) {
	ctx := context.Background()
	a := newFakeKeeper(t, StateLeader)
	tr := NewTracker(TrackerConfig{Members: []string{a.addr()}, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)
	a.close() // now nothing's alive
	tr.ProbeOnce(ctx)

	fake := &fakeReconfigurer{}
	o, _ := NewOrchestrator(OrchestratorConfig{
		Shard:          "default",
		Expected:       func() []string { return []string{a.addr(), "fresh:9181"} },
		CurrentMembers: func(context.Context) []string { return []string{a.addr()} }, // need a remove
		Tracker:        tr,
		Dial:           func(context.Context, []string) (Reconfigurer, error) { return fake, nil },
		ServerIDFor:    func(string) (int, bool) { return 1, true },
	})
	err := o.Reconcile(ctx)
	if err == nil {
		t.Fatal("expected error when no live members (§5 territory)")
	}
}

func TestOrchestrator_EmptyExpectedNoOp(t *testing.T) {
	ctx := context.Background()
	a := newFakeKeeper(t, StateLeader)
	tr := NewTracker(TrackerConfig{Members: []string{a.addr()}, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)

	fake := &fakeReconfigurer{}
	o, _ := NewOrchestrator(OrchestratorConfig{
		Shard:          "default",
		Expected:       func() []string { return nil }, // "unknown"
		CurrentMembers: func(context.Context) []string { return []string{a.addr()} },
		Tracker:        tr,
		Dial:           func(context.Context, []string) (Reconfigurer, error) { return fake, nil },
		ServerIDFor:    func(string) (int, bool) { return 1, true },
	})
	if err := o.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile must no-op on empty expected, got: %v", err)
	}
	if calls, _ := fake.snapshot(); len(calls) != 0 {
		t.Errorf("empty expected must not issue reconfig, got %d calls", len(calls))
	}
}

func hostOf(addr string) string {
	for i, c := range addr {
		if c == ':' {
			return addr[:i]
		}
	}
	return addr
}
