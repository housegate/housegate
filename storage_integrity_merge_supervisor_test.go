package housegate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// controllableMergeGuard answers every requested id with its configured
// per-table error (nil = healthy) and records each pass's requested ids.
type controllableMergeGuard struct {
	mu        sync.Mutex
	tableErrs map[string]error
	globalErr error
	passes    [][]string
	called    chan struct{}
}

func (g *controllableMergeGuard) AssertTables(_ context.Context, ids []string) (sicore.MergeGuardReport, error) {
	g.mu.Lock()
	g.passes = append(g.passes, append([]string(nil), ids...))
	report := sicore.MergeGuardReport{Tables: map[string]error{}}
	for _, id := range ids {
		report.Tables[id] = g.tableErrs[id]
	}
	err := g.globalErr
	g.mu.Unlock()
	if g.called != nil {
		select {
		case g.called <- struct{}{}:
		default:
		}
	}
	return report, err
}

func (g *controllableMergeGuard) set(id string, err error) {
	g.mu.Lock()
	if g.tableErrs == nil {
		g.tableErrs = map[string]error{}
	}
	g.tableErrs[id] = err
	g.mu.Unlock()
}

func active(ids ...string) []sitable.Table {
	out := make([]sitable.Table, 0, len(ids))
	for _, id := range ids {
		out = append(out, sitable.Table{ID: id, Status: sitable.Active})
	}
	return out
}

func TestMergeSupervisorStartsClosedUntilAssertSucceeds(t *testing.T) {
	guard := &controllableMergeGuard{}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.t")...), time.Second)
	if err := supervisor.CheckMergeHealth("db1.t"); err == nil || !strings.Contains(err.Error(), "not asserted") {
		t.Fatalf("initial health err = %v, want not asserted", err)
	}
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatalf("Assert: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.t"); err != nil {
		t.Fatalf("health after successful assert: %v", err)
	}
}

// TestMergeSupervisorHealthIsPerTable is spec 2026-09-24 §9.4: one unready
// table blocks only itself.
func TestMergeSupervisorHealthIsPerTable(t *testing.T) {
	guard := &controllableMergeGuard{}
	guard.set("db1.bad", sicore.ErrMergeGuardTableMissing)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.good", "db1.bad")...), time.Second)
	err := supervisor.Assert(context.Background())
	if err == nil || !strings.Contains(err.Error(), "db1.bad") || strings.Contains(err.Error(), "db1.good") {
		t.Fatalf("Assert err = %v, want only db1.bad reported", err)
	}
	if err := supervisor.CheckMergeHealth("db1.good"); err != nil {
		t.Fatalf("a healthy table must admit while another is unready: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.bad"); !errors.Is(err, sicore.ErrMergeGuardTableMissing) {
		t.Fatalf("db1.bad health = %v, want ErrMergeGuardTableMissing", err)
	}
	if got := strings.Join(supervisor.Unhealthy(), ","); got != "db1.bad" {
		t.Fatalf("Unhealthy = %q", got)
	}
}

func TestMergeSupervisorGlobalFailureClosesEveryTable(t *testing.T) {
	guard := &controllableMergeGuard{globalErr: errors.New("clickhouse reconnect failed")}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.a", "db1.b")...), time.Second)
	if err := supervisor.Assert(context.Background()); err == nil {
		t.Fatal("a global failure must fail the pass")
	}
	for _, id := range []string{"db1.a", "db1.b"} {
		if err := supervisor.CheckMergeHealth(id); err == nil || !strings.Contains(err.Error(), "clickhouse reconnect failed") {
			t.Fatalf("%s health = %v, want the global failure", id, err)
		}
	}
	guard.mu.Lock()
	guard.globalErr = nil
	guard.mu.Unlock()
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatalf("recovery Assert: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.a"); err != nil {
		t.Fatalf("health after recovery: %v", err)
	}
}

func TestMergeSupervisorRunPeriodicallyReasserts(t *testing.T) {
	called := make(chan struct{}, 1)
	guard := &controllableMergeGuard{called: called}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.t")...), time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("periodic reassert did not run")
	}
}

// TestMergeSupervisorAssertsImmediatelyOnChange is spec 2026-09-24 §9.4: a
// newly Active table is asserted on Changed(), not after reassert_interval.
func TestMergeSupervisorAssertsImmediatelyOnChange(t *testing.T) {
	called := make(chan struct{}, 4)
	guard := &controllableMergeGuard{called: called}
	fake := sitable.NewFake(sitable.Ordinary, active("db1.t")...)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, fake, time.Hour)
	if err := supervisor.Assert(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-called
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)
	if err := supervisor.CheckMergeHealth("db1.new"); err == nil {
		t.Fatal("a table no pass has asserted must fail closed")
	}
	fake.Set(active("db1.t", "db1.new")...)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("a table-state change did not trigger a reassert")
	}
	deadline := time.Now().Add(time.Second)
	for supervisor.CheckMergeHealth("db1.new") != nil {
		if time.Now().After(deadline) {
			t.Fatalf("db1.new health = %v after the change-triggered pass", supervisor.CheckMergeHealth("db1.new"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	guard.mu.Lock()
	last := guard.passes[len(guard.passes)-1]
	guard.mu.Unlock()
	if strings.Join(last, ",") != "db1.new,db1.t" {
		t.Fatalf("change-triggered pass asserted %v, want the new snapshot's sorted Active set", last)
	}
}

// TestMergeSupervisorRunReturnsOnCancelBeforeAnyPass guards the change-driven
// loop: a canceled context skips the pass, so an unasserted version must not
// make Run spin instead of returning.
func TestMergeSupervisorRunReturnsOnCancelBeforeAnyPass(t *testing.T) {
	supervisor := NewStorageIntegrityMergeSupervisor(&controllableMergeGuard{}, sitable.NewFake(sitable.Ordinary, active("db1.t")...), time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		supervisor.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after its context was canceled")
	}
}
