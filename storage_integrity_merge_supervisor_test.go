package housegate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type controllableMergeGuard struct {
	mu     sync.Mutex
	err    error
	called chan struct{}
}

func (g *controllableMergeGuard) AssertStopMerges(context.Context) error {
	g.mu.Lock()
	err := g.err
	g.mu.Unlock()
	if g.called != nil {
		select {
		case g.called <- struct{}{}:
		default:
		}
	}
	return err
}

func (g *controllableMergeGuard) setError(err error) {
	g.mu.Lock()
	g.err = err
	g.mu.Unlock()
}

func TestMergeSupervisorStartsClosedUntilAssertSucceeds(t *testing.T) {
	guard := &controllableMergeGuard{}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, time.Second)

	if err := supervisor.CheckMergeHealth(); err == nil || !strings.Contains(err.Error(), "not asserted") {
		t.Fatalf("initial health err = %v, want not asserted", err)
	}
	if err := supervisor.AssertStopMerges(context.Background()); err != nil {
		t.Fatalf("AssertStopMerges: %v", err)
	}
	if err := supervisor.CheckMergeHealth(); err != nil {
		t.Fatalf("health after successful assert: %v", err)
	}
}

func TestMergeSupervisorClosesAndReopensHealthAcrossReasserts(t *testing.T) {
	guard := &controllableMergeGuard{}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, time.Second)
	if err := supervisor.AssertStopMerges(context.Background()); err != nil {
		t.Fatalf("initial AssertStopMerges: %v", err)
	}

	guard.setError(errors.New("clickhouse reconnect failed"))
	if err := supervisor.AssertStopMerges(context.Background()); err == nil {
		t.Fatal("failed reassert unexpectedly succeeded")
	}
	if err := supervisor.CheckMergeHealth(); err == nil || !strings.Contains(err.Error(), "clickhouse reconnect failed") {
		t.Fatalf("closed health err = %v, want reconnect failure", err)
	}

	guard.setError(nil)
	if err := supervisor.AssertStopMerges(context.Background()); err != nil {
		t.Fatalf("recovery AssertStopMerges: %v", err)
	}
	if err := supervisor.CheckMergeHealth(); err != nil {
		t.Fatalf("health after recovery: %v", err)
	}
}

func TestMergeSupervisorRunPeriodicallyReasserts(t *testing.T) {
	called := make(chan struct{}, 1)
	guard := &controllableMergeGuard{called: called}
	supervisor := NewStorageIntegrityMergeSupervisor(guard, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("periodic reassert did not run")
	}
}
