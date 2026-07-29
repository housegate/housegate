package integration

import (
	"context"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/metrics"
)

// TestCHPollerAgainstClickHouse drives the metrics.CHPoller against the shared
// testcontainers ClickHouse (chEnv, started in TestMain) and asserts it reads
// real system-table values over the native clickhouse-go/v2 driver. This is the
// production query path that the docker-free unit tests can only fake.
func TestCHPollerAgainstClickHouse(t *testing.T) {
	p, err := metrics.NewCHPoller(chEnv.Addr, chEnv.Database, chEnv.User, chEnv.Password, 5*time.Second)
	if err != nil {
		t.Fatalf("NewCHPoller: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Run a query first so system.events has a nonzero Query counter even on a
	// freshly-started server, then poll.
	conn := openConn(t, chEnv.Addr)
	var ignore uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&ignore); err != nil {
		t.Fatalf("warm-up SELECT 1: %v", err)
	}

	got, err := p.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll against live ClickHouse: %v", err)
	}

	if !got.Reachable {
		t.Error("Reachable = false against a live ClickHouse")
	}
	if got.Replica != chEnv.Addr {
		t.Errorf("Replica = %q, want %q", got.Replica, chEnv.Addr)
	}
	// A running ClickHouse always reports nonzero MemoryTracking in
	// system.metrics; this proves the curated mapping read a real value.
	if got.MemoryTrackingBytes == 0 {
		t.Errorf("MemoryTrackingBytes = 0; expected a live server to report nonzero memory tracking. full: %+v", got)
	}
	t.Logf("live CH metrics: %+v", got)
}

// TestCHPollerUnreachable points the poller at a dead address and asserts the
// fail-open contract end to end: an error is returned and Reachable is false.
func TestCHPollerUnreachable(t *testing.T) {
	// 127.0.0.1:1 is a reserved port nothing listens on.
	p, err := metrics.NewCHPoller("127.0.0.1:1", "default", "default", "", 2*time.Second)
	if err != nil {
		t.Fatalf("NewCHPoller: %v", err)
	}
	defer func() { _ = p.Close() }()

	got, err := p.Poll(context.Background())
	if err == nil {
		t.Fatal("expected an error polling a dead address")
	}
	if got.Reachable {
		t.Error("Reachable = true, want false for a dead address")
	}
}
