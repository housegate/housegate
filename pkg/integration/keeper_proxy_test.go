package integration

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/keeper"
)

// TestKeeperProxy exercises pkg/keeper (link A of the keeper-pool design)
// against a real 3-node clickhouse-keeper:25.8 quorum. One cluster + one
// proxy are shared across ordered subtests (the failover case runs last,
// since it degrades the quorum). Pins the proxy's own behaviour: it relays
// the real keeper 4LW protocol, tracks the live quorum, and re-steers on
// failover. The richer "real CH replicates through pkg/keeper" assertion
// lives in TestInterserverMeshReplication (interserver_mesh_test.go).
func TestKeeperProxy(t *testing.T) {
	cluster := testenv.StartKeeperCluster(t, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := keeper.NewTracker(keeper.TrackerConfig{
		Members:       cluster.Endpoints,
		ProbeInterval: 300 * time.Millisecond,
		ProbeTimeout:  2 * time.Second,
	})
	go tr.Run(ctx)

	srv, err := keeper.NewServer(keeper.ServerConfig{
		Tracker:           tr,
		Strategy:          keeper.LeaderPref,
		ReconcileInterval: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("keeper.NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	proxyAddr := ln.Addr().String()
	go func() { _ = srv.Serve(ctx, ln) }()

	t.Run("DiscoversQuorumAndLeader", func(t *testing.T) {
		waitUntil(t, 30*time.Second, func() bool {
			return len(tr.Live()) == 3 && tr.Leader() != ""
		})
	})

	t.Run("RelaysFourLetterWord", func(t *testing.T) {
		// ruok -> imok proves the L4 relay carries the real keeper
		// protocol end-to-end (client -> proxy -> a live keeper -> back).
		if got := proxyFourLetter(t, proxyAddr, "ruok"); !strings.HasPrefix(got, "imok") {
			t.Fatalf("ruok via proxy = %q, want imok", got)
		}
		// mntr round-trips a larger, multi-line reply through the relay.
		if got := proxyFourLetter(t, proxyAddr, "mntr"); !strings.Contains(got, "zk_server_state") {
			t.Fatalf("mntr via proxy missing zk_server_state: %q", got)
		}
	})

	t.Run("ReSteersOnLeaderDown", func(t *testing.T) {
		oldLeader := tr.Leader()
		idx := cluster.IndexOf(oldLeader)
		if idx < 0 {
			t.Fatalf("leader %q is not a known cluster endpoint %v", oldLeader, cluster.Endpoints)
		}

		// Kill the leader the proxy was steering to. 3->2 keeps a
		// majority, so the survivors elect a new leader and the tracker
		// must converge on it.
		cluster.Stop(t, idx)
		waitUntil(t, 45*time.Second, func() bool {
			l := tr.Leader()
			return l != "" && l != oldLeader && len(tr.Live()) == 2
		})

		// The proxy keeps serving keeper traffic, now steered to a live
		// member — no client-visible reconfiguration.
		if got := proxyFourLetter(t, proxyAddr, "ruok"); !strings.HasPrefix(got, "imok") {
			t.Fatalf("post-failover ruok via proxy = %q, want imok", got)
		}
	})
}

// proxyFourLetter opens a fresh connection to the keeper proxy, sends a 4LW
// command, and returns the relayed keeper response.
func proxyFourLetter(t *testing.T, proxyAddr, word string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", proxyAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial keeper proxy: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(word)); err != nil {
		t.Fatalf("write %q: %v", word, err)
	}
	b, err := io.ReadAll(c)
	if err != nil && len(b) == 0 {
		t.Fatalf("read %q response: %v", word, err)
	}
	return string(b)
}

func waitUntil(t *testing.T, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", budget)
}
