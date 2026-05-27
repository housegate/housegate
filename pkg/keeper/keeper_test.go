package keeper

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeKeeper is an in-process stand-in for a clickhouse-keeper client port.
// It answers the 4LW words the proxy uses (ruok/mntr) and otherwise behaves
// as an echo upstream so the L4 relay can be exercised without docker.
type fakeKeeper struct {
	ln    net.Listener
	mu    sync.Mutex
	state ServerState
}

func newFakeKeeper(t *testing.T, state ServerState) *fakeKeeper {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakeKeeper listen: %v", err)
	}
	fk := &fakeKeeper{ln: ln, state: state}
	go fk.serve()
	t.Cleanup(fk.close)
	return fk
}

func (fk *fakeKeeper) addr() string { return fk.ln.Addr().String() }
func (fk *fakeKeeper) close()       { _ = fk.ln.Close() }

func (fk *fakeKeeper) setState(s ServerState) {
	fk.mu.Lock()
	fk.state = s
	fk.mu.Unlock()
}

func (fk *fakeKeeper) serve() {
	for {
		c, err := fk.ln.Accept()
		if err != nil {
			return
		}
		go fk.handle(c)
	}
}

func (fk *fakeKeeper) handle(c net.Conn) {
	defer c.Close()
	word := make([]byte, 4)
	if _, err := io.ReadFull(c, word); err != nil {
		return
	}
	switch string(word) {
	case "ruok":
		_, _ = c.Write([]byte("imok"))
	case "mntr":
		fk.mu.Lock()
		st := fk.state
		fk.mu.Unlock()
		_, _ = fmt.Fprintf(c, "zk_version\tfake-keeper\nzk_server_state\t%s\n", st)
	default:
		// Relay/echo upstream: echo the bytes we consumed, then stream.
		_, _ = c.Write(word)
		_, _ = io.Copy(c, c)
	}
}

func TestParseServerState(t *testing.T) {
	cases := map[string]ServerState{
		"zk_version\tx\nzk_server_state\tleader\n":    StateLeader,
		"zk_server_state\tfollower\n":                 StateFollower,
		"zk_server_state\tstandalone\n":               StateStandalone,
		"zk_latency_min\t0\nzk_packets_received\t9\n": StateUnknown,
		"": StateUnknown,
	}
	for in, want := range cases {
		if got := parseServerState(in); got != want {
			t.Errorf("parseServerState(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseStrategy(t *testing.T) {
	cases := map[string]Strategy{
		"leader_pref": LeaderPref,
		"any_voter":   AnyVoter,
		"":            AnyVoter, // default
		"bogus":       AnyVoter, // unknown falls back to any_voter
	}
	for in, want := range cases {
		if got := ParseStrategy(in); got != want {
			t.Errorf("ParseStrategy(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestTrackerDiscoversQuorumAndLeader(t *testing.T) {
	ctx := context.Background()
	leader := newFakeKeeper(t, StateLeader)
	f1 := newFakeKeeper(t, StateFollower)
	f2 := newFakeKeeper(t, StateFollower)

	tr := NewTracker(TrackerConfig{
		Members:      []string{leader.addr(), f1.addr(), f2.addr()},
		ProbeTimeout: 500 * time.Millisecond,
	})
	tr.ProbeOnce(ctx)

	if got := len(tr.Live()); got != 3 {
		t.Fatalf("Live() = %d members, want 3 (%v)", got, tr.Snapshot())
	}
	if got := tr.Leader(); got != leader.addr() {
		t.Fatalf("Leader() = %q, want %q", got, leader.addr())
	}

	// One follower goes away: quorum still has a leader, Live drops to 2.
	f1.close()
	tr.ProbeOnce(ctx)
	if got := len(tr.Live()); got != 2 {
		t.Fatalf("after follower down: Live() = %d, want 2", got)
	}
	if tr.Leader() != leader.addr() {
		t.Fatalf("after follower down: leader changed unexpectedly")
	}

	// Leader goes away: no member reports leader anymore.
	leader.close()
	tr.ProbeOnce(ctx)
	if got := tr.Leader(); got != "" {
		t.Fatalf("after leader down: Leader() = %q, want empty", got)
	}
}

func TestTrackerSetMembersDropsRemoved(t *testing.T) {
	ctx := context.Background()
	a := newFakeKeeper(t, StateLeader)
	b := newFakeKeeper(t, StateFollower)
	tr := NewTracker(TrackerConfig{Members: []string{a.addr(), b.addr()}, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)
	if len(tr.Live()) != 2 {
		t.Fatalf("Live() = %d, want 2", len(tr.Live()))
	}
	// Reconfig removes b from the expected set.
	tr.SetMembers([]string{a.addr()})
	if got := tr.Live(); len(got) != 1 || got[0] != a.addr() {
		t.Fatalf("after SetMembers: Live() = %v, want [%s]", got, a.addr())
	}
}

// startProxy launches a keeper.Server in front of the tracker on an
// ephemeral port and returns the server plus its bound address.
func startProxy(t *testing.T, ctx context.Context, tr *Tracker, strat Strategy) (*Server, string) {
	t.Helper()
	srv, err := NewServer(ServerConfig{
		Tracker:           tr,
		Strategy:          strat,
		ReconcileInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = srv.Serve(ctx, ln) }()
	return srv, addr
}

// roundTrip dials the proxy, writes payload, and returns the echoed bytes.
func roundTrip(t *testing.T, addr, payload string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(payload)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	return string(buf)
}

func TestServerRelaysToLiveMember(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k := newFakeKeeper(t, StateLeader)
	tr := NewTracker(TrackerConfig{Members: []string{k.addr()}, ProbeInterval: 50 * time.Millisecond, ProbeTimeout: 500 * time.Millisecond})
	tr.ProbeOnce(ctx)
	_, addr := startProxy(t, ctx, tr, AnyVoter)

	const msg = "RELAY-PAYLOAD-1234"
	if got := roundTrip(t, addr, msg); got != msg {
		t.Fatalf("relay round-trip = %q, want %q", got, msg)
	}
}

func TestServerReSteersOnMemberDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := newFakeKeeper(t, StateLeader)
	b := newFakeKeeper(t, StateFollower)
	tr := NewTracker(TrackerConfig{
		Members:       []string{a.addr(), b.addr()},
		ProbeInterval: 50 * time.Millisecond,
		ProbeTimeout:  500 * time.Millisecond,
	})
	go tr.Run(ctx)
	waitFor(t, 3*time.Second, func() bool { return len(tr.Live()) == 2 && tr.Leader() == a.addr() })

	// LeaderPref steers the held connection deterministically to leader a.
	srv, addr := startProxy(t, ctx, tr, LeaderPref)

	held, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer held.Close()
	// Drive one echo so the relay is fully established on leader a.
	if _, err := held.Write([]byte("WARMUP--")); err != nil {
		t.Fatalf("warmup write: %v", err)
	}
	buf := make([]byte, 8)
	_ = held.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(held, buf); err != nil {
		t.Fatalf("warmup read: %v", err)
	}
	if srv.LiveConns() != 1 {
		t.Fatalf("LiveConns = %d, want 1", srv.LiveConns())
	}

	// Leader dies; promote b. The reconcile loop must drop the held conn
	// (its upstream left the quorum) and a new conn must steer to b.
	a.close()
	b.setState(StateLeader)
	waitFor(t, 5*time.Second, func() bool { return tr.Leader() == b.addr() && len(tr.Live()) == 1 })
	waitFor(t, 5*time.Second, func() bool { return srv.LiveConns() == 0 })
	if srv.Dropped() < 1 {
		t.Fatalf("Dropped = %d, want >= 1 (stale conn should be re-steered)", srv.Dropped())
	}

	const msg = "AFTER-FAILOVER-XYZ"
	if got := roundTrip(t, addr, msg); got != msg {
		t.Fatalf("post-failover relay = %q, want %q", got, msg)
	}
}

func waitFor(t *testing.T, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", budget)
}
