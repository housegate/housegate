// Package keeper implements housegate's CH-facing keeper proxy (link A of
// the keeper-pool design): a co-located ClickHouse points its <zookeeper>
// at this proxy, and the proxy forwards the keeper-client byte stream to a
// live quorum member, re-steering when quorum membership changes.
//
// The proxy is deliberately protocol-unaware (L4 byte relay). It never
// participates in Raft and never drives reconfiguration; it only observes
// quorum liveness via the read-only four-letter-word (4LW) channel and
// steers connections. ClickHouse's keeper client tolerates reconnects and
// re-establishes its session / ephemerals / watches itself, which is what
// makes a re-steer transparent to CH (proved end-to-end by
// TestInterserverMeshReplication: real ReplicatedMergeTree coordination
// cannot succeed without keeper, and the test runs it through pkg/keeper).
package keeper

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Member is the observed state of one keeper in the pool.
type Member struct {
	Addr  string
	Alive bool
	State ServerState
}

// TrackerConfig configures quorum health tracking.
type TrackerConfig struct {
	// Members is the initial expected keeper-client endpoint set
	// (host:port). May be updated at runtime via SetMembers.
	Members []string
	// MembersFunc, when non-nil, is the live source of quorum membership
	// (e.g. backed by NetworkState observing the on-chain keeper-pool
	// change). It is consulted before every probe sweep; a non-empty
	// result replaces the member set so the proxy tracks reconfigurations
	// (added/removed keepers). An empty/nil result is ignored — the
	// current members are kept rather than going dark on a transient read.
	MembersFunc func() []string
	// ProbeInterval between health sweeps (default 1s).
	ProbeInterval time.Duration
	// ProbeTimeout per 4LW probe (default 2s).
	ProbeTimeout time.Duration
}

// Tracker keeps a live view of the keeper quorum by periodically probing
// each expected member's 4LW endpoint.
type Tracker struct {
	probeInterval time.Duration
	probeTimeout  time.Duration
	membersFunc   func() []string

	mu      sync.RWMutex
	members []string
	state   map[string]Member
}

// NewTracker builds a Tracker over the configured member set. Call Run to
// start probing, or ProbeOnce for a single synchronous sweep.
func NewTracker(cfg TrackerConfig) *Tracker {
	pi := cfg.ProbeInterval
	if pi <= 0 {
		pi = time.Second
	}
	pt := cfg.ProbeTimeout
	if pt <= 0 {
		pt = 2 * time.Second
	}
	t := &Tracker{
		probeInterval: pi,
		probeTimeout:  pt,
		membersFunc:   cfg.MembersFunc,
		state:         map[string]Member{},
	}
	t.SetMembers(cfg.Members)
	return t
}

// SetMembers replaces the expected member set. New members start as
// not-alive until the next probe; removed members are dropped so Live()
// stops returning them immediately (the reconcile loop then re-steers any
// connection still pinned to a removed member).
func (t *Tracker) SetMembers(members []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.members = append([]string(nil), members...)
	next := make(map[string]Member, len(members))
	for _, m := range members {
		if cur, ok := t.state[m]; ok {
			next[m] = cur
		} else {
			next[m] = Member{Addr: m}
		}
	}
	t.state = next
}

// Run probes until ctx is cancelled.
func (t *Tracker) Run(ctx context.Context) {
	t.ProbeOnce(ctx)
	tk := time.NewTicker(t.probeInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.ProbeOnce(ctx)
		}
	}
}

// ProbeOnce runs one health sweep across all expected members in parallel.
// If a live membership source (MembersFunc) is configured, the member set
// is refreshed from it first, so a reconfigured quorum is picked up.
func (t *Tracker) ProbeOnce(ctx context.Context) {
	if t.membersFunc != nil {
		if m := t.membersFunc(); len(m) > 0 {
			t.SetMembers(m)
		}
	}

	t.mu.RLock()
	members := append([]string(nil), t.members...)
	t.mu.RUnlock()

	results := make([]Member, len(members))
	var wg sync.WaitGroup
	for i, addr := range members {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			alive, state := probe(ctx, addr, t.probeTimeout)
			results[i] = Member{Addr: addr, Alive: alive, State: state}
		}(i, addr)
	}
	wg.Wait()

	t.mu.Lock()
	for _, r := range results {
		if _, ok := t.state[r.Addr]; ok { // ignore members removed mid-sweep
			t.state[r.Addr] = r
		}
	}
	t.mu.Unlock()
}

// Live returns the addresses of currently-alive members, sorted for
// deterministic iteration.
func (t *Tracker) Live() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var live []string
	for _, m := range t.members {
		if s, ok := t.state[m]; ok && s.Alive {
			live = append(live, m)
		}
	}
	sort.Strings(live)
	return live
}

// Leader returns the address of the member reporting leader (or standalone,
// for a single-node ensemble), or "" if none is currently known.
func (t *Tracker) Leader() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, m := range t.members {
		if s, ok := t.state[m]; ok && s.Alive && (s.State == StateLeader || s.State == StateStandalone) {
			return m
		}
	}
	return ""
}

// Snapshot returns a copy of per-member state in expected-member order.
func (t *Tracker) Snapshot() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		if s, ok := t.state[m]; ok {
			out = append(out, s)
		}
	}
	return out
}
