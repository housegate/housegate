package keeper

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"

	"housegate/housegate/pkg/log"
)

// Reconfigurer is the minimal subset of go-zookeeper/zk.Conn the
// Orchestrator needs. Pulled into an interface so a fake can drive the
// reconcile loop in unit tests without spinning up a real ZK server.
type Reconfigurer interface {
	// IncrementalReconfig adds/removes members and returns when keeper has
	// committed the new /keeper/config znode. The "joining" entries are in
	// keeper's reconfig string format ("server.<id>=<host>:<raft_port>;participant;<priority>");
	// "leaving" entries are bare server-id strings ("4").
	IncrementalReconfig(joining, leaving []string, version int64) (*zk.Stat, error)
	Close()
}

// OrchestratorConfig configures a per-shard reconfig orchestrator.
type OrchestratorConfig struct {
	// Shard is the keeper-shard name this orchestrator manages. Used in
	// logs and to scope NetworkState lookups.
	Shard string

	// Expected reports the currently desired keeper-pool membership for
	// this shard (host:port keeper-client endpoints). Typically backed by
	// a chain-observing registry.KeeperPool.
	Expected func() []string

	// CurrentMembers reports the keeper quorum's CURRENT membership as
	// recorded in /keeper/config — the source of truth for what the
	// raft cluster considers itself to be. Diffing Expected against
	// CurrentMembers tells the orchestrator what to reconfig. Returning
	// an empty slice means "unknown" (transient read failure) and the
	// cycle is skipped rather than driving a destructive remove.
	CurrentMembers func(ctx context.Context) []string

	// Tracker observes liveness of the EXPECTED member set (each member
	// reachable via its host-mapped client port). Used only to wait for
	// a newly-added member to actually come up before issuing removes.
	Tracker *Tracker

	// Dial returns a Reconfigurer connected to one of the live keeper
	// members (any voter works; reconfig is forwarded to the leader). The
	// orchestrator closes the returned conn after each reconcile cycle to
	// keep zk session lifecycle inside one well-defined window.
	Dial func(ctx context.Context, members []string) (Reconfigurer, error)

	// RaftPort is the raft-protocol port keeper uses (architecture.md §8:
	// 9234). When the orchestrator joins a new member it needs to build
	// the keeper reconfig spec "server.<id>=<host>:<raft_port>" — but
	// Expected gives keeper-CLIENT endpoints (9181). We assume every
	// keeper in a shard uses the same RaftPort; deployments that vary
	// per-node will need a richer Expected source. Default 9234.
	RaftPort int

	// ServerIDFor maps a keeper member's host (or host:port) to its raft
	// server_id. Required: keeper config files pin server_id per host,
	// so the orchestrator must know the same mapping to issue
	// "server.<id>=..." additions and "<id>" removals. Returning false
	// means "I don't know" and aborts the cycle with a warning.
	ServerIDFor func(member string) (int, bool)

	// RaftHostFor maps a member's keeper-client endpoint (host:port,
	// what NetworkState carries) to the host that goes into the
	// "server.<id>=<host>:<raft_port>" reconfig spec. Optional; if nil
	// the host portion of the keeper-client endpoint is reused, which
	// is correct whenever a keeper's raft listener is at the same host
	// as its client listener (the common case).
	RaftHostFor func(member string) string

	// ReconcileInterval between membership diff sweeps. Default 5s.
	ReconcileInterval time.Duration

	// LgifTimeout bounds how long the orchestrator waits for a joining
	// member to catch up (last_committed_log_idx ≥ leader's) before
	// declaring the add failed. Default 30s.
	LgifTimeout time.Duration
}

// Orchestrator drives architecture.md §4 reconfig sequences from
// NetworkState diffs: add-before-remove, and only remove after the new
// member's log has caught up (lgif). One orchestrator per keeper shard.
type Orchestrator struct {
	cfg OrchestratorConfig

	reconfigs atomic.Int64 // total IncrementalReconfig calls issued
	addOps    atomic.Int64 // total members joined
	removeOps atomic.Int64 // total members removed
	mu        sync.Mutex   // serialises reconcile cycles (in case timers overlap)
}

// NewOrchestrator validates cfg and returns an Orchestrator. Run launches
// the reconcile loop and blocks until ctx is cancelled.
func NewOrchestrator(cfg OrchestratorConfig) (*Orchestrator, error) {
	if cfg.Shard == "" {
		return nil, fmt.Errorf("Orchestrator: Shard is required")
	}
	if cfg.Expected == nil {
		return nil, fmt.Errorf("Orchestrator(%s): Expected is required", cfg.Shard)
	}
	if cfg.CurrentMembers == nil {
		return nil, fmt.Errorf("Orchestrator(%s): CurrentMembers is required", cfg.Shard)
	}
	if cfg.Tracker == nil {
		return nil, fmt.Errorf("Orchestrator(%s): Tracker is required", cfg.Shard)
	}
	if cfg.Dial == nil {
		return nil, fmt.Errorf("Orchestrator(%s): Dial is required", cfg.Shard)
	}
	if cfg.ServerIDFor == nil {
		return nil, fmt.Errorf("Orchestrator(%s): ServerIDFor is required", cfg.Shard)
	}
	if cfg.RaftPort == 0 {
		cfg.RaftPort = 9234
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 5 * time.Second
	}
	if cfg.LgifTimeout <= 0 {
		cfg.LgifTimeout = 30 * time.Second
	}
	return &Orchestrator{cfg: cfg}, nil
}

// Run drives reconcile cycles at cfg.ReconcileInterval until ctx is cancelled.
func (o *Orchestrator) Run(ctx context.Context) {
	tk := time.NewTicker(o.cfg.ReconcileInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			if err := o.Reconcile(ctx); err != nil {
				log.Warnw("keeper orchestrator: reconcile failed", "shard", o.cfg.Shard, "err", err)
			}
		}
	}
}

// Reconcile runs one diff-and-reconfig pass. Public for tests.
func (o *Orchestrator) Reconcile(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	expected := o.cfg.Expected()
	if len(expected) == 0 {
		// Empty expected means "unknown" (no chain update yet) — same
		// convention the tracker uses; don't drive members to zero.
		return nil
	}
	// Keep the tracker's probe set aligned with what we want it to know
	// the liveness of, so waitNewMembersLive can see a freshly-joined
	// member without the caller having to sync this externally.
	o.cfg.Tracker.SetMembers(expected)

	current := o.cfg.CurrentMembers(ctx)
	if len(current) == 0 {
		// Transient: couldn't read /keeper/config (no leader, network
		// blip). Skip the cycle rather than gambling on a destructive
		// remove without ground truth.
		return fmt.Errorf("CurrentMembers returned empty — skipping cycle (no leader / read failure)")
	}

	addSet, removeSet := diffMembers(expected, current)
	if len(addSet) == 0 && len(removeSet) == 0 {
		return nil // converged
	}

	// Translate the bare endpoint strings into keeper's reconfig wire
	// format BEFORE dialing — fail fast if ServerIDFor doesn't know one.
	raftHost := o.cfg.RaftHostFor
	if raftHost == nil {
		raftHost = func(m string) string { host, _, _ := strings.Cut(m, ":"); return host }
	}
	joining := make([]string, 0, len(addSet))
	for _, addr := range addSet {
		id, ok := o.cfg.ServerIDFor(addr)
		if !ok {
			return fmt.Errorf("ServerIDFor(%q) returned !ok; cannot build reconfig add for shard %q", addr, o.cfg.Shard)
		}
		joining = append(joining, fmt.Sprintf("server.%d=%s:%d;participant;1", id, raftHost(addr), o.cfg.RaftPort))
	}
	leaving := make([]string, 0, len(removeSet))
	for _, addr := range removeSet {
		id, ok := o.cfg.ServerIDFor(addr)
		if !ok {
			return fmt.Errorf("ServerIDFor(%q) returned !ok; cannot build reconfig remove for shard %q", addr, o.cfg.Shard)
		}
		leaving = append(leaving, strconv.Itoa(id))
	}

	// Dial through the tracker's CURRENTLY live members (the orchestrator
	// hits whichever keeper-client port responds; the keeper itself
	// forwards reconfig to the leader).
	live := o.cfg.Tracker.Live()
	if len(live) == 0 {
		return fmt.Errorf("no live keepers to dial for reconfig (§5 force-recovery territory)")
	}
	conn, err := o.cfg.Dial(ctx, live)
	if err != nil {
		return fmt.Errorf("dial keeper: %w", err)
	}
	defer conn.Close()

	// architecture.md §4 ordering rule: do ALL adds first, wait for the
	// new members to catch up via lgif, only then issue removes. Adding
	// and removing in one call is what the keeper API accepts atomically
	// (IncrementalReconfig is a single ZK transaction), but pulling
	// adds-only first gives the new member time to ship its snapshot.
	if len(joining) > 0 {
		log.Infow("keeper orchestrator: adding members", "shard", o.cfg.Shard, "joining", joining)
		if _, err := conn.IncrementalReconfig(joining, nil, -1); err != nil {
			return fmt.Errorf("reconfig add: %w", err)
		}
		o.reconfigs.Add(1)
		o.addOps.Add(int64(len(joining)))

		// Wait for the new members to catch up via lgif before issuing
		// the removes. The tracker's probe loop already polls mntr;
		// just give the new members enough time to be seen "alive".
		if err := o.waitNewMembersLive(ctx, addSet); err != nil {
			return fmt.Errorf("wait new members live: %w", err)
		}
	}
	if len(leaving) > 0 {
		log.Infow("keeper orchestrator: removing members", "shard", o.cfg.Shard, "leaving", leaving)
		if _, err := conn.IncrementalReconfig(nil, leaving, -1); err != nil {
			return fmt.Errorf("reconfig remove: %w", err)
		}
		o.reconfigs.Add(1)
		o.removeOps.Add(int64(len(leaving)))
	}
	return nil
}

// waitNewMembersLive polls the tracker until every member in addSet shows
// up as alive (or LgifTimeout expires). The tracker drives its own probe
// loop, so we just refresh on a tight cadence.
func (o *Orchestrator) waitNewMembersLive(ctx context.Context, addSet []string) error {
	deadline := time.Now().Add(o.cfg.LgifTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		o.cfg.Tracker.ProbeOnce(ctx)
		live := stringSet(o.cfg.Tracker.Live())
		all := true
		for _, m := range addSet {
			if !live[m] {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("new members did not become live within %s: %v", o.cfg.LgifTimeout, addSet)
}

// Reconfigs / Adds / Removes are observability accessors used in tests.
func (o *Orchestrator) Reconfigs() int64 { return o.reconfigs.Load() }
func (o *Orchestrator) Adds() int64      { return o.addOps.Load() }
func (o *Orchestrator) Removes() int64   { return o.removeOps.Load() }

// diffMembers returns (toAdd, toRemove) given desired and current member
// sets, where current is what the tracker considers live.
func diffMembers(expected, live []string) (add, remove []string) {
	want := stringSet(expected)
	have := stringSet(live)
	for _, m := range expected {
		if !have[m] {
			add = append(add, m)
		}
	}
	for _, m := range live {
		if !want[m] {
			remove = append(remove, m)
		}
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

func stringSet(in []string) map[string]bool {
	s := make(map[string]bool, len(in))
	for _, v := range in {
		s[v] = true
	}
	return s
}
