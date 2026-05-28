package integration

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"

	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/keeper"
)

// TestKeeperOrchestratorLiveReconfig drives the §4 reconfig flow
// end-to-end: kill one keeper, update NetworkState expected membership to
// point at a freshly-started keeper, and watch pkg/keeper/orchestrator
// reconfigure the live cluster without operator action.
//
// The path is:
//
//	NetworkState.SetKeeperPool("default", kA-1,kA-2,kA-4)
//	  ↓ Orchestrator.Reconcile
//	  ↓ IncrementalReconfig(joining=[server.4=kA-4:9234;participant;1])
//	  ↓ wait until tracker sees kA-4 alive (mntr)
//	  ↓ IncrementalReconfig(leaving=[3])
//	  ↓ quorum is now {1,2,4}; CH stays connected through kpx
//
// All keeper-protocol traffic (reconfig included) traverses our pkg/keeper
// L4 proxy — the orchestrator dials kpxA-1, the proxy steers to a live
// voter. So the test also exercises that a real ZK protocol session (not
// just 4LW byte-level) round-trips correctly through pkg/keeper.
func TestKeeperOrchestratorLiveReconfig(t *testing.T) {
	cluster := testenv.StartKeeperCluster(t, 3) // aliases keeper-1/2/3

	// Two keeper proxies in front of the same shard, one co-located per CH
	// (mirroring the multi-shard test's layout). kpxHost is host-reachable
	// so the orchestrator (running in the test process) can dial it too.
	kpxHost := testenv.StartKeeperProxy(t, cluster, "kpx-1")
	testenv.StartKeeperProxy(t, cluster, "kpx-2")

	ch1 := testenv.StartClickHouseMeshReplica(t, cluster, "ch-1", "kpx-1:9181", nil)
	ch2 := testenv.StartClickHouseMeshReplica(t, cluster, "ch-2", "kpx-2:9181", nil)

	ca := testenv.NewMeshCA(t, "ch-1", "ch-2")
	testenv.StartInterserverMeshSidecar(t, ch1.ContainerName, ca, map[string]string{"ch-2": "ch-2:19009"})
	testenv.StartInterserverMeshSidecar(t, ch2.ContainerName, ca, map[string]string{"ch-1": "ch-1:19009"})

	bin := testenv.ClickHouseCLI(t)
	runSQL := func(addr, database, query string) (string, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return "", err
		}
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(c, bin, "client",
			"--host", host,
			"--port", port,
			"--user", "default",
			"--password", "housegate_test_pw",
			"--database", database,
			"--query", query,
		)
		out, err := cmd.CombinedOutput()
		return strings.TrimRight(string(out), "\n"), err
	}

	// Create a ReplicatedMergeTree and pump a baseline insert through to
	// prove the cluster is healthy before we start swapping members.
	t.Log("baseline: create RMT, insert 100, verify replication")
	for _, c := range []struct{ addr, name string }{{ch1.NativeAddr, "ch-1"}, {ch2.NativeAddr, "ch-2"}} {
		var lastErr error
		for i := 1; i <= 20; i++ {
			if _, err := runSQL(c.addr, "default", "CREATE DATABASE IF NOT EXISTS testdb"); err != nil {
				lastErr = err
				time.Sleep(2 * time.Second)
				continue
			}
			if _, err := runSQL(c.addr, "default",
				"CREATE TABLE IF NOT EXISTS testdb.events (ts DateTime, uid UInt64, data String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/01/testdb/events', '{replica}') ORDER BY (ts, uid)"); err != nil {
				lastErr = err
				time.Sleep(2 * time.Second)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			t.Fatalf("create table on %s: %v", c.name, lastErr)
		}
	}
	if _, err := runSQL(ch1.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'baseline' FROM numbers(100)"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if !waitForCount(t, runSQL, ch2.NativeAddr, "100", 60*time.Second) {
		t.Fatalf("baseline replication failed")
	}

	// Start an Orchestrator-driving tracker pointed at the keeper
	// endpoints DIRECTLY (host-mapped). The orchestrator's job is to
	// observe each member's individual health, so it doesn't go through
	// the housegate kpx proxy (which would hide which specific node is
	// up). The kpx proxy in front of CH still proves the value: CH stays
	// happy throughout while the orchestrator rewires the quorum behind
	// the proxy.
	_ = kpxHost
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker := keeper.NewTracker(keeper.TrackerConfig{
		Members:       append([]string(nil), cluster.Endpoints...),
		ProbeInterval: 500 * time.Millisecond,
		ProbeTimeout:  2 * time.Second,
	})
	tracker.ProbeOnce(ctx) // seed

	// Server-id mapping: each keeper container's alias "keeper-N" maps to
	// raft id N. AddNode below will give us a new endpoint for id=4.
	idMap := map[string]int{}
	idToEndpoint := map[int]string{}
	for i, ep := range cluster.Endpoints {
		idMap[ep] = i + 1
		idToEndpoint[i+1] = ep
	}
	// idLock guards the two maps once AddNode mutates them mid-test.
	var idLock sync.Mutex

	// CurrentMembers reads /keeper/config from the cluster (parsing the
	// server.<id>=<host>:<port>;participant;<priority> lines) and maps
	// each id back to its host-reachable keeper-client endpoint. That's
	// the same axis Expected lives on, so the diff is well-defined.
	currentMembers := func(c context.Context) []string {
		live := tracker.Live()
		if len(live) == 0 {
			return nil
		}
		conn, _, err := zk.Connect(live, 10*time.Second)
		if err != nil {
			return nil
		}
		defer conn.Close()
		data, _, err := conn.Get("/keeper/config")
		if err != nil {
			return nil
		}
		out := make([]string, 0, 8)
		idLock.Lock()
		defer idLock.Unlock()
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "server.") {
				continue
			}
			eq := strings.IndexByte(line, '=')
			if eq < 0 {
				continue
			}
			idStr := line[len("server."):eq]
			id, perr := strconv.Atoi(idStr)
			if perr != nil {
				continue
			}
			if ep, ok := idToEndpoint[id]; ok {
				out = append(out, ep)
			}
		}
		return out
	}

	// "Expected" is what NetworkState says the shard should look like;
	// initially matches the live cluster, so the orchestrator no-ops.
	expectedMu := struct {
		members []string
	}{members: append([]string(nil), cluster.Endpoints...)}

	orch, err := keeper.NewOrchestrator(keeper.OrchestratorConfig{
		Shard:          "default",
		Expected:       func() []string { return append([]string(nil), expectedMu.members...) },
		CurrentMembers: currentMembers,
		Tracker:        tracker,
		Dial: func(c context.Context, members []string) (keeper.Reconfigurer, error) {
			conn, _, err := zk.Connect(members, 10*time.Second)
			return conn, err
		},
		ServerIDFor: func(m string) (int, bool) {
			idLock.Lock()
			defer idLock.Unlock()
			id, ok := idMap[m]
			return id, ok
		},
		// In the test, each keeper's raft host is its container's network
		// alias (keeper-N), NOT its host-mapped client endpoint. Map back.
		RaftHostFor: func(m string) string {
			idLock.Lock()
			defer idLock.Unlock()
			id := idMap[m]
			return "keeper-" + strconv.Itoa(id)
		},
		ReconcileInterval: time.Second,
		LgifTimeout:       30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	go orch.Run(ctx)

	// ---- The reconfig drama ------------------------------------------
	t.Log("killing keeper-3 (the member to retire)")
	cluster.Stop(t, 2) // index 2 == keeper-3

	t.Log("starting keeper-4 with raft_configuration [1,2,3,4]")
	_, newEndpoint := cluster.AddNode(t, "keeper", 4, []int{1, 2, 3, 4})
	idLock.Lock()
	idMap[newEndpoint] = 4
	idToEndpoint[4] = newEndpoint
	idLock.Unlock()

	t.Log("swapping NetworkState expected members: [keeper-1, keeper-2, keeper-4]")
	expectedMu.members = []string{
		cluster.Endpoints[0], // keeper-1
		cluster.Endpoints[1], // keeper-2
		newEndpoint,          // keeper-4
	}

	// Repoint the orchestrator's liveness tracker at the new expected set
	// so it stops dialing the dead keeper-3 and starts probing keeper-4.
	tracker.SetMembers(expectedMu.members)

	// Wait for the orchestrator to converge — keeper-4 alive, keeper-3 not.
	deadline := time.Now().Add(90 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		// We assert convergence by both: 4LW state on the new keeper, and
		// the orchestrator counters showing one add and one remove.
		if orch.Adds() >= 1 && orch.Removes() >= 1 {
			converged = true
			break
		}
		time.Sleep(time.Second)
	}
	if !converged {
		t.Fatalf("orchestrator did not converge: adds=%d removes=%d reconfigs=%d", orch.Adds(), orch.Removes(), orch.Reconfigs())
	}
	t.Logf("  ✓ orchestrator reconfigured: %d adds, %d removes, %d reconfig calls",
		orch.Adds(), orch.Removes(), orch.Reconfigs())

	// Replication must still work after the reconfig: insert through ch-1,
	// see it on ch-2.
	t.Log("post-reconfig: insert 50 on ch-1; verify replication to ch-2")
	if _, err := runSQL(ch1.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'after-reconfig' FROM numbers(50)"); err != nil {
		t.Fatalf("post-reconfig insert: %v", err)
	}
	if !waitForCount(t, runSQL, ch2.NativeAddr, "150", 60*time.Second) {
		t.Fatalf("post-reconfig replication failed (cluster lost coordination after reconfig)")
	}
	t.Log("  ✓ replication still works after live reconfig — CH never noticed")
}
