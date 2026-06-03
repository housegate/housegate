package integration

import (
	"context"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
)

// TestInterserverMeshReplication exercises a full ReplicatedMergeTree
// workload with BOTH CH↔keeper (9181, link A) and CH↔CH interserver (9009,
// link B) flowing through housegate, with the interserver hop authenticated
// by mTLS between housegate sidecars:
//
//	3-node Keeper quorum         (keeper-1/2/3)
//	2 keeper proxies              (kpx-1, kpx-2 — pkg/keeper, front the quorum)
//	2-node ReplicatedMergeTree   (ch-1, ch-2; <zookeeper> -> their kpx)
//	2 interserver mesh sidecars   (imesh-{1,2} sharing CH's netns)
//
// Each CH advertises 127.0.0.1:9010 as its interserver host, so each peer
// naturally dials its OWN sidecar's egress (same trick that makes a
// co-located housegate work for 9000). The sidecar's egress reads the
// source replica from the HTTP endpoint URL, mTLS-dials the source's
// sidecar ingress, the ingress validates the client cert against the
// shared CA and forwards to the source CH on 127.0.0.2:9010 (a secondary
// loopback IP so it doesn't collide with the sidecar's egress on
// 127.0.0.1:9010 — CH couples interserver listen+advertised port).
//
// Replication succeeding proves that BOTH housegates were on the path AND
// both successfully completed mTLS — no peer can reach CH's interserver
// without a CA-signed client cert. The keeper hop is similarly proven by
// replication: ReplicatedMergeTree coordination is impossible without it.
func TestInterserverMeshReplication(t *testing.T) {
	cluster := testenv.StartKeeperCluster(t, 3)
	testenv.StartKeeperProxy(t, cluster, "kpx-1")
	testenv.StartKeeperProxy(t, cluster, "kpx-2")

	ch1 := testenv.StartClickHouseMeshReplica(t, cluster, "ch-1", "kpx-1:9181", nil)
	ch2 := testenv.StartClickHouseMeshReplica(t, cluster, "ch-2", "kpx-2:9181", nil)

	// Self-signed CA + a single leaf cert (used by both sidecars as both
	// client cert and server cert). In production each housegate would
	// have its own cert; sharing one here keeps the test self-contained.
	ca := testenv.NewMeshCA(t, "ch-1", "ch-2")

	// Each peer's ingress is reachable at <peer-alias>:19009 over the
	// docker network (because the sidecar shares its CH's netns, joining
	// the network under that alias).
	testenv.StartInterserverMeshSidecar(t, ch1.ContainerName, ca, map[string]string{"ch-2": "ch-2:19009"})
	testenv.StartInterserverMeshSidecar(t, ch2.ContainerName, ca, map[string]string{"ch-1": "ch-1:19009"})

	bin := testenv.ClickHouseCLI(t)
	runSQL := func(addr, database, query string) (string, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return "", err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, "client",
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

	t.Log("creating ReplicatedMergeTree on both replicas (through kpx + mesh sidecars)")
	tblDDL := "CREATE TABLE testdb.events (ts DateTime, uid UInt64, data String) " +
		"ENGINE=ReplicatedMergeTree('/clickhouse/tables/01/testdb/events', '{replica}') " +
		"ORDER BY (ts, uid)"
	for _, c := range []struct{ addr, name string }{{ch1.NativeAddr, "ch-1"}, {ch2.NativeAddr, "ch-2"}} {
		var lastErr error
		for attempt := 1; attempt <= 20; attempt++ {
			if _, err := runSQL(c.addr, "default", "CREATE DATABASE IF NOT EXISTS testdb"); err != nil {
				lastErr = err
				time.Sleep(2 * time.Second)
				continue
			}
			if _, err := runSQL(c.addr, "default", tblDDL); err != nil {
				lastErr = err
				time.Sleep(2 * time.Second)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			t.Fatalf("failed to create table on %s: %v", c.name, lastErr)
		}
	}

	t.Log("ch-1 INSERT 1000 → ch-2 must fetch via gw-2 egress → mTLS → gw-1 ingress → ch-1")
	if _, err := runSQL(ch1.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'mesh' FROM numbers(1000)"); err != nil {
		t.Fatalf("insert on ch-1: %v", err)
	}
	if !waitForCount(t, runSQL, ch2.NativeAddr, "1000", 60*time.Second) {
		dumpReplica(t, runSQL, ch2.NativeAddr, "ch-2")
		t.Fatalf("ch-2 did not replicate to 1000 rows (mTLS mesh path failed)")
	}
	t.Log("  ✓ forward direction: mesh egress→mTLS→ingress→ch-1 succeeded")

	t.Log("ch-2 INSERT 500 → ch-1 must fetch via gw-1 egress → mTLS → gw-2 ingress → ch-2")
	if _, err := runSQL(ch2.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'mesh-rev' FROM numbers(500)"); err != nil {
		t.Fatalf("insert on ch-2: %v", err)
	}
	if !waitForCount(t, runSQL, ch1.NativeAddr, "1500", 60*time.Second) {
		dumpReplica(t, runSQL, ch1.NativeAddr, "ch-1")
		t.Fatalf("ch-1 did not replicate to 1500 rows (reverse mTLS mesh path failed)")
	}
	t.Log("  ✓ reverse direction: both mesh sidecars are on the path, mTLS authenticated both legs")
}

// waitForCount polls SELECT count() on addr until it equals want or the
// budget expires. ReplicatedMergeTree fetches parts asynchronously in a
// background queue, so this is more robust than a one-shot SYNC REPLICA
// (which blocks and can be killed by the per-query timeout if a fetch is
// stuck).
func waitForCount(t *testing.T, runSQL func(addr, db, q string) (string, error), addr, want string, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		out, err := runSQL(addr, "default", "SELECT count() FROM testdb.events")
		if err == nil && strings.TrimSpace(out) == want {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// dumpReplica logs the replication-queue exceptions and replica health for
// a node — the diagnostic surface when a part fetch through the mesh fails
// to complete.
func dumpReplica(t *testing.T, runSQL func(addr, db, q string) (string, error), addr, name string) {
	t.Helper()
	q, _ := runSQL(addr, "default",
		"SELECT type, num_tries, last_exception FROM system.replication_queue WHERE database='testdb' FORMAT Vertical")
	t.Logf("  %s replication_queue:\n%s", name, q)
	r, _ := runSQL(addr, "default",
		"SELECT is_readonly, is_session_expired, active_replicas, total_replicas, last_queue_update_exception FROM system.replicas WHERE database='testdb' FORMAT Vertical")
	t.Logf("  %s replicas:\n%s", name, r)
}
