package integration

import (
	"context"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"

	"housegate/housegate/pkg/integration/testenv"
)

// TestMultiShardKeeperIsolation exercises architecture.md §6: two
// independent keeper-pool shards, two user databases each bound to a
// different shard via the ReplicatedMergeTree path prefix that maps onto
// CH's <auxiliary_zookeepers>. The test proves:
//
//  1. each CH replica configures BOTH shards (one primary <zookeeper>,
//     one <auxiliary_zookeepers>) and routes a table's coordination to
//     the shard named in its DDL path;
//  2. each shard has its own pkg/keeper proxy in front (kpxA-1/2 +
//     kpxB-1/2), so traffic for the user-A database only ever touches
//     keeper cluster A and the user-B database only ever touches B;
//  3. taking down cluster A's whole quorum makes the user-A table go
//     read-only / fail to replicate, but the user-B table KEEPS
//     replicating — blast-radius isolation between shards.
//
// (CH-to-CH part replication still goes through the imesh sidecars from
// TestInterserverMeshReplication, so this test also incidentally validates
// that the mTLS mesh works with multiple keeper shards.)
func TestMultiShardKeeperIsolation(t *testing.T) {
	ctx := context.Background()

	// Shared docker network so the two keeper clusters AND the four
	// keeper-proxy sidecars AND both CHs (plus their imesh sidecars) all
	// live on one address space and reach each other by alias.
	dnet, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = dnet.Remove(context.Background()) })

	// Two 3-node keeper clusters with distinct alias prefixes so their
	// keeper-N hostnames don't collide.
	clusterA := testenv.StartKeeperClusterOn(t, dnet, "kA", 3)
	clusterB := testenv.StartKeeperClusterOn(t, dnet, "kB", 3)

	// Two keeper proxies per CH, one fronting each cluster.
	testenv.StartKeeperProxy(t, clusterA, "kpxA-1")
	testenv.StartKeeperProxy(t, clusterA, "kpxA-2")
	testenv.StartKeeperProxy(t, clusterB, "kpxB-1")
	testenv.StartKeeperProxy(t, clusterB, "kpxB-2")

	// Each CH advertises kpxA-N as its primary <zookeeper> and kpxB-N as
	// an <auxiliary_zookeepers> shard named "kshardB". ReplicatedMergeTree
	// paths with an un-prefixed start (e.g. "/clickhouse/tables/...") use
	// the primary; paths prefixed "kshardB:/..." use the aux shard.
	const auxName = "kshardB"
	ch1 := testenv.StartClickHouseMeshReplica(t, clusterA, "ch-1", "kpxA-1:9181",
		map[string]string{auxName: "kpxB-1:9181"})
	ch2 := testenv.StartClickHouseMeshReplica(t, clusterA, "ch-2", "kpxA-2:9181",
		map[string]string{auxName: "kpxB-2:9181"})

	// Mesh sidecars so part-replication actually works.
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

	// Two user databases: db_user_a coordinates through the primary
	// (kpxA → clusterA); db_user_b through the auxiliary (kpxB → clusterB).
	t.Log("creating db_user_a (shard A) and db_user_b (shard B) on both replicas")
	createOnBoth := func(database, engine string) {
		t.Helper()
		ddlDb := "CREATE DATABASE IF NOT EXISTS " + database
		ddlTbl := "CREATE TABLE IF NOT EXISTS " + database + ".events " +
			"(ts DateTime, uid UInt64, data String) ENGINE=" + engine +
			" ORDER BY (ts, uid)"
		for _, c := range []struct{ addr, name string }{{ch1.NativeAddr, "ch-1"}, {ch2.NativeAddr, "ch-2"}} {
			var lastErr error
			for attempt := 1; attempt <= 20; attempt++ {
				if _, err := runSQL(c.addr, "default", ddlDb); err != nil {
					lastErr = err
					time.Sleep(2 * time.Second)
					continue
				}
				if _, err := runSQL(c.addr, "default", ddlTbl); err != nil {
					lastErr = err
					time.Sleep(2 * time.Second)
					continue
				}
				lastErr = nil
				break
			}
			if lastErr != nil {
				t.Fatalf("failed to create %s.events on %s: %v", database, c.name, lastErr)
			}
		}
	}
	createOnBoth("db_user_a",
		`ReplicatedMergeTree('/clickhouse/tables/01/db_user_a/events', '{replica}')`)
	createOnBoth("db_user_b",
		`ReplicatedMergeTree('`+auxName+`:/clickhouse/tables/01/db_user_b/events', '{replica}')`)

	// Both DBs replicate through their respective shard.
	t.Log("baseline: insert in db_user_a (shard A) and db_user_b (shard B); both must replicate")
	if _, err := runSQL(ch1.NativeAddr, "db_user_a",
		"INSERT INTO events SELECT now(), number, 'a' FROM numbers(100)"); err != nil {
		t.Fatalf("insert db_user_a on ch-1: %v", err)
	}
	if _, err := runSQL(ch1.NativeAddr, "db_user_b",
		"INSERT INTO events SELECT now(), number, 'b' FROM numbers(100)"); err != nil {
		t.Fatalf("insert db_user_b on ch-1: %v", err)
	}
	if !waitForDbCount(t, runSQL, ch2.NativeAddr, "db_user_a", "100", 60*time.Second) {
		dumpDbReplica(t, runSQL, ch2.NativeAddr, "db_user_a", "ch-2")
		t.Fatalf("db_user_a (shard A) did not replicate to ch-2")
	}
	if !waitForDbCount(t, runSQL, ch2.NativeAddr, "db_user_b", "100", 60*time.Second) {
		dumpDbReplica(t, runSQL, ch2.NativeAddr, "db_user_b", "ch-2")
		t.Fatalf("db_user_b (shard B) did not replicate to ch-2")
	}
	t.Log("  ✓ both DBs replicated through their respective keeper shards")

	// Blast-radius isolation: kill all of cluster A.
	t.Log("killing every node of keeper cluster A — shard A goes read-only, shard B must keep working")
	for i := range clusterA.Aliases {
		clusterA.Stop(t, i)
	}

	// db_user_a must go read-only / refuse new inserts (its keeper shard
	// has no quorum). Inserts may either error or hang; we accept either
	// as long as they don't silently succeed.
	insertCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(insertCtx, bin, "client",
		"--host", strings.Split(ch1.NativeAddr, ":")[0],
		"--port", strings.Split(ch1.NativeAddr, ":")[1],
		"--user", "default", "--password", "housegate_test_pw",
		"--database", "db_user_a",
		"--query", "INSERT INTO events SELECT now(), number, 'a-after-killA' FROM numbers(50)",
	)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Errorf("insert into db_user_a unexpectedly succeeded after cluster A was killed (out=%q)", out)
	} else {
		t.Logf("  ✓ db_user_a insert failed as expected: %v", err)
	}

	// db_user_b must still accept inserts and replicate.
	t.Log("inserting into db_user_b (shard B); replication must keep working")
	if _, err := runSQL(ch1.NativeAddr, "db_user_b",
		"INSERT INTO events SELECT now(), number, 'b-after-killA' FROM numbers(50)"); err != nil {
		t.Fatalf("insert db_user_b on ch-1 after killing cluster A: %v", err)
	}
	if !waitForDbCount(t, runSQL, ch2.NativeAddr, "db_user_b", "150", 60*time.Second) {
		dumpDbReplica(t, runSQL, ch2.NativeAddr, "db_user_b", "ch-2")
		t.Fatalf("db_user_b did not replicate to 150 rows after cluster A killed (shard isolation broken)")
	}
	t.Logf("  ✓ db_user_b replicated to 150 rows — keeper shard isolation works")
}

// waitForDbCount mirrors waitForCount but lets the test pick which
// database (and shard) to poll against.
func waitForDbCount(t *testing.T, runSQL func(addr, db, q string) (string, error), addr, database, want string, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		out, err := runSQL(addr, database, "SELECT count() FROM events")
		if err == nil && strings.TrimSpace(out) == want {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// dumpDbReplica is the multi-DB sibling of dumpReplica.
func dumpDbReplica(t *testing.T, runSQL func(addr, db, q string) (string, error), addr, database, name string) {
	t.Helper()
	q, _ := runSQL(addr, "default",
		"SELECT database, type, num_tries, last_exception FROM system.replication_queue WHERE database='"+database+"' FORMAT Vertical")
	t.Logf("  %s replication_queue (%s):\n%s", name, database, q)
	r, _ := runSQL(addr, "default",
		"SELECT database, is_readonly, is_session_expired, active_replicas, total_replicas FROM system.replicas WHERE database='"+database+"' FORMAT Vertical")
	t.Logf("  %s replicas (%s):\n%s", name, database, r)
}
