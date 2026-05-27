package integration

import (
	"context"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
	"housegate/housegate/pkg/interserver"
)

// TestInterserverProxy exercises pkg/interserver (link B of the keeper-pool
// design) against a real clickhouse-server:25.8 interserver listener. It
// pins the gateway's behaviour: it relays the real CH interserver HTTP
// surface, and its IP allowlist refuses sources outside the configured
// CIDRs before any bytes reach CH. (End-to-end part replication through two
// gateways is the docker testbed's domain; here we keep to the gateway's
// own contract against a real interserver port.)
func TestInterserverProxy(t *testing.T) {
	chInterserver := testenv.StartClickHouseInterserver(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("RelaysToRealCHInterserver", func(t *testing.T) {
		srv, err := interserver.NewServer(interserver.ServerConfig{
			Target: func() string { return chInterserver },
		})
		if err != nil {
			t.Fatalf("interserver.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("gateway listen: %v", err)
		}
		go func() { _ = srv.Serve(ctx, ln) }()

		resp := httpGetRaw(t, ln.Addr().String())
		if !strings.HasPrefix(resp, "HTTP/") {
			t.Fatalf("expected an HTTP response relayed from CH interserver, got %q", resp)
		}
		if srv.Served() < 1 {
			t.Errorf("Served = %d, want >= 1", srv.Served())
		}
		if _, down := srv.Bytes(); down == 0 {
			t.Errorf("bytesDown = 0, want > 0 (CH response should flow back through the gateway)")
		}
	})

	t.Run("AllowlistRejectsForeignSource", func(t *testing.T) {
		_, allowOnly10, _ := net.ParseCIDR("10.0.0.0/8") // excludes loopback
		srv, err := interserver.NewServer(interserver.ServerConfig{
			Target:     func() string { return chInterserver },
			AllowCIDRs: []*net.IPNet{allowOnly10},
		})
		if err != nil {
			t.Fatalf("interserver.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("gateway listen: %v", err)
		}
		go func() { _ = srv.Serve(ctx, ln) }()

		c, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial gateway: %v", err)
		}
		defer c.Close()
		_, _ = c.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 16)
		if n, rerr := c.Read(buf); rerr == nil && n > 0 {
			t.Fatalf("expected rejected connection (EOF), got %d bytes: %q", n, buf[:n])
		}
		if srv.Rejected() < 1 {
			t.Errorf("Rejected = %d, want >= 1", srv.Rejected())
		}
	})
}

// TestInterserverReplication exercises the full ReplicatedMergeTree
// part-replication path through two housegate interserver gateways
// (link B of the keeper-pool design):
//
//	3-node Keeper quorum         (keeper-1/2/3)
//	2-node ReplicatedMergeTree   (ch-1, ch-2)
//	2 interserver gateways        (gw-1, gw-2 — containers on the keeper net)
//
// Each CH advertises its co-located gateway alias (gw-1 / gw-2) as its
// interserver host, so a peer fetching a part connects to that gateway,
// which forwards to the source CH's real interserver (9009). Everything is
// container-to-container on the keeper network — the gateway runs in a
// container (not the test process) because a CH container cannot reliably
// dial back to a host process on a user-defined network.
//
// A part only ever flows IN through the SOURCE node's gateway, so we insert
// in BOTH directions to exercise gw-1 (ch-2 fetches from ch-1) and gw-2
// (ch-1 fetches from ch-2). Replication succeeding proves the fetch went
// through the gateway: a CH only ever learns the peer's advertised gateway
// address, never the peer's real interserver port.
func TestInterserverReplication(t *testing.T) {
	cluster := testenv.StartKeeperCluster(t, 3)

	// CH nodes advertise their co-located gateway alias as the interserver
	// host; their real interserver (9009) stays in-network.
	ch1 := testenv.StartClickHouseReplicatedNode(t, cluster, "ch-1", "gw-1")
	ch2 := testenv.StartClickHouseReplicatedNode(t, cluster, "ch-2", "gw-2")

	// Gateways forward to each co-located CH's in-network interserver.
	testenv.StartInterserverGateway(t, cluster, "gw-1", "ch-1:9009")
	testenv.StartInterserverGateway(t, cluster, "gw-2", "ch-2:9009")

	bin := testenv.ClickHouseCLI(t)

	// runSQL executes a query against a CH node via clickhouse-client
	// with the explicit password set in the container (see
	// CLICKHOUSE_PASSWORD in StartClickHouseReplicatedNode).
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

	// ---- Create ReplicatedMergeTree table on both replicas ----------
	t.Log("creating ReplicatedMergeTree on both replicas")
	tblDDL := "CREATE TABLE testdb.events (ts DateTime, uid UInt64, data String) " +
		"ENGINE=ReplicatedMergeTree('/clickhouse/tables/01/testdb/events', '{replica}') " +
		"ORDER BY (ts, uid)"

	for _, c := range []struct {
		addr string
		name string
	}{{ch1.NativeAddr, "ch-1"}, {ch2.NativeAddr, "ch-2"}} {
		t.Logf("  connecting to %s at %s", c.name, c.addr)
		var lastErr error
		for attempt := 1; attempt <= 20; attempt++ {
			out, err := runSQL(c.addr, "default",
				"CREATE DATABASE IF NOT EXISTS testdb")
			if err != nil {
				lastErr = err
				t.Logf("  %s attempt %d: CREATE DATABASE failed: %v, out=%q", c.name, attempt, err, out)
				time.Sleep(2 * time.Second)
				continue
			}
			out, err = runSQL(c.addr, "default", tblDDL)
			if err != nil {
				lastErr = err
				t.Logf("  %s attempt %d: CREATE TABLE failed: %v, out=%q", c.name, attempt, err, out)
				time.Sleep(2 * time.Second)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			t.Fatalf("failed to create table on %s after 20 attempts: %v", c.name, lastErr)
		}
		t.Logf("  %s: table ready", c.name)
	}

	// ---- Baseline: insert on ch-1, read on ch-2 (through gateways) --
	t.Log("inserting 1000 rows on ch-1")
	if _, err := runSQL(ch1.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'baseline' FROM numbers(1000)"); err != nil {
		t.Fatalf("insert on ch-1: %v", err)
	}

	t.Log("waiting for ch-2 to replicate the part (fetch from ch-1 through gateway-1)")
	if !waitForCount(t, runSQL, ch2.NativeAddr, "1000", 60*time.Second) {
		dumpReplica(t, runSQL, ch2.NativeAddr, "ch-2")
		t.Fatalf("ch-2 did not replicate to 1000 rows within 60s (fetch through gw-1 failed)")
	}
	t.Log("  ch-2 reached 1000 rows — fetch from ch-1 went through gateway-1")

	// ---- Reverse direction: insert on ch-2, read on ch-1 -------------
	// This makes ch-1 fetch the new part from ch-2, exercising gateway-2
	// (a part fetch only flows through the gateway of the node that HAS
	// the part, so the baseline above only touched gateway-1).
	t.Log("inserting 500 rows on ch-2 (ch-1 then fetches through gateway-2)")
	if _, err := runSQL(ch2.NativeAddr, "default",
		"INSERT INTO testdb.events SELECT now(), number, 'reverse' FROM numbers(500)"); err != nil {
		t.Fatalf("insert on ch-2: %v", err)
	}
	if !waitForCount(t, runSQL, ch1.NativeAddr, "1500", 60*time.Second) {
		dumpReplica(t, runSQL, ch1.NativeAddr, "ch-1")
		t.Fatalf("ch-1 did not replicate to 1500 rows within 60s (fetch through gw-2 failed)")
	}
	t.Log("  ch-1 reached 1500 rows — fetch from ch-2 went through gateway-2")
	// Both directions replicated, so both gateways relayed a real part
	// fetch (a CH only ever learns the peer's advertised gateway alias).
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
// a node — the diagnostic surface when a part fetch through the gateway
// fails to complete.
func dumpReplica(t *testing.T, runSQL func(addr, db, q string) (string, error), addr, name string) {
	t.Helper()
	q, _ := runSQL(addr, "default",
		"SELECT type, num_tries, last_exception FROM system.replication_queue WHERE database='testdb' FORMAT Vertical")
	t.Logf("  %s replication_queue:\n%s", name, q)
	r, _ := runSQL(addr, "default",
		"SELECT is_readonly, is_session_expired, active_replicas, total_replicas, last_queue_update_exception FROM system.replicas WHERE database='testdb' FORMAT Vertical")
	t.Logf("  %s replicas:\n%s", name, r)
}

// httpGetRaw sends a minimal HTTP/1.0 GET over a fresh connection to addr
// and returns whatever bytes come back (the relayed CH interserver reply).
func httpGetRaw(t *testing.T, addr string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte("GET / HTTP/1.0\r\nHost: housegate-test\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	b, err := io.ReadAll(c)
	if err != nil && len(b) == 0 {
		t.Fatalf("read response: %v", err)
	}
	return string(b)
}
