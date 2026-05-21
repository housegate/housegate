package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/cluster"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
)

// TestReplicaRouting_BothReachable starts a proxy with TWO upstream
// replicas and verifies that round-robin actually visits both.
//
// Each replica owns the same table name but a different marker value
// inserted directly (bypassing the proxy). When the proxy load-balances
// queries across replicas we expect to see both marker values in the
// returned rows; if it pins to a single replica we only ever see one.
func TestReplicaRouting_BothReachable(t *testing.T) {
	a, b := testenv.StartClickHousePair(t)

	// Seed each replica with a distinct marker. We connect directly to
	// the container so the seed itself does not depend on the proxy.
	seedMarker(t, a, 100)
	seedMarker(t, b, 200)

	proxy := testenv.StartReplicatedProxy(t, a, b)

	// 20 attempts gives us a vanishingly small chance of one side being
	// missed by chance — round-robin should land on each side ≈10 times.
	const attempts = 20
	seen := map[uint64]int{}
	for i := 0; i < attempts; i++ {
		v := selectMarker(t, proxy.Addr)
		seen[v]++
	}

	if seen[100] == 0 || seen[200] == 0 {
		t.Fatalf("expected both replicas to be hit across %d attempts, got: %v",
			attempts, seen)
	}
	t.Logf("routing distribution over %d attempts: A=100→%d, B=200→%d",
		attempts, seen[100], seen[200])
}

// TestReplicaRouting_FailoverOnDown verifies that when one of the two
// replicas becomes unreachable mid-run, the proxy stops sending traffic
// to it. The cluster manager defaults (5s probe, 3 consecutive failures)
// take ~15s to detect a downed replica, which is too coarse for a unit
// test — we tighten the health check via WithConfigMutator so the
// observation surfaces in seconds.
//
// Both replicas seed the same marker value so the test does not care
// which replica answered; the assertion is "queries continue to
// succeed once the proxy has caught up to the failure."
func TestReplicaRouting_FailoverOnDown(t *testing.T) {
	a, b := testenv.StartClickHousePair(t)

	seedMarker(t, a, 1)
	seedMarker(t, b, 1)

	// Aggressive health-check tuning: 200ms probe, 1 failure marks the
	// replica unhealthy, 1s probe timeout. With these values a stopped
	// container should be detected within ~1.2s. Recovery threshold
	// stays at the default — we never expect B to come back in this test.
	tightHealthCheck := testenv.WithConfigMutator(func(cfg *config.Config) {
		cfg.HealthCheck = &cluster.HealthCheckConfig{
			Interval:          cluster.Duration{Duration: 200 * time.Millisecond},
			Timeout:           cluster.Duration{Duration: 1 * time.Second},
			FailureThreshold:  1,
			RecoveryThreshold: 2,
		}
	})

	proxy := testenv.StartReplicatedProxy(t, a, b, tightHealthCheck)

	// Warm-up: prove the proxy reaches both replicas before we kill one.
	// Without this we cannot distinguish "failover works" from "we only
	// ever talked to A and never noticed B".
	for i := 0; i < 4; i++ {
		_ = selectMarker(t, proxy.Addr)
	}

	b.Stop(t)
	// Wait long enough for the active health checker to mark B
	// unhealthy. 3s gives ~15 probe cycles at 200ms — plenty of margin
	// over the 1-failure threshold + 1s probe timeout worst case.
	time.Sleep(3 * time.Second)

	const attempts = 10
	failures := 0
	for i := 0; i < attempts; i++ {
		if _, err := trySelectMarker(t, proxy.Addr); err != nil {
			failures++
			t.Logf("attempt %d failed: %v", i+1, err)
		}
	}
	if failures > 0 {
		t.Errorf("expected all %d queries to succeed after replica B down, %d failed",
			attempts, failures)
	}
}

// seedMarker connects directly to the given replica (bypassing the
// proxy) and writes a row into a shared "marker_test" Memory table.
// Engine=Memory is intentional: no disk, no replication, no setup;
// gone when the container dies.
func seedMarker(t *testing.T, env *testenv.ClickHouseEnv, value uint64) {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{env.Addr},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.User,
			Password: env.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("seedMarker: open %s: %v", env.Addr, err)
	}
	defer conn.Close()

	ctx := context.Background()
	if err := conn.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS marker_test (v UInt64) ENGINE=Memory",
	); err != nil {
		t.Fatalf("seedMarker: create table on %s: %v", env.Addr, err)
	}
	if err := conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO marker_test VALUES (%d)", value),
	); err != nil {
		t.Fatalf("seedMarker: insert on %s: %v", env.Addr, err)
	}
}

// selectMarker opens a FRESH connection to the proxy on every call so
// the cluster manager picks a new replica each invocation. clickhouse-go
// keeps idle connections in a pool by default; reusing a Conn would let
// the proxy keep us pinned to whichever replica handled the handshake.
//
// Fails the test on error — use trySelectMarker when you need the call
// site to handle failure.
func selectMarker(t *testing.T, proxyAddr string) uint64 {
	t.Helper()
	v, err := trySelectMarker(t, proxyAddr)
	if err != nil {
		t.Fatalf("selectMarker: %v", err)
	}
	return v
}

func trySelectMarker(t *testing.T, proxyAddr string) (uint64, error) {
	t.Helper()
	// chEnv shares chUser/chPassword/chDatabase with every container
	// testenv starts (they are package-level constants), so the
	// auth values are valid against the pair-mode proxy too — they
	// describe how the proxy was told to authenticate to upstream,
	// not which container the client is connecting through.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return 0, fmt.Errorf("open proxy: %w", err)
	}
	defer conn.Close()

	var v uint64
	if err := conn.QueryRow(context.Background(),
		"SELECT v FROM marker_test LIMIT 1",
	).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

// TestReplicaRouting_Random verifies the "random" load-balancing
// strategy reaches both replicas. The distribution will not be as
// uniform as round-robin's 10:10, but over 30 attempts the probability
// of missing one side entirely is (1/2)^30 — vanishing.
func TestReplicaRouting_Random(t *testing.T) {
	a, b := testenv.StartClickHousePair(t)
	seedMarker(t, a, 100)
	seedMarker(t, b, 200)

	proxy := testenv.StartReplicatedProxy(t, a, b,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Routing = &cluster.RoutingConfig{Strategy: "random"}
		}),
	)

	const attempts = 30
	seen := map[uint64]int{}
	for i := 0; i < attempts; i++ {
		seen[selectMarker(t, proxy.Addr)]++
	}
	if seen[100] == 0 || seen[200] == 0 {
		t.Fatalf("random strategy missed a replica across %d attempts: %v",
			attempts, seen)
	}
	t.Logf("random distribution over %d attempts: A=100→%d, B=200→%d",
		attempts, seen[100], seen[200])
}

// TestReplicaRouting_Weighted verifies that ReplicaConfig.Weight
// actually skews the distribution. Setting B's weight to 9× A's
// weight should make B answer ~9× as often. We assert the much
// weaker "B answered at least 3× as often as A" so the test does
// not flake on small-sample variance.
//
// Expected over 50 attempts at 1:9: A≈5, B≈45. Binomial σ≈2.1, so
// the B/A ≥ 3 floor sits well outside any realistic noise band but
// fails loudly if the weighted strategy is silently equivalent to
// round-robin.
func TestReplicaRouting_Weighted(t *testing.T) {
	a, b := testenv.StartClickHousePair(t)
	seedMarker(t, a, 100)
	seedMarker(t, b, 200)

	proxy := testenv.StartReplicatedProxy(t, a, b,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Routing = &cluster.RoutingConfig{Strategy: "weighted"}
			// cfg.Shard was just populated by buildReplicatedServerConfig;
			// adjust the replica weights in-place to 1:9.
			cfg.Shard.Replicas[0].Weight = 1
			cfg.Shard.Replicas[1].Weight = 9
		}),
	)

	const attempts = 50
	seen := map[uint64]int{}
	for i := 0; i < attempts; i++ {
		seen[selectMarker(t, proxy.Addr)]++
	}
	if seen[100] == 0 || seen[200] == 0 {
		t.Fatalf("weighted strategy missed a replica across %d attempts: %v",
			attempts, seen)
	}
	if seen[200] < seen[100]*3 {
		t.Errorf("expected B (weight 9) to receive ≥3× A (weight 1); got A=%d B=%d",
			seen[100], seen[200])
	}
	t.Logf("weighted 1:9 distribution over %d attempts: A=100→%d, B=200→%d",
		attempts, seen[100], seen[200])
}
