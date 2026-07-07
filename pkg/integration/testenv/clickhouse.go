// Package testenv brings up ClickHouse containers for integration tests.
//
// All containers run independently — no Keeper / Zookeeper, no replication
// coordination — because housegate is an L7 proxy and only cares about
// routing connections to a set of equivalent upstream addresses. When a
// test needs "two replicas", it should treat the pair as two independent
// CH instances; data consistency between them is not testenv's concern.
package testenv

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"golang.org/x/sync/errgroup"
)

const (
	chImage    = "clickhouse/clickhouse-server:26.3"
	chUser     = "housegate_test"
	chPassword = "housegate_test_pw"
	chDatabase = "housegate_test"
)

// ClickHouseEnv holds connection parameters for a running ClickHouse container.
type ClickHouseEnv struct {
	// Addr is "host:port" for the native TCP port (9000).
	Addr     string
	User     string
	Password string
	Database string

	// ctr is the underlying testcontainers handle. Held so that Stop
	// can ask docker to halt the container mid-test (failover scenarios)
	// without going through full Terminate. nil for envs returned from
	// older code paths that did not store the ref.
	ctr *tcclickhouse.ClickHouseContainer
}

// Stop halts the underlying container without removing it. Used by
// failover tests that need a replica to become unreachable mid-run.
// The container is still cleaned up at test teardown by t.Cleanup
// (Terminate is idempotent against an already-stopped container).
//
// The timeout bounds how long docker waits for graceful shutdown before
// SIGKILL — 1s is plenty for "make this CH instance disappear from the
// network".
func (e *ClickHouseEnv) Stop(t *testing.T) {
	t.Helper()
	if e.ctr == nil {
		t.Fatal("ClickHouseEnv.Stop: container handle not available")
	}
	timeout := 1 * time.Second
	if err := e.ctr.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stop ClickHouse container: %v", err)
	}
}

// StartClickHouse starts a single ClickHouse container and registers cleanup
// via t.Cleanup.
func StartClickHouse(t *testing.T) *ClickHouseEnv {
	t.Helper()
	env, cleanup, err := startClickHouse(context.Background())
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(cleanup)
	return env
}

// StartClickHouseForMain is the TestMain variant: returns the env and a
// cleanup the caller must invoke before os.Exit.
func StartClickHouseForMain() (*ClickHouseEnv, func(), error) {
	return startClickHouse(context.Background())
}

// StartClickHousePair starts two independent ClickHouse containers in
// parallel and registers cleanup via t.Cleanup. The pair is intended for
// tests that exercise housegate's multi-replica routing (load balancing,
// health checks, failover) — both nodes are otherwise unrelated, share
// no state, and host whatever tables the test creates on each side
// independently.
func StartClickHousePair(t *testing.T) (*ClickHouseEnv, *ClickHouseEnv) {
	t.Helper()
	a, b, cleanup, err := startClickHousePair(context.Background())
	if err != nil {
		t.Fatalf("start ClickHouse pair: %v", err)
	}
	t.Cleanup(cleanup)
	return a, b
}

// StartClickHousePairForMain is the TestMain variant of StartClickHousePair.
func StartClickHousePairForMain() (*ClickHouseEnv, *ClickHouseEnv, func(), error) {
	return startClickHousePair(context.Background())
}

func startClickHouse(ctx context.Context) (*ClickHouseEnv, func(), error) {
	ctr, err := tcclickhouse.Run(ctx, chImage,
		tcclickhouse.WithUsername(chUser),
		tcclickhouse.WithPassword(chPassword),
		tcclickhouse.WithDatabase(chDatabase),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("run ClickHouse container: %w", err)
	}

	addr, err := ctr.ConnectionHost(ctx)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, nil, fmt.Errorf("get ClickHouse connection host: %w", err)
	}

	// testcontainers' clickhouse module waits on HTTP 8123 being
	// healthy, which is NOT the same as native TCP 9000 being ready
	// to accept connections — the native listener typically comes up
	// a few hundred ms later. Poll it explicitly so the caller's
	// first dial (clickhouse-go or a direct net.Dial in test
	// fixtures) does not race the server.
	if err := waitNativeTCP(ctx, addr, 10*time.Second); err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, nil, fmt.Errorf("wait for native TCP at %s: %w", addr, err)
	}

	cleanup := func() {
		_ = ctr.Terminate(context.Background())
	}

	return &ClickHouseEnv{
		Addr:     addr,
		User:     ctr.User,
		Password: ctr.Password,
		Database: ctr.DbName,
		ctr:      ctr,
	}, cleanup, nil
}

func startClickHousePair(ctx context.Context) (*ClickHouseEnv, *ClickHouseEnv, func(), error) {
	var (
		envA, envB         *ClickHouseEnv
		cleanupA, cleanupB func()
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		e, c, err := startClickHouse(gctx)
		if err != nil {
			return fmt.Errorf("node A: %w", err)
		}
		envA, cleanupA = e, c
		return nil
	})
	g.Go(func() error {
		e, c, err := startClickHouse(gctx)
		if err != nil {
			return fmt.Errorf("node B: %w", err)
		}
		envB, cleanupB = e, c
		return nil
	})

	if err := g.Wait(); err != nil {
		// One side failed; tear down whichever side did come up so we
		// do not leak a container.
		if cleanupA != nil {
			cleanupA()
		}
		if cleanupB != nil {
			cleanupB()
		}
		return nil, nil, nil, err
	}

	cleanup := func() {
		// Always tear both down, even if one Terminate panics or
		// errors — the other container would otherwise leak.
		if cleanupA != nil {
			cleanupA()
		}
		if cleanupB != nil {
			cleanupB()
		}
	}
	return envA, envB, cleanup, nil
}

// waitNativeTCP polls a TCP address until a dial succeeds or budget
// expires. Used to bridge the gap between testcontainers' HTTP-port
// health check and the native protocol port actually being ready —
// native 9000 typically lags HTTP 8123 by a few hundred ms.
func waitNativeTCP(ctx context.Context, addr string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("dial deadline exceeded: %w", lastErr)
	}
	return fmt.Errorf("dial deadline exceeded")
}
