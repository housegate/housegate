package integration

import (
	"context"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/integration/testenv"
)

// chEnv is the shared ClickHouse container, started once in TestMain so
// every test in the package re-uses the same upstream. Re-creating it
// per test would add ~5s × N to the suite for no benefit.
var chEnv *testenv.ClickHouseEnv

func TestMain(m *testing.M) {
	env, cleanup, err := testenv.StartClickHouseForMain()
	if err != nil {
		// Container failed to start (no docker, no network, etc.) —
		// nothing left to test. os.Exit(1) so CI flags the failure
		// loudly rather than reporting an empty pass.
		os.Stderr.WriteString("integration: " + err.Error() + "\n")
		os.Exit(1)
	}
	chEnv = env
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// openConn opens a clickhouse-go native connection to the proxy with the
// shared container's credentials.
func openConn(t *testing.T, proxyAddr string) clickhouse.Conn {
	t.Helper()
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
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestProxyForwardsSelectOne is the bare-minimum end-to-end check: client
// → proxy → ClickHouse → client, with a result whose value depends on the
// query reaching the server. Failure here means handshake, packet flow,
// or shutdown is broken at the most basic level.
func TestProxyForwardsSelectOne(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)

	var result uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if result != 1 {
		t.Errorf("SELECT 1 = %d, want 1", result)
	}
}
