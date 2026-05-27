package testenv

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartClickHouseInterserver starts a single ClickHouse container with its
// interserver port (9009) exposed and bound on all interfaces, and returns
// a host-reachable "host:port" for it. Used by the interserver-gateway
// integration test, which puts the housegate gateway (in the test process)
// in front of this real interserver listener.
//
// The stock clickhouse-server:25.8 image binds only loopback by default
// (see tests/keeper-testbed: the same gotcha), so we drop in a config.d
// override that opens 0.0.0.0. No keeper / replication is configured — the
// test only needs a real interserver HTTP listener to relay to.
func StartClickHouseInterserver(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	const listenXML = `<clickhouse><listen_host replace="replace">0.0.0.0</listen_host></clickhouse>`
	req := testcontainers.ContainerRequest{
		Image:        chImage,
		ExposedPorts: []string{"9009/tcp"},
		Files: []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(listenXML),
			ContainerFilePath: "/etc/clickhouse-server/config.d/zz-listen.xml",
			FileMode:          0o644,
		}},
		WaitingFor: wait.ForListeningPort("9009/tcp").WithStartupTimeout(60 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start ClickHouse (interserver): %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("ClickHouse host: %v", err)
	}
	mp, err := ctr.MappedPort(ctx, "9009/tcp")
	if err != nil {
		t.Fatalf("ClickHouse 9009 mapped port: %v", err)
	}
	addr := net.JoinHostPort(hostStr(host), mp.Port())

	// The wait strategy fires when the port is open; give the interserver
	// HTTP handler a beat to be fully ready before the test dials it.
	if err := waitNativeTCP(ctx, addr, 10*time.Second); err != nil {
		t.Fatalf("wait interserver TCP %s: %v", addr, err)
	}
	return addr
}
