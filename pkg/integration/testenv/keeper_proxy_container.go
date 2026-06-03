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

// StartKeeperProxy runs the pkg/keeper proxy (via the bazel-built cmd/kpx
// binary) in a container on the keeper cluster's Docker network, reachable
// by CH at <alias>:9181. It fronts the whole quorum (members keeper-1..n)
// and steers each connection to a live member.
//
// A ClickHouse pointing its <zookeeper> at this alias therefore sends ALL
// its keeper-client (9181) traffic through housegate — the "transparent
// quorum endpoint": CH knows one stable address, quorum membership churn is
// absorbed behind the proxy. Running it as a container keeps every hop
// container-to-container (CH → kpx → keeper).
//
// Returns a host-reachable "host:port" so host-side test code (e.g. the
// orchestrator integration test dialing real ZK) can reach the proxy too.
func StartKeeperProxy(t *testing.T, cluster *KeeperCluster, alias string) string {
	t.Helper()
	ctx := context.Background()
	bin := runfileBinary(t, "pkg/integration/testenv/cmd/kpx/kpx_/kpx")
	dnet := cluster.DockerNetwork()

	members := make([]string, 0, len(cluster.Aliases))
	for _, a := range cluster.Aliases {
		members = append(members, a+":9181")
	}

	req := testcontainers.ContainerRequest{
		Image:          "alpine:3.20",
		ExposedPorts:   []string{"9181/tcp"}, // so the wait strategy + host-side tests can reach the proxy
		Networks:       []string{dnet.Name},
		NetworkAliases: map[string][]string{dnet.Name: {alias}},
		Files: []testcontainers.ContainerFile{{
			HostFilePath:      bin,
			ContainerFilePath: "/kpx",
			FileMode:          0o755,
		}},
		Cmd:        []string{"/kpx", "-listen", ":9181", "-members", strings.Join(members, ",")},
		WaitingFor: wait.ForListeningPort("9181/tcp").WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start keeper proxy %s: %v", alias, err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("keeper proxy %s host: %v", alias, err)
	}
	mp, err := ctr.MappedPort(ctx, "9181/tcp")
	if err != nil {
		t.Fatalf("keeper proxy %s mapped port: %v", alias, err)
	}
	return net.JoinHostPort(hostStr(host), mp.Port())
}
