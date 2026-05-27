package testenv

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

const keeperImage = "clickhouse/clickhouse-keeper:25.8"

// KeeperCluster is an N-node clickhouse-keeper Raft quorum on a private
// docker network. Unlike the ClickHouse helpers (which run unrelated
// instances), these nodes DO coordinate — they form one quorum — because
// the keeper proxy is specifically about steering across a live quorum and
// surviving membership changes.
//
// Endpoints[i] is a host-reachable "host:port" for node i's client port
// (9181); the keeper proxy under test (running in the test process on the
// host) uses these as its member set. Raft (9234) stays container-to-
// container over the private network and is never exposed to the host.
type KeeperCluster struct {
	Endpoints  []string
	containers []testcontainers.Container
	net        *testcontainers.DockerNetwork
}

// StartKeeperCluster starts an n-node keeper quorum and waits until a
// leader is elected. Cleanup is registered via t.Cleanup.
func StartKeeperCluster(t *testing.T, n int) *KeeperCluster {
	t.Helper()
	kc, cleanup, err := startKeeperCluster(context.Background(), n)
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatalf("start keeper cluster: %v", err)
	}
	t.Cleanup(cleanup)
	kc.WaitForLeader(t, 90*time.Second)
	return kc
}

// Stop halts node idx without removing it (failover scenarios). Terminate
// at cleanup is idempotent against an already-stopped container.
func (kc *KeeperCluster) Stop(t *testing.T, idx int) {
	t.Helper()
	timeout := 2 * time.Second
	if err := kc.containers[idx].Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stop keeper node %d: %v", idx, err)
	}
}

// IndexOf returns the node index whose endpoint equals addr, or -1.
func (kc *KeeperCluster) IndexOf(addr string) int {
	for i, e := range kc.Endpoints {
		if e == addr {
			return i
		}
	}
	return -1
}

// WaitForLeader polls the cluster until some node reports leader, or fails.
func (kc *KeeperCluster) WaitForLeader(t *testing.T, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		for _, e := range kc.Endpoints {
			if st := keeperServerState(e, time.Second); st == "leader" || st == "standalone" {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("keeper cluster did not elect a leader within %s", budget)
}

func startKeeperCluster(ctx context.Context, n int) (*KeeperCluster, func(), error) {
	dnet, err := network.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create network: %w", err)
	}
	cleanups := []func(){func() { _ = dnet.Remove(context.Background()) }}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	containers := make([]testcontainers.Container, n)
	endpoints := make([]string, n)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		i := i
		alias := fmt.Sprintf("keeper-%d", i+1)
		g.Go(func() error {
			req := testcontainers.ContainerRequest{
				Image:          keeperImage,
				ExposedPorts:   []string{"9181/tcp"},
				Networks:       []string{dnet.Name},
				NetworkAliases: map[string][]string{dnet.Name: {alias}},
				Files: []testcontainers.ContainerFile{{
					Reader:            strings.NewReader(keeperConfigXML(i+1, n)),
					ContainerFilePath: "/etc/clickhouse-keeper/keeper_config.xml",
					FileMode:          0o644,
				}},
				WaitingFor: wait.ForListeningPort("9181/tcp").WithStartupTimeout(60 * time.Second),
			}
			ctr, err := testcontainers.GenericContainer(gctx, testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
			if err != nil {
				return fmt.Errorf("%s: %w", alias, err)
			}
			containers[i] = ctr
			host, err := ctr.Host(gctx)
			if err != nil {
				return fmt.Errorf("%s host: %w", alias, err)
			}
			mp, err := ctr.MappedPort(gctx, "9181/tcp")
			if err != nil {
				return fmt.Errorf("%s mapped port: %w", alias, err)
			}
			endpoints[i] = net.JoinHostPort(hostStr(host), mp.Port())
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		for _, c := range containers {
			if c != nil {
				cleanups = append(cleanups, func() { _ = c.Terminate(context.Background()) })
			}
		}
		cleanup()
		return nil, nil, err
	}
	for _, c := range containers {
		c := c
		cleanups = append(cleanups, func() { _ = c.Terminate(context.Background()) })
	}

	return &KeeperCluster{Endpoints: endpoints, containers: containers, net: dnet}, cleanup, nil
}

// hostStr normalises testcontainers' host (sometimes "localhost") to a dial
// target. localhost is fine for net.Dial; kept as a hook for environments
// that need an override.
func hostStr(h string) string {
	if h == "" {
		return "127.0.0.1"
	}
	return h
}

// keeperConfigXML renders a keeper config for node id within an n-node
// ensemble. Raft peers are addressed by their network aliases keeper-1..n.
func keeperConfigXML(id, n int) string {
	var raft strings.Builder
	for j := 1; j <= n; j++ {
		fmt.Fprintf(&raft,
			"            <server><id>%d</id><hostname>keeper-%d</hostname><port>9234</port></server>\n",
			j, j)
	}
	return fmt.Sprintf(`<clickhouse>
    <logger><level>warning</level><console>true</console></logger>
    <listen_host>0.0.0.0</listen_host>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>%d</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <enable_reconfiguration>true</enable_reconfiguration>
        <four_letter_word_white_list>*</four_letter_word_white_list>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <snapshot_distance>50</snapshot_distance>
        </coordination_settings>
        <raft_configuration>
%s        </raft_configuration>
    </keeper_server>
</clickhouse>
`, id, raft.String())
}

// keeperServerState returns the zk_server_state reported by mntr (or "" on
// any error). A small 4LW helper kept local to testenv so the harness has
// no dependency on pkg/keeper internals.
func keeperServerState(addr string, timeout time.Duration) string {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return ""
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte("mntr")); err != nil {
		return ""
	}
	b, _ := io.ReadAll(conn)
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "\t"); ok && k == "zk_server_state" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
