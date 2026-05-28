package testenv

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ClickHouseReplicatedNode holds connection parameters for a ClickHouse
// container participating in a ReplicatedMergeTree cluster via a common
// Keeper quorum.
type ClickHouseReplicatedNode struct {
	// NativeAddr is "host:port" for the native TCP port (9000), mapped to
	// the host so the test's clickhouse-client can reach it.
	NativeAddr string
	// ContainerName is the underlying container's name (without the
	// leading "/"). Mesh-mode tests need it to start an imesh sidecar
	// that joins this container's network namespace.
	ContainerName string
}

// StartClickHouseReplicatedNode starts a ClickHouse container configured for
// ReplicatedMergeTree replication alongside an existing KeeperCluster.
//
// The container joins the Keeper cluster's private Docker network so it
// discovers peers by name. Its <zookeeper> points at keeperEndpoint (a
// single host:port — typically a keeper-proxy alias like "kpx-1:9181"), so
// ALL keeper-client (9181) traffic flows through housegate rather than
// directly to the keepers. Its interserver is advertised as
// interserverGatewayAlias:9009 — peers fetch this node's parts through the
// co-located interserver gateway, not from this node directly; the real
// interserver still listens on 9009 in-network and is NOT exposed to host.
//
// interserver_http_port is BOTH the listen port and the advertised port in
// ClickHouse, so it is fixed at 9009 and the advertised host carries the
// indirection via the gateway alias.
func StartClickHouseReplicatedNode(t *testing.T, cluster *KeeperCluster, nodeName, keeperEndpoint, interserverGatewayAlias string) *ClickHouseReplicatedNode {
	t.Helper()
	ctx := context.Background()

	kHost, kPort, err := net.SplitHostPort(keeperEndpoint)
	if err != nil {
		t.Fatalf("invalid keeperEndpoint %q: %v", keeperEndpoint, err)
	}

	listenXML := `<clickhouse><listen_host replace="replace">0.0.0.0</listen_host></clickhouse>`
	macrosXML := fmt.Sprintf(`<clickhouse><macros><shard>01</shard><replica>%s</replica></macros></clickhouse>`, nodeName)

	// Single <zookeeper> node = the keeper-proxy endpoint; CH never sees the
	// real quorum addresses.
	keeperXML := fmt.Sprintf("<clickhouse>\n\t<zookeeper>\n\t\t<node><host>%s</host><port>%s</port></node>\n\t\t<session_timeout_ms>30000</session_timeout_ms>\n\t</zookeeper>\n\t<distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>\n</clickhouse>", kHost, kPort)

	// Advertise the co-located gateway alias as the interserver host; CH
	// still listens on 9009 in-network for the gateway to forward to.
	interserverXML := fmt.Sprintf(`<clickhouse>
	<interserver_http_host>%s</interserver_http_host>
	<interserver_http_port>9009</interserver_http_port>
</clickhouse>`, interserverGatewayAlias)

	dnet := cluster.DockerNetwork()
	req := testcontainers.ContainerRequest{
		Image:        chImage,
		ExposedPorts: []string{"9000/tcp"}, // native only; interserver (9009) stays in-network
		Hostname:     nodeName,
		Env: map[string]string{
			// The CH 25.8 image locks the default user to localhost unless
			// a password is set; set a known one so clickhouse-client can
			// connect from the host.
			"CLICKHOUSE_PASSWORD": "housegate_test_pw",
		},
		Networks:       []string{dnet.Name},
		NetworkAliases: map[string][]string{dnet.Name: {nodeName}},
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(listenXML), ContainerFilePath: "/etc/clickhouse-server/config.d/zz-listen.xml", FileMode: 0o644},
			{Reader: strings.NewReader(macrosXML), ContainerFilePath: "/etc/clickhouse-server/config.d/macros.xml", FileMode: 0o644},
			{Reader: strings.NewReader(keeperXML), ContainerFilePath: "/etc/clickhouse-server/config.d/keeper.xml", FileMode: 0o644},
			{Reader: strings.NewReader(interserverXML), ContainerFilePath: "/etc/clickhouse-server/config.d/interserver.xml", FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(120 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start CH node %s: %v", nodeName, err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("CH node %s host: %v", nodeName, err)
	}
	nativePort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("CH node %s 9000 mapped port: %v", nodeName, err)
	}
	nativeAddr := net.JoinHostPort(hostStr(host), nativePort.Port())
	if err := waitNativeTCP(ctx, nativeAddr, 15*time.Second); err != nil {
		t.Fatalf("wait native TCP %s: %v", nativeAddr, err)
	}

	name, _ := ctr.Name(ctx)
	return &ClickHouseReplicatedNode{
		NativeAddr:    nativeAddr,
		ContainerName: strings.TrimPrefix(name, "/"),
	}
}

// StartClickHouseMeshReplica starts a CH replica configured for the
// two-hop mTLS interserver-mesh. Differences from StartClickHouseReplicatedNode:
//
//   - <interserver_http_host>127.0.0.1</interserver_http_host>  ← advertised to peers
//   - <interserver_http_port>9010</interserver_http_port>        ← advertised + listen port
//   - <interserver_listen_host>127.0.0.2</interserver_listen_host>
//     CH's real interserver binds the secondary loopback 127.0.0.2:9010 so
//     a sidecar (sharing this container's netns) can take 127.0.0.1:9010
//     without colliding. Peers reading "127.0.0.1:9010" from keeper
//     naturally dial THEIR OWN sidecar (the same trick that makes co-located
//     housegate work for 9000).
//   - No 9009 expose; interserver stays in-netns, the sidecar's mTLS
//     ingress (0.0.0.0:19009) is the only externally-reachable surface.
func StartClickHouseMeshReplica(t *testing.T, cluster *KeeperCluster, nodeName, keeperEndpoint string) *ClickHouseReplicatedNode {
	t.Helper()
	ctx := context.Background()

	kHost, kPort, err := net.SplitHostPort(keeperEndpoint)
	if err != nil {
		t.Fatalf("invalid keeperEndpoint %q: %v", keeperEndpoint, err)
	}

	listenXML := `<clickhouse>
	<listen_host replace="replace">0.0.0.0</listen_host>
	<interserver_listen_host replace="replace">127.0.0.2</interserver_listen_host>
	<interserver_http_host>127.0.0.1</interserver_http_host>
	<interserver_http_port>9010</interserver_http_port>
</clickhouse>`
	macrosXML := fmt.Sprintf(`<clickhouse><macros><shard>01</shard><replica>%s</replica></macros></clickhouse>`, nodeName)
	keeperXML := fmt.Sprintf("<clickhouse>\n\t<zookeeper>\n\t\t<node><host>%s</host><port>%s</port></node>\n\t\t<session_timeout_ms>30000</session_timeout_ms>\n\t</zookeeper>\n\t<distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>\n</clickhouse>", kHost, kPort)

	dnet := cluster.DockerNetwork()
	req := testcontainers.ContainerRequest{
		Image:        chImage,
		ExposedPorts: []string{"9000/tcp"}, // native only — interserver stays in-netns
		Hostname:     nodeName,
		Env: map[string]string{
			"CLICKHOUSE_PASSWORD": "housegate_test_pw",
		},
		Networks:       []string{dnet.Name},
		NetworkAliases: map[string][]string{dnet.Name: {nodeName}},
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(listenXML), ContainerFilePath: "/etc/clickhouse-server/config.d/zz-listen.xml", FileMode: 0o644},
			{Reader: strings.NewReader(macrosXML), ContainerFilePath: "/etc/clickhouse-server/config.d/macros.xml", FileMode: 0o644},
			{Reader: strings.NewReader(keeperXML), ContainerFilePath: "/etc/clickhouse-server/config.d/keeper.xml", FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(120 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start CH mesh replica %s: %v", nodeName, err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("CH mesh replica %s host: %v", nodeName, err)
	}
	nativePort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("CH mesh replica %s 9000 mapped port: %v", nodeName, err)
	}
	nativeAddr := net.JoinHostPort(hostStr(host), nativePort.Port())
	if err := waitNativeTCP(ctx, nativeAddr, 15*time.Second); err != nil {
		t.Fatalf("wait native TCP %s: %v", nativeAddr, err)
	}

	name, _ := ctr.Name(ctx)
	return &ClickHouseReplicatedNode{
		NativeAddr:    nativeAddr,
		ContainerName: strings.TrimPrefix(name, "/"),
	}
}
