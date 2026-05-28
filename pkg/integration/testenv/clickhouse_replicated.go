package testenv

import (
	"context"
	"fmt"
	"net"
	"sort"
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

// StartClickHouseMeshReplica starts a CH replica configured for the
// two-hop mTLS interserver-mesh:
//
//   - <zookeeper> points at primaryKeeper (typically a keeper-proxy alias
//     like "kpx-1:9181"), so ALL keeper-client (9181) traffic flows
//     through housegate rather than directly to the keepers.
//   - auxKeepers, when non-empty, registers each entry as an
//     <auxiliary_zookeepers> shard (architecture.md §6 multi-shard pools).
//     The map key is the shard name as it appears in CH DDL
//     (ReplicatedMergeTree('<shard>:/path/...', '{replica}')); the value
//     is the keeper-proxy endpoint (host:port) for that shard. The primary
//     keeper itself is NOT addressable by name in DDL (un-prefixed paths
//     route to it); only the auxiliary shards are.
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
func StartClickHouseMeshReplica(t *testing.T, cluster *KeeperCluster, nodeName, primaryKeeper string, auxKeepers map[string]string) *ClickHouseReplicatedNode {
	t.Helper()
	ctx := context.Background()

	kHost, kPort, err := net.SplitHostPort(primaryKeeper)
	if err != nil {
		t.Fatalf("invalid primaryKeeper %q: %v", primaryKeeper, err)
	}

	listenXML := `<clickhouse>
	<listen_host replace="replace">0.0.0.0</listen_host>
	<interserver_listen_host replace="replace">127.0.0.2</interserver_listen_host>
	<interserver_http_host>127.0.0.1</interserver_http_host>
	<interserver_http_port>9010</interserver_http_port>
</clickhouse>`
	macrosXML := fmt.Sprintf(`<clickhouse><macros><shard>01</shard><replica>%s</replica></macros></clickhouse>`, nodeName)

	// <zookeeper> = primary; <auxiliary_zookeepers> = secondary shards
	// (architecture.md §6). Each aux key appears verbatim as the XML
	// element name and as the DDL shard name; restrict to valid XML
	// identifiers when picking names.
	var keeperXML strings.Builder
	fmt.Fprintf(&keeperXML, "<clickhouse>\n\t<zookeeper>\n\t\t<node><host>%s</host><port>%s</port></node>\n\t\t<session_timeout_ms>30000</session_timeout_ms>\n\t</zookeeper>\n", kHost, kPort)
	if len(auxKeepers) > 0 {
		names := make([]string, 0, len(auxKeepers))
		for name := range auxKeepers {
			names = append(names, name)
		}
		sort.Strings(names)
		keeperXML.WriteString("\t<auxiliary_zookeepers>\n")
		for _, name := range names {
			aHost, aPort, err := net.SplitHostPort(auxKeepers[name])
			if err != nil {
				t.Fatalf("invalid aux keeper %q=%q: %v", name, auxKeepers[name], err)
			}
			fmt.Fprintf(&keeperXML, "\t\t<%s>\n\t\t\t<node><host>%s</host><port>%s</port></node>\n\t\t\t<session_timeout_ms>30000</session_timeout_ms>\n\t\t</%s>\n", name, aHost, aPort, name)
		}
		keeperXML.WriteString("\t</auxiliary_zookeepers>\n")
	}
	keeperXML.WriteString("\t<distributed_ddl><path>/clickhouse/task_queue/ddl</path></distributed_ddl>\n</clickhouse>")

	dnet := cluster.DockerNetwork()
	req := testcontainers.ContainerRequest{
		Image:        chImage,
		ExposedPorts: []string{"9000/tcp"}, // native only — interserver stays in-netns
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
			{Reader: strings.NewReader(keeperXML.String()), ContainerFilePath: "/etc/clickhouse-server/config.d/keeper.xml", FileMode: 0o644},
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
