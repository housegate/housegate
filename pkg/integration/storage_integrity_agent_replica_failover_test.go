package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/sitable"
)

// The agent replica-failover test (spec 2026-10-14 §11, Plan S3-C4 K46) runs
// the agent of a client whose only database, rrdb, is owned by indexer 2,
// against a member host (indexer 1, Task 43's startReplicaReadsHost) that
// serves rrdb.a and rrdb.b from hg_replica copies. The owner is a fake
// HouseGate that refuses every statement with the quarantine refusal its host
// answers for its tables. The agent's network state is a JSON-RPC fake of the
// storage-node RPC that advertises indexer 1 as rrdb's replica host.

// failoverOwnerRevision is below the addendum revision, so the fake owner
// speaks no addendum; the client, the agent and the member host then all
// negotiate it.
const failoverOwnerRevision = 54453

var failoverOwnerRefusal = chproto.TableRefusedMessage(replicaReadsDB+".a", "indexer_quarantined", "storage-integrity indexer 2 is quarantined")

// startQuarantinedOwner is the fake owner host: it answers the hello with
// ClickHouse's timezone, a SET with EndOfStream and every other statement
// with the quarantine refusal.
func startQuarantinedOwner(t *testing.T, timezone string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
				codec := chproto.NewCodec(conn, chproto.DirFromClient)
				if _, err := codec.ReadPacket(uint64(chproto.ClientHelloCode)); err != nil {
					return
				}
				var b proto.Buffer
				(&proto.ServerHello{Name: "owner", Major: 25, Minor: 8, Revision: failoverOwnerRevision, Timezone: timezone, DisplayName: "quarantined-owner"}).EncodeAware(&b, failoverOwnerRevision)
				if _, err := conn.Write(b.Buf); err != nil {
					return
				}
				codec.SetRevision(failoverOwnerRevision)
				codec.SetCompression(proto.CompressionDisabled)
				for {
					q, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
					if err != nil || q.Type != uint64(chproto.ClientQueryCode) {
						return
					}
					if pkt, err := codec.ReadPacket(); err != nil || pkt.Type != uint64(chproto.ClientDataCode) {
						return
					}
					if strings.HasPrefix(q.Decoded.(*chproto.Query).Body, "SET ") {
						if _, err := conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
							return
						}
						continue
					}
					if err := codec.WriteException(&chproto.Exception{Code: proto.Error(chproto.CodeQueryIsProhibited), Name: "DB::Exception", Message: failoverOwnerRefusal}); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln
}

// failoverNetworkRPC is the agent's network state: indexers 1 (the member
// host) and 2 (the owner), rrdb owned by 2 and permitted to the agent's
// account, and replica_hosts {rrdb: [1]}.
func failoverNetworkRPC(t *testing.T, member, owner string) string {
	t.Helper()
	var mu sync.Mutex
	var indexers []network.IndexerInfo
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string        `json:"method"`
			Params []interface{} `json:"params"`
			ID     uint64        `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		infos := indexers
		mu.Unlock()
		database := network.DatabaseInfo{DatabaseId: replicaReadsDB, IndexerId: replicaReadsOwner}
		var result any
		switch req.Method {
		case "sentio_getIndexerInfos":
			result = infos
		case "sentio_getIndexerInfoById":
			id, _ := req.Params[0].(float64)
			for _, info := range infos {
				if info.IndexerId == uint64(id) {
					result = info
				}
			}
		case "sentio_getDatabaseInfoById":
			if req.Params[0] == replicaReadsDB {
				result = database
			}
		case "sentio_getDatabaseInfoByAccount":
			result = []network.DatabaseInfo{database}
		case "sentio_getStorageIntegrityInfo":
			result = map[string]any{"enabled": true, "replica_databases": []string{}, "replica_hosts": map[string][]uint64{replicaReadsDB: {1}}}
		}
		body, _ := json.Marshal(result)
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, body)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, rpcPort, _ := net.SplitHostPort(u.Host)
	info := func(id uint64, addr string) network.IndexerInfo {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		hp, _ := strconv.Atoi(port)
		sp, _ := strconv.Atoi(rpcPort)
		return network.IndexerInfo{IndexerId: id, IndexerUrl: host, ClickhouseProxyPort: uint16(hp), StorageNodeRpcPort: uint16(sp)}
	}
	mu.Lock()
	indexers = []network.IndexerInfo{info(1, member), info(replicaReadsOwner, owner)}
	mu.Unlock()
	return srv.URL
}

// replicaFailovers reads clickhouse_proxy_agent_replica_failovers_total for
// reason from the in-process agent's registry.
func replicaFailovers(t *testing.T, reason string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "clickhouse_proxy_agent_replica_failovers_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("clickhouse_proxy_agent_replica_failovers_total{reason=%q} is not exported", reason)
	return 0
}

// TestAgentReplicaFailover_ReadsAQuarantinedOwnersTableFromAMember: a client
// of the quarantined owner's database reads, in safe mode, through its agent:
// the agent's session lands on the owner, which refuses the read as
// quarantined, and the agent replays the hello and the read on the member
// host, which answers from the copies. A write is refused by the owner and
// never replayed, and a session that issued a SET no longer fails over (the
// client drops a connection after any error, so a write cannot be followed on
// its session). Once the owner cannot be reached at all, a new session's
// hello fails over too.
func TestAgentReplicaFailover_ReadsAQuarantinedOwnersTableFromAMember(t *testing.T) {
	seed := seedReplicaReads(t)
	ctx := context.Background()
	var timezone string
	if err := seed.QueryRow(ctx, "SELECT timezone()").Scan(&timezone); err != nil {
		t.Fatal(err)
	}
	member := startReplicaReadsHost(t, sitable.NewFake(sitable.Ordinary, replicaReadsServed()...))
	owner := startQuarantinedOwner(t, timezone)
	rpcURL := failoverNetworkRPC(t, member.Addr, owner.Addr().String())
	rpc, err := network.NewRpcNetworkState(rpcURL, network.RpcOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	agent := testenv.StartAgentProxyWithSelector(t, authTestKey1, func(cfg *config.Config, opts *housegate.Options) {
		cfg.NetworkState.Source = rpcURL
		opts.NetworkState = rpc
	})

	quarantined := replicaFailovers(t, "owner_quarantined")
	conn := openConnDB(t, agent.Addr, replicaReadsDB)
	safe := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"SQL_x_read_mode": clickhouse.CustomSetting{Value: "safe"}}))
	var n uint64
	if err := conn.QueryRow(safe, "SELECT count() FROM a JOIN b ON a.id = b.id").Scan(&n); err != nil || n != 2 {
		t.Fatalf("read through the agent = %d, %v; want the copies' JOIN count 2", n, err)
	}
	if got := replicaFailovers(t, "owner_quarantined"); got != quarantined+1 {
		t.Fatalf("failovers{owner_quarantined} = %g, want %g", got, quarantined+1)
	}
	if got := queryUint64(t, conn, "SELECT sum(id) FROM rrdb.a"); got != 6 {
		t.Fatalf("a second read on the failed-over session = %d, want 6 from the copy", got)
	}

	writer := openConnDB(t, agent.Addr, replicaReadsDB)
	if err := writer.Exec(ctx, "INSERT INTO a (id, v) VALUES (9, 'x')"); err == nil || !strings.Contains(err.Error(), failoverOwnerRefusal) {
		t.Fatalf("a write through the agent = %v, want the owner's refusal %q", err, failoverOwnerRefusal)
	}
	stateful := openConnDB(t, agent.Addr, replicaReadsDB)
	if err := stateful.Exec(ctx, "SET max_threads = 1"); err != nil {
		t.Fatalf("SET through the agent: %v", err)
	}
	if err := stateful.QueryRow(ctx, "SELECT count() FROM a").Scan(&n); err == nil || !strings.Contains(err.Error(), failoverOwnerRefusal) {
		t.Fatalf("a read after a SET on the same session = %d, %v; want the owner's refusal, never a failover", n, err)
	}
	if got := replicaFailovers(t, "owner_quarantined"); got != quarantined+1 {
		t.Fatalf("failovers{owner_quarantined} = %g after the write and the stateful read, want still %g", got, quarantined+1)
	}
	if got := queryUint64(t, seed, "SELECT count() FROM "+sitable.ReplicaDatabase+"."+replicaReadsCopyA); got != 3 {
		t.Fatalf("copy rows = %d, want the 3 installed rows", got)
	}

	unreachable := replicaFailovers(t, "owner_unreachable")
	_ = owner.Close()
	fresh := openConnDB(t, agent.Addr, replicaReadsDB)
	if got := queryUint64(t, fresh, "SELECT count() FROM a"); got != 3 {
		t.Fatalf("a read through a session whose owner is unreachable = %d, want 3 from the copy", got)
	}
	if got := replicaFailovers(t, "owner_unreachable"); got != unreachable+1 {
		t.Fatalf("failovers{owner_unreachable} = %g, want %g", got, unreachable+1)
	}
}

// heldConn buffers writes while hold is set, so a test can put several
// packets on the wire in one segment, as TCP does when a sender's
// back-to-back writes reach a busy or distant receiver together.
type heldConn struct {
	net.Conn
	hold bool
	buf  bytes.Buffer
}

func (c *heldConn) Write(p []byte) (int, error) {
	if c.hold {
		return c.buf.Write(p)
	}
	return c.Conn.Write(p)
}

func (c *heldConn) release() error {
	c.hold = false
	_, err := c.Conn.Write(c.buf.Bytes())
	c.buf.Reset()
	return err
}

// TestServerProxy_ServesAChunkedQuerySentWithTheAddendum is the devnet2
// replica-failover drill (Plan S3-C4 Task 48b) against a real HouseGate host:
// the agent's failover replay writes the addendum, the chunked Query and its
// empty Data terminator back to back, and when they reach the host in one
// read the host must still deframe the Query. Before the fix the host's
// bufio had prefetched the chunk frame while chunked reads were disabled,
// read the frame header's low byte as the packet type and closed the
// session ("skip body for client type=16|17 not yet supported").
func TestServerProxy_ServesAChunkedQuerySentWithTheAddendum(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	raw, err := net.DialTimeout("tcp", proxy.Addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(30 * time.Second))
	conn := &heldConn{Conn: raw}
	codec := chproto.NewCodec(conn, chproto.DirToUpstream)

	hello := &chproto.ClientHello{
		Name: "failover-replay", Major: 25, Minor: 8,
		ProtocolVersion: chproto.MaxSupportedRevision,
		Database:        chEnv.Database, User: chEnv.User, Password: chEnv.Password,
	}
	if err := codec.WriteClientHello(hello); err != nil {
		t.Fatal(err)
	}
	codec.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	pkt, err := codec.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("read server hello: %v", err)
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok {
		t.Fatalf("server hello = %T %+v", pkt.Decoded, pkt.Decoded)
	}
	rev := min(int(hello.ProtocolVersion), int(srv.Revision))
	codec.SetRevision(rev)
	if !chproto.SupportsChunkedPackets(rev) {
		t.Fatalf("negotiated revision %d has no chunked transport", rev)
	}

	conn.hold = true
	res := codec.ResolveUpstreamAddendum(chproto.AddendumResult{}, chproto.AddendumOpts{
		ProposedRecv: "chunked_optional",
		ProposedSend: "chunked_optional",
	})
	if res.NegotiatedSend != "chunked" {
		t.Fatalf("the host does not take chunked packets (send=%q); the drill's framing is not reproduced", res.NegotiatedSend)
	}
	if err := codec.SendAddendum(res); err != nil {
		t.Fatal(err)
	}
	if err := codec.WriteQuery(&chproto.Query{
		ID:   "chunked-with-addendum",
		Body: "SELECT 42",
		Info: proto.ClientInfo{
			ProtocolVersion: rev, Major: 25, Minor: 8,
			Interface: proto.InterfaceTCP, Query: proto.ClientQueryInitial,
			InitialAddress: "127.0.0.1:0", ClientHostname: "failover-replay", ClientName: "failover-replay",
		},
	}); err != nil {
		t.Fatal(err)
	}
	var terminator proto.Buffer
	terminator.PutUVarInt(uint64(chproto.ClientDataCode))
	terminator.PutString("")
	(&proto.Block{}).EncodeAware(&terminator, rev)
	if err := codec.WriteRawPacket(terminator.Buf); err != nil {
		t.Fatal(err)
	}
	if err := conn.release(); err != nil {
		t.Fatal(err)
	}

	for {
		pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			t.Fatalf("the host broke before answering the Query sent with the addendum: %v", err)
		}
		if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
			t.Fatalf("the host answered code=%d %s", exc.Code, exc.Message)
		}
		if pkt.Type == uint64(chproto.ServerEndOfStreamCode) {
			return
		}
	}
}
