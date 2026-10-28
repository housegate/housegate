package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/proxy"
)

// fakeReplicaDiscovery answers StorageIntegrityReplicaHosts with hosts or err
// and records each call.
type fakeReplicaDiscovery struct {
	hosts []uint64
	err   error
	calls []string
}

func (d *fakeReplicaDiscovery) StorageIntegrityReplicaHosts(_ context.Context, database string, owner uint64) ([]uint64, error) {
	d.calls = append(d.calls, fmt.Sprintf("%s/%d", database, owner))
	return d.hosts, d.err
}

// replicaFailoverState is a network of four indexers: db2 is owned by
// indexer 1, and indexer 2 advertises no HouseGate port.
func replicaFailoverState() *network.InMemoryNetworkState {
	ns := network.NewInMemoryNetworkState()
	ns.DatabaseInfos["db2"] = network.DatabaseInfo{IndexerId: 1}
	ns.IndexerInfos[0] = network.IndexerInfo{IndexerId: 0, IndexerUrl: "node-a", ClickhouseProxyPort: 9001}
	ns.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "node-b", ClickhouseProxyPort: 9002}
	ns.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "node-c"}
	ns.IndexerInfos[3] = network.IndexerInfo{IndexerId: 3, IndexerUrl: "node-d", ClickhouseProxyPort: 9004}
	return ns
}

// TestReplicaFailoverCandidatesSkipTheOwnerAndUnboundIndexers: candidates are
// the database's replica hosts in ascending id, once each, without the owner,
// an indexer without a HouseGate port or one the topology does not know; a
// database without a name or an owner, and a failed lookup, give none.
func TestReplicaFailoverCandidatesSkipTheOwnerAndUnboundIndexers(t *testing.T) {
	ctx := context.Background()
	ns := replicaFailoverState()
	discovery := &fakeReplicaDiscovery{hosts: []uint64{3, 2, 9, 0, 1, 3}}
	f := &ReplicaFailover{Discovery: discovery, Databases: ns, Topology: ns}
	got, err := f.Candidates(ctx, "db2")
	if err != nil || !reflect.DeepEqual(got, []string{"node-a:9001", "node-d:9004"}) {
		t.Fatalf("Candidates(db2) = %v, %v; want [node-a:9001 node-d:9004]", got, err)
	}
	if !reflect.DeepEqual(discovery.calls, []string{"db2/1"}) {
		t.Fatalf("discovery calls = %v, want one for db2 with owner 1", discovery.calls)
	}
	if !reflect.DeepEqual(discovery.hosts, []uint64{3, 2, 9, 0, 1, 3}) {
		t.Fatalf("Candidates reordered the discovery's slice: %v", discovery.hosts)
	}

	for _, database := range []string{"", "unknown"} {
		if got, err := f.Candidates(ctx, database); err == nil || got != nil {
			t.Fatalf("Candidates(%q) = %v, %v; want no candidate and an error", database, got, err)
		}
	}
	if len(discovery.calls) != 1 {
		t.Fatalf("discovery calls = %v; a database without an owner must not be looked up", discovery.calls)
	}

	discovery.err = errors.New("rpc: replica hosts of db2: no indexer answered sentio_getStorageIntegrityInfo")
	if got, err := f.Candidates(ctx, "db2"); !errors.Is(err, discovery.err) || got != nil {
		t.Fatalf("Candidates with a failed lookup = %v, %v; want no candidate and the lookup's error", got, err)
	}
}

// TestReplicaFailoverReasonClassifiesQuarantineAndUnreachable: a read fails
// over only on the exact read-side quarantine refusal (owner_quarantined) or
// a broken connection (owner_unreachable); a hello fails over on any failure,
// an Exception included, because the remote host's pivot to the owner
// failed. Every other Exception of a read reaches the client unchanged, and
// both reasons are exported by the proxy's counters from startup.
func TestReplicaFailoverReasonClassifiesQuarantineAndUnreachable(t *testing.T) {
	f := &ReplicaFailover{}
	quarantined := chproto.TableRefusedMessage("db2.t", "indexer_quarantined", "storage-integrity indexer 1 is quarantined")
	exc := func(code int32, msg string) *chproto.Exception {
		return &chproto.Exception{Code: proto.Error(code), Name: "DB::Exception", Message: msg}
	}
	for _, tc := range []struct {
		name   string
		phase  proxy.FailoverPhase
		cause  error
		exc    *chproto.Exception
		reason string
		ok     bool
	}{
		{"read refused as quarantined", proxy.FailoverAtRead, nil, exc(392, quarantined), FailoverReasonOwnerQuarantined, true},
		{"read refused as quarantined, seq unspent", proxy.FailoverAtRead, nil, exc(392, quarantined+chproto.SeqUnspentSuffix), FailoverReasonOwnerQuarantined, true},
		{"read connection broke", proxy.FailoverAtRead, io.EOF, nil, FailoverReasonOwnerUnreachable, true},
		{"read refused for another reason", proxy.FailoverAtRead, nil, exc(392, chproto.TableRefusedMessage("db2.t", "SCHEMA_INVALID", "bad column")), "", false},
		{"read refused by the arbiter's write-side quarantine", proxy.FailoverAtRead, nil, exc(392, chproto.IndexerQuarantinedMessage("db2.t")), "", false},
		{"read refused as pending", proxy.FailoverAtRead, nil, exc(733, "storage_integrity: table db2.t is pending activation (retryable)"), "", false},
		{"read refused by ClickHouse", proxy.FailoverAtRead, nil, exc(60, "Table db2.t does not exist"), "", false},
		{"hello dial failed", proxy.FailoverAtHello, errors.New("dial upstream: dial tcp 127.0.0.1:1: connect: connection refused"), nil, FailoverReasonOwnerUnreachable, true},
		{"hello answered an Exception", proxy.FailoverAtHello, errors.New("upstream rejected handshake"), exc(392, "forward: dial peer: connection refused"), FailoverReasonOwnerUnreachable, true},
		{"nothing failed", proxy.FailoverAtRead, nil, nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := f.Reason(tc.phase, tc.cause, tc.exc)
			if reason != tc.reason || ok != tc.ok {
				t.Fatalf("Reason = %q, %v; want %q, %v", reason, ok, tc.reason, tc.ok)
			}
		})
	}

	if FailoverReasonOwnerUnreachable != "owner_unreachable" || FailoverReasonOwnerQuarantined != "owner_quarantined" {
		t.Fatalf("reasons = %q, %q", FailoverReasonOwnerUnreachable, FailoverReasonOwnerQuarantined)
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exported := map[string]bool{}
	for _, family := range families {
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "reason" {
					exported[family.GetName()+"/"+label.GetValue()] = true
				}
			}
		}
	}
	for _, name := range []string{"clickhouse_proxy_agent_replica_failovers_total", "clickhouse_proxy_agent_replica_failover_exhausted_total"} {
		for _, reason := range []string{FailoverReasonOwnerQuarantined, FailoverReasonOwnerUnreachable} {
			if !exported[name+"/"+reason] {
				t.Fatalf("%s{reason=%q} is not exported", name, reason)
			}
		}
	}
}

// TestReplicaFailoverLogsEachFailover: every failover that reached a serving
// replica host is logged at info as "agent: replica failover" with the
// database, its owner, the reason and both addresses.
func TestReplicaFailoverLogsEachFailover(t *testing.T) {
	ns := replicaFailoverState()
	f := &ReplicaFailover{Discovery: &fakeReplicaDiscovery{hosts: []uint64{0}}, Databases: ns, Topology: ns}
	var buf bytes.Buffer
	ctx := log.WithContext(context.Background(), log.New(slog.NewTextHandler(&buf, nil)))
	if _, err := f.Candidates(ctx, "db2"); err != nil {
		t.Fatal(err)
	}
	f.FailedOver(ctx, "db2", FailoverReasonOwnerQuarantined, "node-b:9002", "node-a:9001")
	line := buf.String()
	for _, want := range []string{`level=INFO`, `msg="agent: replica failover"`, `database=db2`, `owner=1`, `reason=owner_quarantined`, `from=node-b:9002`, `to=node-a:9001`} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %s", line, want)
		}
	}
}

// ReplicaFailover satisfies the relay's port under its pkg/proxy names.
var (
	_ proxy.UpstreamFailover = (*ReplicaFailover)(nil)
	_ proxy.FailoverReporter = (*ReplicaFailover)(nil)
)

type replicaDeadlineTransport func(*http.Request) (*http.Response, error)

func (f replicaDeadlineTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestReplicaFailoverCandidatesHonorsRPCDeadline covers owner and topology
// RPCs with an in-memory transport; no network connection is opened.
func TestReplicaFailoverCandidatesHonorsRPCDeadline(t *testing.T) {
	for _, blocked := range []string{"sentio_getDatabaseInfoById", "sentio_getIndexerInfoById"} {
		t.Run(blocked, func(t *testing.T) {
			client := &http.Client{Transport: replicaDeadlineTransport(func(r *http.Request) (*http.Response, error) {
				var req struct {
					Method string `json:"method"`
					ID     uint64 `json:"id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					return nil, err
				}
				if req.Method == blocked {
					select {
					case <-r.Context().Done():
						return nil, r.Context().Err()
					case <-time.After(500 * time.Millisecond):
						return nil, io.EOF
					}
				}
				body := `{"databaseId":"db2","indexerId":1}`
				if req.Method == "sentio_getIndexerInfoById" {
					body = `{"indexerId":0,"indexerUrl":"node-a","clickhouseProxyPort":9001}`
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, body)))}, nil
			})}
			rpc, err := network.NewRpcNetworkState("http://fixture.invalid", network.RpcOptions{HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			f := &ReplicaFailover{Discovery: &fakeReplicaDiscovery{hosts: []uint64{0}}, Databases: rpc, Topology: rpc}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			start := time.Now()
			got, _ := f.Candidates(ctx, "db2")
			if len(got) != 0 {
				t.Fatalf("blocked lookup returned %v", got)
			}
			if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
				t.Fatalf("candidate lookup ignored deadline: %s", elapsed)
			}
		})
	}
}
