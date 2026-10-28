package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

// replicaHostsNode is one fake storage-node JSON-RPC endpoint. It answers
// sentio_getIndexerInfos with indexers and sentio_getStorageIntegrityInfo with
// info (a JSON object; empty answers a JSON-RPC error), and records each
// method it is asked.
type replicaHostsNode struct {
	srv      *httptest.Server
	indexers []IndexerInfo
	info     string

	mu    sync.Mutex
	calls []string
}

func newReplicaHostsNode(t *testing.T, info string) *replicaHostsNode {
	t.Helper()
	n := &replicaHostsNode{info: info}
	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			ID     uint64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n.mu.Lock()
		n.calls = append(n.calls, req.Method)
		indexers, info := n.indexers, n.info
		n.mu.Unlock()
		result := ""
		switch req.Method {
		case "sentio_getIndexerInfos":
			b, _ := json.Marshal(indexers)
			result = string(b)
		case "sentio_getStorageIntegrityInfo":
			result = info
		}
		if result == "" {
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"unavailable"}}`, req.ID)
			return
		}
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, result)
	}))
	t.Cleanup(n.srv.Close)
	return n
}

// indexer is the IndexerInfo that advertises n as indexer id's storage RPC.
func (n *replicaHostsNode) indexer(t *testing.T, id uint64) IndexerInfo {
	t.Helper()
	u, err := url.Parse(n.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return IndexerInfo{IndexerId: id, IndexerUrl: host, StorageNodeRpcPort: uint16(p), ClickhouseProxyPort: 9000}
}

func (n *replicaHostsNode) asked() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.calls...)
}

// TestRpcNetworkStateReplicaHostsAsksTheSourceThenThePeers: the agent's
// replica-host lookup (Plan S3-C4 K50) asks its network-state source first,
// then each indexer other than the owner in ascending id, and takes the first
// decoded answer, an empty one included; when nobody answers it fails with
// the contract's text.
func TestRpcNetworkStateReplicaHostsAsksTheSourceThenThePeers(t *testing.T) {
	ctx := context.Background()

	t.Run("the source answers", func(t *testing.T) {
		source := newReplicaHostsNode(t, `{"enabled":true,"replica_hosts":{"db2":[0,2]}}`)
		rpc, err := NewRpcNetworkState(source.srv.URL, RpcOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
		if err != nil || !reflect.DeepEqual(got, []uint64{0, 2}) {
			t.Fatalf("StorageIntegrityReplicaHosts = %v, %v; want [0 2]", got, err)
		}
		if calls := source.asked(); !reflect.DeepEqual(calls, []string{"sentio_getStorageIntegrityInfo"}) {
			t.Fatalf("source asked %v, want only sentio_getStorageIntegrityInfo", calls)
		}
	})

	t.Run("the source fails, the peers answer in ascending id without the owner", func(t *testing.T) {
		source := newReplicaHostsNode(t, "")
		peerA := newReplicaHostsNode(t, "")
		owner := newReplicaHostsNode(t, `{"replica_hosts":{"db2":[9]}}`)
		peerC := newReplicaHostsNode(t, `{"replica_hosts":{"db2":[0,2]}}`)
		peerD := newReplicaHostsNode(t, `{"replica_hosts":{"db2":[7]}}`)
		source.indexers = []IndexerInfo{
			peerD.indexer(t, 3), peerC.indexer(t, 2), owner.indexer(t, 1), peerA.indexer(t, 0),
			{IndexerId: 4, IndexerUrl: "127.0.0.1", ClickhouseProxyPort: 9000}, // no storage RPC port
		}
		rpc, err := NewRpcNetworkState(source.srv.URL, RpcOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
		if err != nil || !reflect.DeepEqual(got, []uint64{0, 2}) {
			t.Fatalf("StorageIntegrityReplicaHosts = %v, %v; want indexer 2's [0 2]", got, err)
		}
		if calls := source.asked(); !reflect.DeepEqual(calls, []string{"sentio_getStorageIntegrityInfo", "sentio_getIndexerInfos"}) {
			t.Fatalf("source asked %v, want the info first and then the indexer list", calls)
		}
		if calls := peerA.asked(); len(calls) != 1 {
			t.Fatalf("indexer 0 asked %v, want one info call before indexer 2", calls)
		}
		if calls := owner.asked(); len(calls) != 0 {
			t.Fatalf("the owner was asked %v; an unreachable owner must never block the lookup", calls)
		}
		if calls := peerD.asked(); len(calls) != 0 {
			t.Fatalf("indexer 3 was asked %v after indexer 2 answered", calls)
		}
	})

	t.Run("an older server answers no replica hosts", func(t *testing.T) {
		source := newReplicaHostsNode(t, `{"enabled":true,"network_id":"devnet2"}`)
		rpc, err := NewRpcNetworkState(source.srv.URL, RpcOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
		if err != nil || len(got) != 0 {
			t.Fatalf("StorageIntegrityReplicaHosts = %v, %v; want empty and no error", got, err)
		}
	})

	t.Run("nobody answers", func(t *testing.T) {
		source := newReplicaHostsNode(t, "")
		peer := newReplicaHostsNode(t, "")
		source.indexers = []IndexerInfo{peer.indexer(t, 0)}
		rpc, err := NewRpcNetworkState(source.srv.URL, RpcOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
		const want = "rpc: replica hosts of db2: no indexer answered sentio_getStorageIntegrityInfo"
		if err == nil || err.Error() != want {
			t.Fatalf("err = %v, want %q", err, want)
		}
	})
}

// TestRpcNetworkStateReplicaHostsCachesPerDatabase: a successful answer is
// reused per database for DefaultReplicaHostsTTL and handed out as a copy; a
// failed lookup is not cached.
func TestRpcNetworkStateReplicaHostsCachesPerDatabase(t *testing.T) {
	ctx := context.Background()
	source := newReplicaHostsNode(t, `{"replica_hosts":{"db2":[0,2],"db3":[0]}}`)
	rpc, err := NewRpcNetworkState(source.srv.URL, RpcOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	rpc.now = func() time.Time { return now }
	infoCalls := func() int {
		n := 0
		for _, m := range source.asked() {
			if m == "sentio_getStorageIntegrityInfo" {
				n++
			}
		}
		return n
	}

	first, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
	if err != nil || !reflect.DeepEqual(first, []uint64{0, 2}) {
		t.Fatalf("first lookup = %v, %v", first, err)
	}
	first[0] = 99
	now = now.Add(DefaultReplicaHostsTTL - time.Second)
	again, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1)
	if err != nil || !reflect.DeepEqual(again, []uint64{0, 2}) || infoCalls() != 1 {
		t.Fatalf("cached lookup = %v, %v after %d info calls; want [0 2] from the cache", again, err, infoCalls())
	}
	again[0] = 99
	if third, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1); err != nil || !reflect.DeepEqual(third, []uint64{0, 2}) || infoCalls() != 1 {
		t.Fatalf("mutating a cached answer changed the cache: %v, %v", third, err)
	}
	if other, err := rpc.StorageIntegrityReplicaHosts(ctx, "db3", 1); err != nil || !reflect.DeepEqual(other, []uint64{0}) || infoCalls() != 2 {
		t.Fatalf("db3 lookup = %v, %v after %d info calls; want its own answer", other, err, infoCalls())
	}
	now = now.Add(time.Second)
	if _, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1); err != nil || infoCalls() != 3 {
		t.Fatalf("expired lookup: %v after %d info calls; want a new call at DefaultReplicaHostsTTL", err, infoCalls())
	}

	source.mu.Lock()
	source.info = ""
	source.mu.Unlock()
	now = now.Add(DefaultReplicaHostsTTL)
	for i := 0; i < 2; i++ {
		if _, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1); err == nil {
			t.Fatal("a lookup nobody answers must fail")
		}
	}
	if infoCalls() != 5 {
		t.Fatalf("info calls = %d, want every failed lookup to ask again", infoCalls())
	}
	if DefaultReplicaHostsTTL != 30*time.Second {
		t.Fatalf("DefaultReplicaHostsTTL = %s, want 30s", DefaultReplicaHostsTTL)
	}
}

// TestRpcNetworkStateReplicaHostsHonorsTheLookupDeadline ensures the topology
// fallback cannot outlive a caller's deadline while a peer list is blocked.
func TestRpcNetworkStateReplicaHostsHonorsTheLookupDeadline(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			ID     uint64 `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req.Method == "sentio_getIndexerInfos" {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"unavailable"}}`, req.ID)
	}))
	defer source.Close()
	rpc, err := NewRpcNetworkState(source.URL, RpcOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := rpc.StorageIntegrityReplicaHosts(ctx, "db2", 1); err == nil {
		t.Fatal("lookup unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("lookup ignored its deadline: %s", elapsed)
	}
}
