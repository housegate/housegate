package network_test

// External test package so we can also pull in
// housegate/housegate/pkg/plugins/sidecar for the Selector integration
// test (sidecar imports network — internal-package test would cycle).
// We re-declare the JSON-RPC wire envelope locally so the test can
// fake-serve it without touching unexported types in pkg/network.

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugins/sidecar"
	"housegate/housegate/pkg/registry"
)

// JSON-RPC 2.0 wire envelope (local copy — tests the public boundary).
type rpcRequest struct {
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      uint64        `json:"id"`
}

type rpcResponse struct {
	JsonRpc string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcErrEnvelope `json:"error,omitempty"`
}

type rpcErrEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// rpcMethod is one fake handler in the test JSON-RPC server.
// Returning result=nil + err=nil emits a JSON-null result.
type rpcMethod func(params []interface{}) (result interface{}, err *rpcErrEnvelope)

// fakeRpcServer dispatches JSON-RPC requests to a method table and
// records the method names hit in arrival order.
type fakeRpcServer struct {
	methods map[string]rpcMethod
	calls   []string
}

func (f *fakeRpcServer) handler(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.calls = append(f.calls, req.Method)
	resp := rpcResponse{JsonRpc: "2.0", ID: req.ID, Result: json.RawMessage("null")}
	fn, ok := f.methods[req.Method]
	if !ok {
		resp.Error = &rpcErrEnvelope{Code: -32601, Message: "method not found: " + req.Method}
	} else {
		result, e := fn(req.Params)
		resp.Error = e
		if result != nil {
			b, mErr := json.Marshal(result)
			if mErr != nil {
				http.Error(w, mErr.Error(), http.StatusInternalServerError)
				return
			}
			resp.Result = b
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func newFakeRpc(t *testing.T, methods map[string]rpcMethod) (*network.RpcNetworkState, *fakeRpcServer) {
	t.Helper()
	fake := &fakeRpcServer{methods: methods}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	rpc, err := network.NewRpcNetworkState(srv.URL, network.RpcOptions{})
	if err != nil {
		t.Fatalf("NewRpcNetworkState: %v", err)
	}
	return rpc, fake
}

func TestNewRpcNetworkState_EmptyEndpoint(t *testing.T) {
	if _, err := network.NewRpcNetworkState("", network.RpcOptions{}); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestRpcNetworkState_AllIndexers(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return []network.IndexerInfo{
				{IndexerId: 0, IndexerUrl: "node-a", ClickhouseProxyPort: 9001},
				{IndexerId: 1, IndexerUrl: "node-b", ClickhouseProxyPort: 9002},
			}, nil
		},
	})

	all := rpc.AllIndexers()
	if len(all) != 2 {
		t.Fatalf("want 2 entries, got %d (%v)", len(all), all)
	}
	if all[0].Url != "node-a" {
		t.Errorf("indexer 0: got %q", all[0].Url)
	}
	if all[1].HousegatePort != 9002 {
		t.Errorf("indexer 1 port: got %d", all[1].HousegatePort)
	}
}

func TestRpcNetworkState_AllIndexers_Empty(t *testing.T) {
	// Server returns an empty array — caller must see a non-nil empty
	// map (nil is reserved for transport errors).
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return []network.IndexerInfo{}, nil
		},
	})
	all := rpc.AllIndexers()
	if all == nil {
		t.Error("empty array: want non-nil map for ok-but-empty")
	}
	if len(all) != 0 {
		t.Errorf("want 0 entries, got %d", len(all))
	}
}

func TestRpcNetworkState_ProxyByIndexerId(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			id, _ := params[0].(float64)
			if uint64(id) == 1 {
				return network.IndexerInfo{IndexerId: 1, IndexerUrl: "node-b", ClickhouseProxyPort: 9002}, nil
			}
			return nil, nil
		},
	})

	addr, ok := rpc.ProxyByIndexerId(1)
	if !ok {
		t.Fatal("hit: want ok=true")
	}
	if addr.Url != "node-b" || addr.HousegatePort != 9002 {
		t.Errorf("hit: got %+v", addr)
	}
	if _, ok := rpc.ProxyByIndexerId(999); ok {
		t.Error("miss: want ok=false")
	}
}

func TestRpcNetworkState_Get(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			id, _ := params[0].(string)
			if id == "db-1" {
				return network.DatabaseInfo{DatabaseId: "db-1", IndexerId: 7}, nil
			}
			return nil, nil
		},
	})

	db, ok := rpc.Get("db-1")
	if !ok || db.IndexerId != 7 {
		t.Errorf("hit: ok=%v db=%+v", ok, db)
	}
	if _, ok := rpc.Get("db-other"); ok {
		t.Error("miss: want ok=false")
	}
}

func TestRpcNetworkState_PermissionsFor(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoByAccount": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			acct, _ := params[0].(string)
			if strings.EqualFold(acct, "0xabc") {
				// Real wire shape: an account may own multiple databases.
				return []network.DatabaseInfo{
					{DatabaseId: "db-1", IndexerId: 1},
					{DatabaseId: "db-2", IndexerId: 2},
				}, nil
			}
			return nil, nil
		},
	})

	perms, ok := rpc.PermissionsFor("0xabc")
	if !ok {
		t.Fatal("owned account: want ok=true")
	}
	if len(perms) != 2 {
		t.Fatalf("owned account: want 2 entries, got %v", perms)
	}
	for _, db := range []string{"db-1", "db-2"} {
		auth, has := perms[db]
		if !has {
			t.Fatalf("owned account: want %s in perms, got %v", db, perms)
		}
		if auth&registry.DbAuthOwner == 0 {
			t.Errorf("owned account: %s want DbAuthOwner bit set, got bitmap=%v", db, auth)
		}
	}

	// Unknown account → null result, still ok=true with empty map.
	perms, ok = rpc.PermissionsFor("0xunknown")
	if !ok {
		t.Fatal("unknown account: want ok=true")
	}
	if len(perms) != 0 {
		t.Errorf("unknown account: want empty map, got %v", perms)
	}
}

func TestRpcNetworkState_RpcErrorEnvelope(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfoById": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return nil, &rpcErrEnvelope{Code: -32000, Message: "boom"}
		},
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return nil, &rpcErrEnvelope{Code: -32000, Message: "boom"}
		},
	})
	if _, ok := rpc.ProxyByIndexerId(1); ok {
		t.Error("rpc error: want ok=false")
	}
	// All-indexers transport failure returns nil (not empty map) so
	// the caller can distinguish "lookup failed" from "no indexers".
	if all := rpc.AllIndexers(); all != nil {
		t.Errorf("rpc error: want nil map, got %v", all)
	}
}

func TestRpcNetworkState_UnsupportedServerMethods(t *testing.T) {
	rpc, _ := network.NewRpcNetworkState("http://example.invalid", network.RpcOptions{})

	if all := rpc.All(); all == nil || len(all) != 0 {
		t.Errorf("All: want empty (non-nil) map, got %v", all)
	}
	if _, err := rpc.HasPermission("a", "d", registry.Read); err == nil {
		t.Error("HasPermission: want error on rpc backend")
	}
}

// TestRpcNetworkState_SelectorIntegration runs the actual sidecar
// Selector against an RPC-backed Registry end-to-end.
func TestRpcNetworkState_SelectorIntegration(t *testing.T) {
	const account = "0xabc"

	indexerInfos := []network.IndexerInfo{
		{IndexerId: 0, IndexerUrl: "node-a", ClickhouseProxyPort: 9001},
		{IndexerId: 1, IndexerUrl: "node-b", ClickhouseProxyPort: 9002},
	}
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return indexerInfos, nil
		},
		"sentio_getDatabaseInfoByAccount": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			acct, _ := params[0].(string)
			if acct == account {
				return []network.DatabaseInfo{{DatabaseId: "db-1", IndexerId: 1}}, nil
			}
			return nil, nil
		},
		"sentio_getDatabaseInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			id, _ := params[0].(string)
			if id == "db-1" {
				return network.DatabaseInfo{DatabaseId: "db-1", IndexerId: 1}, nil
			}
			return nil, nil
		},
	})

	// Permissioned tier: account owns db-1 on indexer 1.
	sel := &sidecar.Selector{Topology: rpc, Databases: rpc, Access: rpc, Account: account}
	choice, err := sel.Pick(rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("Pick(owned account): %v", err)
	}
	if choice.IsBootstrap {
		t.Errorf("owned account: expected permissioned tier, got bootstrap")
	}
	if choice.IndexerId != 1 {
		t.Errorf("owned account: want indexer 1, got %+v", choice)
	}

	// Bootstrap tier: unknown account falls through to any bound peer.
	selBoot := &sidecar.Selector{Topology: rpc, Databases: rpc, Access: rpc, Account: "0xstranger"}
	bootChoice, err := selBoot.Pick(rand.New(rand.NewSource(1)))
	if err != nil {
		t.Fatalf("Pick(unknown account): %v", err)
	}
	if !bootChoice.IsBootstrap {
		t.Errorf("unknown account: expected bootstrap=true, got %+v", bootChoice)
	}
}
