package network_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

// TestRpcNetworkState_TableStatusWireShape pins the spec 2026-09-24 §10.2
// JSON-RPC contract sentio-node implements: method name, positional
// (database, table) params, and the six result fields.
func TestRpcNetworkState_TableStatusWireShape(t *testing.T) {
	var gotParams []interface{}
	result := json.RawMessage(`{"status":"refused","refused_code":"column_type","refused_reason":"column b","schema_json":"","schema_hash":"","registry_version":42}`)
	rpc, fake := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			gotParams = params
			return result, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.calls, ",") != "sentio_getStorageIntegrityTableStatus" || len(gotParams) != 2 || gotParams[0] != "shop" || gotParams[1] != "orders" {
		t.Fatalf("calls = %v params = %v", fake.calls, gotParams)
	}
	want := registry.TableStatus{Status: "refused", RefusedCode: "column_type", RefusedReason: "column b", RegistryVersion: 42}
	if got != want {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
}

func TestRpcNetworkState_TableStatusFailures(t *testing.T) {
	for name, method := range map[string]rpcMethod{
		"rpc error": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return nil, &rpcErrEnvelope{Code: -32000, Message: "down"}
		},
		"null result":    func([]interface{}) (interface{}, *rpcErrEnvelope) { return nil, nil },
		"unknown status": func([]interface{}) (interface{}, *rpcErrEnvelope) { return map[string]any{"status": "retiring"}, nil },
	} {
		rpc, _ := newFakeRpc(t, map[string]rpcMethod{"sentio_getStorageIntegrityTableStatus": method})
		if _, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}
