package network_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

func nullDatabaseInfo([]interface{}) (interface{}, *rpcErrEnvelope) { return nil, nil }

// TestRpcNetworkState_TableStatusWireShape pins the spec 2026-09-24 §10.2
// JSON-RPC contract sentio-node implements: method name, positional
// (database, table) params, and the six result fields.
func TestRpcNetworkState_TableStatusWireShape(t *testing.T) {
	var gotParams []interface{}
	result := json.RawMessage(`{"status":"refused","refused_code":"column_type","refused_reason":"column b","schema_json":"","schema_hash":"","registry_version":42}`)
	rpc, fake := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoById": nullDatabaseInfo,
		"sentio_getStorageIntegrityTableStatus": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			gotParams = params
			return result, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.calls, ",") != "sentio_getDatabaseInfoById,sentio_getStorageIntegrityTableStatus" || len(gotParams) != 2 || gotParams[0] != "shop" || gotParams[1] != "orders" {
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
		rpc, _ := newFakeRpc(t, map[string]rpcMethod{"sentio_getDatabaseInfoById": nullDatabaseInfo, "sentio_getStorageIntegrityTableStatus": method})
		if _, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}

// TestRpcNetworkStateAcceptsReplicaStatus is spec 2026-10-14 §11: a host that
// serves another indexer's table from its member's copy answers the status
// replica with the owner's schema and the four additive fields, and the
// client decodes them instead of refusing an unknown status.
func TestRpcNetworkStateAcceptsReplicaStatus(t *testing.T) {
	result := json.RawMessage(`{"status":"replica","refused_code":"","refused_reason":"","schema_json":"{\"table_id\":\"shop.orders\"}","schema_hash":"0xabc","registry_version":42,` +
		`"replica_of":2,"served_from":"0x5e1f","owner_entry":{"indexer_id":2,"safe_block_seq":812,"applied_promotion_seq":77},"frozen_since":1760500000000}`)
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoById": nullDatabaseInfo,
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return result, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil {
		t.Fatalf("a replica answer must decode: %v", err)
	}
	if got.Status != registry.TableStatusReplica || got.SchemaJSON != `{"table_id":"shop.orders"}` || got.SchemaHash != "0xabc" || got.RegistryVersion != 42 {
		t.Fatalf("status = %+v, want replica with the owner's schema", got)
	}
	wantEntry := registry.OwnerEntry{IndexerID: 2, SafeBlockSeq: 812, AppliedPromotionSeq: 77}
	if got.ReplicaOf == nil || *got.ReplicaOf != 2 || got.ServedFrom != "0x5e1f" || got.OwnerEntry == nil || *got.OwnerEntry != wantEntry || got.FrozenSince != 1760500000000 {
		t.Fatalf("replica fields = of %v from %q entry %v since %d, want 2, 0x5e1f, %+v, 1760500000000", got.ReplicaOf, got.ServedFrom, got.OwnerEntry, got.FrozenSince, wantEntry)
	}
}

// TestRpcNetworkStateStillRefusesAnUnknownStatus: accepting replica widens
// the known names by exactly one; any other name stays the same error.
func TestRpcNetworkStateStillRefusesAnUnknownStatus(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoById": nullDatabaseInfo,
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return map[string]any{"status": "replicating"}, nil
		},
	})
	_, err := rpc.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err == nil || err.Error() != `rpc: getStorageIntegrityTableStatus shop.orders: unknown status "replicating"` {
		t.Fatalf("err = %v, want the unknown-status error", err)
	}
}
