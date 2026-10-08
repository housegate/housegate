package network_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// hostingFake starts a second JSON-RPC server (the indexer hosting the
// database) and returns its storage port.
func hostingFake(t *testing.T, methods map[string]rpcMethod) (*fakeRpcServer, uint16) {
	t.Helper()
	fake := &fakeRpcServer{methods: methods}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, portText, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(portText)
	return fake, uint16(port)
}

func bootstrapFor(t *testing.T, hostingPort uint16, extra map[string]rpcMethod) (*network.RpcNetworkState, *fakeRpcServer) {
	t.Helper()
	methods := map[string]rpcMethod{
		"sentio_getDatabaseInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			if params[0] == "devuser1" {
				return network.DatabaseInfo{DatabaseId: "devuser1", IndexerId: 2}, nil
			}
			return nil, nil
		},
		"sentio_getIndexerInfoById": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return network.IndexerInfo{IndexerId: 2, IndexerUrl: "127.0.0.1", ClickhouseProxyPort: 33001, StorageNodeRpcPort: hostingPort}, nil
		},
	}
	for name, method := range extra {
		methods[name] = method
	}
	return newFakeRpc(t, methods)
}

func TestRpcNetworkState_StatusRoutesToTheHostingIndexer(t *testing.T) {
	hosting, port := hostingFake(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return map[string]any{"status": "active"}, nil
		},
	})
	rpc, bootstrap := bootstrapFor(t, port, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return map[string]any{"status": "ordinary"}, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "devuser1", "t")
	if err != nil || got.Status != registry.TableStatusActive {
		t.Fatalf("status = %+v, %v; want the hosting indexer's active", got, err)
	}
	if strings.Join(hosting.calls, ",") != "sentio_getStorageIntegrityTableStatus" {
		t.Fatalf("hosting calls = %v", hosting.calls)
	}
	if strings.Contains(strings.Join(bootstrap.calls, ","), "sentio_getStorageIntegrityTableStatus") {
		t.Fatalf("bootstrap answered a routed status: %v", bootstrap.calls)
	}
	// An unknown database falls back to the bootstrap.
	got, err = rpc.StorageIntegrityTableStatus(context.Background(), "nope", "t")
	if err != nil || got.Status != registry.TableStatusOrdinary {
		t.Fatalf("unknown database = %+v, %v; want the bootstrap's answer", got, err)
	}
}

func TestRpcNetworkState_StorageIntegrityInfoShape(t *testing.T) {
	_, port := hostingFake(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityInfo": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			if len(params) != 0 {
				return nil, &rpcErrEnvelope{Code: -32602, Message: "no params expected"}
			}
			return map[string]any{
				"enabled": true, "network_id": "devnet2-si", "keeper_shard_id": 0,
				"si_indexer_id": nil, "self_indexer_id": 2, "client_lanes_enabled": false,
				"ingress_max_payload_bytes": 67108864, "max_token_age_seconds": 60,
				"default_read_mode": "", "registry_version": 7, "server_unix_time": 1760000000,
			}, nil
		},
	})
	rpc, _ := bootstrapFor(t, port, nil)
	info, err := rpc.StorageIntegrityInfo(context.Background(), "devuser1")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Enabled || info.NetworkID != "devnet2-si" || info.SIIndexerID != nil || info.SelfIndexerID == nil || *info.SelfIndexerID != 2 ||
		info.ClientLanesEnabled || info.IngressMaxPayloadBytes != 64<<20 || info.MaxTokenAgeSeconds != 60 ||
		info.EffectiveDefaultReadMode() != "safe" || info.RegistryVersion != 7 || info.ServerUnixTime != 1760000000 {
		t.Fatalf("info = %+v", info)
	}
}

func TestRpcNetworkState_WriterCheck(t *testing.T) {
	var gotParams []interface{}
	_, port := hostingFake(t, map[string]rpcMethod{
		"sentio_isDatabaseWriter": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			gotParams = params
			return params[1] == "0x00000000000000000000000000000000000000b1", nil
		},
	})
	rpc, _ := bootstrapFor(t, port, nil)
	ok, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0x00000000000000000000000000000000000000b1")
	if err != nil || !ok || len(gotParams) != 2 || gotParams[0] != "devuser1" {
		t.Fatalf("writer check = %v, %v params=%v", ok, err, gotParams)
	}
	if ok, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0x00000000000000000000000000000000000000b2"); err != nil || ok {
		t.Fatalf("non-writer = %v, %v", ok, err)
	}
}

// The bootstrap cannot know the writers of a database no indexer hosts, so the
// writer check never asks it: an unhosted database is ErrDatabaseNotHosted
// ("unknown" to the agent), unlike the status and info lookups, which fall
// back to the bootstrap.
func TestRpcNetworkState_WriterCheckUnhostedDatabaseIsUnknown(t *testing.T) {
	_, port := hostingFake(t, nil)
	rpc, bootstrap := bootstrapFor(t, port, map[string]rpcMethod{
		"sentio_isDatabaseWriter": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return false, nil
		},
	})
	ok, err := rpc.StorageIntegrityWriterCheck(context.Background(), "nope", "0xb1")
	if !errors.Is(err, registry.ErrDatabaseNotHosted) || ok {
		t.Fatalf("writer check = %v, %v; want ErrDatabaseNotHosted", ok, err)
	}
	if strings.Contains(strings.Join(bootstrap.calls, ","), "sentio_isDatabaseWriter") {
		t.Fatalf("the bootstrap answered a writer check for an unhosted database: %v", bootstrap.calls)
	}
}

func TestRpcNetworkState_WriterCheckUnsupportedIsAnError(t *testing.T) {
	_, port := hostingFake(t, nil) // method not found
	rpc, _ := bootstrapFor(t, port, nil)
	if _, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0xb1"); err == nil {
		t.Fatal("an indexer without sentio_isDatabaseWriter must answer an error (the caller treats it as unknown)")
	}
}

func TestRpcNetworkState_ProxyAddressCarriesStorageRPCPort(t *testing.T) {
	rpc, _ := bootstrapFor(t, 32003, nil)
	addr, ok := rpc.ProxyByIndexerId(2)
	if !ok || addr.StorageRPCPort != 32003 || addr.Addr() != "127.0.0.1:33001" {
		t.Fatalf("addr = %+v ok=%v", addr, ok)
	}
}

// Spec 2026-10-09 §6.4 (D19): the agent's switch lookup separates "not
// hosted" (null or PendingDelete database) from a failed lookup.
func TestRpcNetworkState_DatabaseHosting(t *testing.T) {
	var _ registry.DatabaseHosting = (*network.RpcNetworkState)(nil)
	indexer := network.IndexerInfo{IndexerId: 2, IndexerUrl: "10.0.0.2", ClickhouseProxyPort: 9000}
	db := network.DatabaseInfo{DatabaseId: "shop", IndexerId: 2}
	dbErr, indexerErr := false, false
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getDatabaseInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			if dbErr {
				return nil, &rpcErrEnvelope{Code: -32000, Message: "boom"}
			}
			if params[0] == "shop" {
				return db, nil
			}
			return nil, nil
		},
		"sentio_getIndexerInfoById": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			if indexerErr {
				return nil, &rpcErrEnvelope{Code: -32000, Message: "boom"}
			}
			if indexer.IndexerId == 0 {
				return nil, nil
			}
			return indexer, nil
		},
	})
	ctx := context.Background()
	addr, id, hosted, err := rpc.DatabaseHosting(ctx, "shop")
	if err != nil || !hosted || id != 2 || addr.Addr() != "10.0.0.2:9000" {
		t.Fatalf("hosted = %+v %d %v %v", addr, id, hosted, err)
	}
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "nope"); err != nil || hosted {
		t.Fatalf("unknown database: hosted=%v err=%v; want not hosted, no error", hosted, err)
	}
	db.PendingDelete = true
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "shop"); err != nil || hosted {
		t.Fatalf("pending delete: hosted=%v err=%v; want not hosted, no error", hosted, err)
	}
	db.PendingDelete = false

	dbErr = true
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "shop"); err == nil || hosted {
		t.Fatalf("database RPC error: hosted=%v err=%v; want an error", hosted, err)
	}
	dbErr = false
	indexerErr = true
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "shop"); err == nil || hosted {
		t.Fatalf("indexer RPC error: hosted=%v err=%v; want an error", hosted, err)
	}
	indexerErr = false
	indexer.ClickhouseProxyPort = 0
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "shop"); err == nil || hosted {
		t.Fatalf("no housegate port: hosted=%v err=%v; want an error", hosted, err)
	}
	indexer = network.IndexerInfo{}
	if _, _, hosted, err := rpc.DatabaseHosting(ctx, "shop"); err == nil || hosted {
		t.Fatalf("unknown indexer: hosted=%v err=%v; want an error", hosted, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := rpc.DatabaseHosting(canceled, "nope"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: err=%v; want context.Canceled", err)
	}
}
