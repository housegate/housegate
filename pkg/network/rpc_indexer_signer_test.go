package network_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/housegate/housegate/pkg/endpointtls"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// TestRpcNetworkStateIndexerSignerClient reads the source over its own client
// (plain http here) and reaches the hosting indexer's storage RPC over a
// signer-verified https client looked up in the same state.
func TestRpcNetworkStateIndexerSignerClient(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.PubkeyToAddress(key.PublicKey).Hex()
	cert, err := endpointtls.NewSignerCertificate(key, 7892301, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	indexer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(rpcResponse{JsonRpc: "2.0", ID: req.ID, Result: json.RawMessage(`{}`)})
	}))
	indexer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	indexer.StartTLS()
	t.Cleanup(indexer.Close)
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(indexer.URL, "https://"))
	port, _ := strconv.ParseUint(portText, 10, 16)

	registered := signer
	info := func() network.IndexerInfo {
		return network.IndexerInfo{IndexerId: 3, IndexerUrl: host, StorageNodeRpcPort: uint16(port), ClickhouseProxyPort: 1, Signer: registered}
	}
	fake := &fakeRpcServer{methods: map[string]rpcMethod{
		"sentio_getDatabaseInfoById": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return network.DatabaseInfo{DatabaseId: "db", IndexerId: 3}, nil
		},
		"sentio_getIndexerInfoById": func([]interface{}) (interface{}, *rpcErrEnvelope) { return info(), nil },
		"sentio_getIndexerInfos": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return []network.IndexerInfo{info()}, nil
		},
	}}
	source := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(source.Close)

	rpc, err := network.NewRpcNetworkState(source.URL, network.RpcOptions{IndexerScheme: "https"})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := endpointtls.NewSigner(7892301, func(addr string) (string, error) { return registry.SignerForEndpoint(rpc, addr) })
	if err != nil {
		t.Fatal(err)
	}
	rpc.SetIndexerHTTPClient(cfg.HTTPClient(2 * time.Second))

	if a := rpc.AllIndexers()[3]; a.Signer != signer || a.StorageRPCPort != uint16(port) {
		t.Fatalf("topology entry %+v", a)
	}
	if _, err := rpc.StorageIntegrityInfo(context.Background(), "db"); err != nil {
		t.Fatalf("signer-verified indexer call: %v", err)
	}
	// Verification runs per connection: a new client (no kept-alive
	// connection) re-checks against the signer registered now.
	registered = "0x0000000000000000000000000000000000000001"
	rpc.SetIndexerHTTPClient(cfg.HTTPClient(2 * time.Second))
	if _, err := rpc.StorageIntegrityInfo(context.Background(), "db"); err == nil || !strings.Contains(err.Error(), "presented a certificate of signer") {
		t.Fatalf("another registered signer: %v", err)
	}
}
