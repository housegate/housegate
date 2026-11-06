package network_test

import (
	"testing"

	"github.com/housegate/housegate/pkg/network"
)

func TestRpcNetworkState_IsIndexerSigner(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return []network.IndexerInfo{
				{IndexerId: 0, IndexerUrl: "node-a", Signer: "0x00000000000000000000000000000000000000AA"},
				{IndexerId: 1, IndexerUrl: "node-b"},
			}, nil
		},
	})

	if !rpc.IsIndexerSigner("0x00000000000000000000000000000000000000aa") {
		t.Error("registered indexer signer not recognised")
	}
	if rpc.IsIndexerSigner("0x00000000000000000000000000000000000000bb") {
		t.Error("unregistered address recognised as an indexer signer")
	}
	if rpc.IsIndexerSigner("") {
		t.Error("empty address matched the indexer that has no signer")
	}
}

// A lookup that fails must not admit anyone.
func TestRpcNetworkState_IsIndexerSigner_LookupFailureRefuses(t *testing.T) {
	rpc, _ := newFakeRpc(t, map[string]rpcMethod{
		"sentio_getIndexerInfos": func(_ []interface{}) (interface{}, *rpcErrEnvelope) {
			return nil, &rpcErrEnvelope{Code: -32000, Message: "boom"}
		},
	})
	if rpc.IsIndexerSigner("0x00000000000000000000000000000000000000aa") {
		t.Error("lookup failure admitted an address as an indexer signer")
	}
}
