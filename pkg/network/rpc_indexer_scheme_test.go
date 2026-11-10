package network

import "testing"

// TestStorageEndpointScheme pins how the storage RPC endpoint an indexer
// advertises gets its scheme: the configured indexer scheme when set (a
// plaintext loopback source with TLS endpoints, or the reverse), else the
// source's scheme as before the setting existed.
func TestStorageEndpointScheme(t *testing.T) {
	for name, tc := range map[string]struct {
		source, scheme, want string
	}{
		"legacy http source":            {"http://127.0.0.1:32003", "", "http://idx.example.com:33003"},
		"legacy https source":           {"https://boot.example.com", "", "https://idx.example.com:33003"},
		"https endpoints, loopback src": {"http://localhost:32003", "https", "https://idx.example.com:33003"},
		"http endpoints, https src":     {"https://boot.example.com", "http", "http://idx.example.com:33003"},
	} {
		r, err := NewRpcNetworkState(tc.source, RpcOptions{IndexerScheme: tc.scheme})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := r.storageEndpoint("idx.example.com", 33003); got != tc.want {
			t.Errorf("%s: storageEndpoint = %q, want %q", name, got, tc.want)
		}
	}
}

func TestNewRpcNetworkStateRejectsUnknownIndexerScheme(t *testing.T) {
	if _, err := NewRpcNetworkState("http://127.0.0.1:1", RpcOptions{IndexerScheme: "tls"}); err == nil {
		t.Fatal("want an error for an unknown indexer scheme")
	}
}
