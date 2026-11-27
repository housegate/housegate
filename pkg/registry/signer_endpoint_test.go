package registry_test

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

type topology map[uint64]registry.ProxyAddress

func (t topology) ProxyByIndexerId(id uint64) (registry.ProxyAddress, bool) {
	a, ok := t[id]
	return a, ok
}

func (t topology) AllIndexers() map[uint64]registry.ProxyAddress {
	if t == nil {
		return nil
	}
	out := make(map[uint64]registry.ProxyAddress, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}

func TestSignerForEndpoint(t *testing.T) {
	topo := topology{
		1: {Url: "Node-A.example", HousegatePort: 31001, ComputeRPCPort: 31002, StorageRPCPort: 31003, Signer: "0xAAAA"},
		2: {Url: "10.0.0.2", HousegatePort: 31001, StorageRPCPort: 31003},
	}
	for addr, want := range map[string]string{
		"node-a.example:31001": "0xaaaa",
		"node-a.example:31002": "0xaaaa",
		"NODE-A.example:31003": "0xaaaa",
		"10.0.0.2:31003":       "",
	} {
		got, err := registry.SignerForEndpoint(topo, addr)
		if err != nil || got != want {
			t.Errorf("%s: %q, %v; want %q", addr, got, err, want)
		}
	}
	for addr, want := range map[string]string{
		"node-a.example:9999":  "not advertised",
		"node-b.example:31001": "not advertised",
		"node-a.example":       "missing port",
		"node-a.example:0":     "invalid port",
	} {
		if _, err := registry.SignerForEndpoint(topo, addr); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", addr, err, want)
		}
	}
	if _, err := registry.SignerForEndpoint(topology(nil), "a:1"); err == nil || !strings.Contains(err.Error(), "could not be read") {
		t.Errorf("unreadable topology: %v", err)
	}
	split := topology{
		1: {Url: "shared.example", HousegatePort: 31001, Signer: "0xaaaa"},
		2: {Url: "shared.example", StorageRPCPort: 31001, Signer: "0xbbbb"},
	}
	if _, err := registry.SignerForEndpoint(split, "shared.example:31001"); err == nil || !strings.Contains(err.Error(), "different signers") {
		t.Errorf("ambiguous: %v", err)
	}
}
