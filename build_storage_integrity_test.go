package housegate

import (
	"testing"

	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
	storageintegrityplugin "housegate/housegate/pkg/plugins/storageintegrity"
)

func TestBuildServerStorageIntegrityWiresInsertPlugin(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.DAEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.SequencerEndpoint = "http://127.0.0.1:18080"

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	chain := requireProxyServer(t, bs.listeners[0]).Hooks.(*plugin.PluginChain)
	if !containsStorageIntegrityQueryPlugin(chain.QueryPlugins) {
		t.Fatalf("storage-integrity query plugin was not wired")
	}
	if !containsStorageIntegrityDataPlugin(chain.DataPlugins) {
		t.Fatalf("storage-integrity data plugin was not wired")
	}
	if !containsStorageIntegrityCompletePlugin(chain.QueryCompletePlugins) {
		t.Fatalf("storage-integrity query-complete plugin was not wired")
	}
}

func containsStorageIntegrityQueryPlugin(plugins []plugin.QueryPlugin) bool {
	for _, p := range plugins {
		if _, ok := p.(*storageintegrityplugin.Plugin); ok {
			return true
		}
	}
	return false
}

func containsStorageIntegrityDataPlugin(plugins []plugin.DataPlugin) bool {
	for _, p := range plugins {
		if _, ok := p.(*storageintegrityplugin.Plugin); ok {
			return true
		}
	}
	return false
}

func containsStorageIntegrityCompletePlugin(plugins []plugin.QueryCompletePlugin) bool {
	for _, p := range plugins {
		if _, ok := p.(*storageintegrityplugin.Plugin); ok {
			return true
		}
	}
	return false
}
