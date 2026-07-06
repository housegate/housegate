package housegate

import (
	"reflect"
	"sort"
	"testing"

	"housegate/housegate/pkg/network"
)

func TestBuildServerStorageIntegrityWiresWorkerRuntime(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.DAEndpoint = "http://127.0.0.1:18080"
	cfg.StorageIntegrity.ArbiterEndpoint = "http://127.0.0.1:18081"
	cfg.StorageIntegrity.Workers.Enabled = true
	cfg.StorageIntegrity.Workers.WorkerID = "hg-a"
	cfg.StorageIntegrity.Workers.ClickHouseAddr = "127.0.0.1:9000"
	cfg.StorageIntegrity.Workers.ClaimPrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cfg.StorageIntegrity.Workers.ReplaySignerSeedHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	cfg.StorageIntegrity.Workers.Replay = true
	cfg.StorageIntegrity.Workers.UnsafeValidation = true
	cfg.StorageIntegrity.Workers.Promotion = true
	cfg.StorageIntegrity.Workers.Mutation = true
	cfg.StorageIntegrity.Workers.Rollback = true
	cfg.StorageIntegrity.Workers.RepairSync = true
	cfg.StorageIntegrity.Workers.SafeAudit = true
	cfg.StorageIntegrity.Workers.Compaction = true

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	var labels []string
	for _, task := range bs.backgroundTasks {
		labels = append(labels, task.Label)
	}
	sort.Strings(labels)
	want := []string{
		"storage-integrity-compaction",
		"storage-integrity-mutation",
		"storage-integrity-promotion",
		"storage-integrity-repair-sync",
		"storage-integrity-rollback",
		"storage-integrity-safe-audit",
		"storage-integrity-table-guard",
		"storage-integrity-verifier",
	}
	if !reflect.DeepEqual(labels, want) {
		t.Fatalf("background task labels = %#v, want %#v", labels, want)
	}
}
