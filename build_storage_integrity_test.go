package housegate

import (
	"context"
	"testing"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/storageintegrity"
)

func TestBuildServerStorageIntegrityAddsBackgroundRuntime(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers = config.StorageIntegrityWorkersConfig{
		PollInterval: config.Duration{Duration: cfg.DialTimeout.Duration},
		Replay:       false,
		Promotion:    false,
		Rollback:     false,
		SafeAudit:    false,
		Finality:     false,
	}

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()
	if len(bs.background) != 1 {
		t.Fatalf("background runners = %d, want 1", len(bs.background))
	}
	if bs.background[0].Label != "storage_integrity" {
		t.Fatalf("background label = %q, want storage_integrity", bs.background[0].Label)
	}
}

func TestBuildServerStorageIntegrityUsesDefaultMockReplayDeps(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers.PollInterval = config.Duration{Duration: cfg.DialTimeout.Duration}
	cfg.StorageIntegrity.Workers.Replay = true
	cfg.StorageIntegrity.Workers.Promotion = false
	cfg.StorageIntegrity.Workers.Rollback = false
	cfg.StorageIntegrity.Workers.SafeAudit = false
	cfg.StorageIntegrity.Workers.Finality = false

	bs, err := buildServer(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()
	if len(bs.background) != 1 {
		t.Fatalf("background runners = %d, want 1", len(bs.background))
	}
}

func TestBuildStorageIntegrityRuntimeUsesInjectedHouseKeeperControlPlane(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.HouseKeeper.Endpoints = []string{"127.0.0.1:9181"}
	cfg.StorageIntegrity.HouseKeeper.WorkerID = "hg-1"
	cfg.StorageIntegrity.HouseKeeper.ReplayQuorum = 2
	cfg.StorageIntegrity.Workers.PollInterval = config.Duration{Duration: cfg.DialTimeout.Duration}

	control := storageintegrity.NewLocalCoordinator(storageintegrity.LocalCoordinatorConfig{})
	rt, err := buildStorageIntegrityRuntime(cfg.StorageIntegrity, Options{
		Config:                        cfg,
		StorageIntegrityControlPlane:  control,
		StorageIntegrityPromotionExec: fakeStorageIntegrityExecutor{},
		StorageIntegrityRollbackExec:  fakeStorageIntegrityExecutor{},
	})
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntime: %v", err)
	}

	if rt.Ingress != control {
		t.Fatalf("Ingress = %T, want injected control plane", rt.Ingress)
	}
	if rt.ReplayJobs != control {
		t.Fatalf("ReplayJobs = %T, want injected control plane", rt.ReplayJobs)
	}
	if rt.Promotions != control {
		t.Fatalf("Promotions = %T, want injected control plane", rt.Promotions)
	}
	if rt.Rollbacks != control {
		t.Fatalf("Rollbacks = %T, want injected control plane", rt.Rollbacks)
	}
	if rt.SafeAudits != control {
		t.Fatalf("SafeAudits = %T, want injected control plane", rt.SafeAudits)
	}
}

type fakeStorageIntegrityExecutor struct{}

func (fakeStorageIntegrityExecutor) ExecPromotionSQL(context.Context, string) error { return nil }

func (fakeStorageIntegrityExecutor) ReadPromotionRows(context.Context, storageintegrity.PromotionReadbackSpec) (storageintegrity.PromotionReadbackResult, error) {
	return storageintegrity.PromotionReadbackResult{}, nil
}

func (fakeStorageIntegrityExecutor) ExecRollbackSQL(context.Context, string) error { return nil }
