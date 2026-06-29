package housegate

import (
	"context"
	"testing"
	"time"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/plugins/commitgate"
	storageintegrityplugin "housegate/housegate/pkg/plugins/storageintegrity"
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
		Rewriter:     stubRewriterFactory{},
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

func TestBuildStorageIntegrityRuntimeUsesRealReplayAndSafeAuditDeps(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.Upstream = "127.0.0.1:56301"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers = config.StorageIntegrityWorkersConfig{
		PollInterval: config.Duration{Duration: cfg.DialTimeout.Duration},
		Replay:       true,
		Promotion:    false,
		Rollback:     false,
		SafeAudit:    true,
		Finality:     false,
	}

	rt, err := buildStorageIntegrityRuntime(cfg.StorageIntegrity, Options{Config: cfg})
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntime: %v", err)
	}
	if _, ok := rt.Replay.Verifier.(*storageintegrity.ClickHouseInsertReplayVerifier); !ok {
		t.Fatalf("replay verifier = %T, want *ClickHouseInsertReplayVerifier", rt.Replay.Verifier)
	}
	if _, ok := rt.SafeAudit.Reader.(*storageintegrity.ClickHouseSafeAuditReader); !ok {
		t.Fatalf("safe audit reader = %T, want *ClickHouseSafeAuditReader", rt.SafeAudit.Reader)
	}
	if _, ok := rt.SafeAudit.Signer.(*storageintegrity.Ed25519WorkerSigner); !ok {
		t.Fatalf("safe audit signer = %T, want *Ed25519WorkerSigner", rt.SafeAudit.Signer)
	}
}

func TestBuildServerStorageIntegrityRunsBeforeCommitGate(t *testing.T) {
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
		Config:              cfg,
		NetworkState:        network.NewInMemoryNetworkState(),
		Rewriter:            stubRewriterFactory{},
		CommitGateObservers: []commitgate.Observer{network.NewPermissionCommitGateObserver(network.NewInMemoryNetworkState())},
	}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	var siIdx, cgIdx = -1, -1
	for i, p := range bs.listeners[0].Runner.(*proxyServerRunner).server.Hooks.(*plugin.PluginChain).QueryPlugins {
		switch p.(type) {
		case *storageintegrityplugin.Plugin:
			siIdx = i
		case *commitgate.Plugin:
			cgIdx = i
		}
	}
	if siIdx < 0 || cgIdx < 0 {
		t.Fatalf("storage-integrity idx=%d commitgate idx=%d, want both present", siIdx, cgIdx)
	}
	if siIdx > cgIdx {
		t.Fatalf("storage-integrity idx=%d must run before commitgate idx=%d", siIdx, cgIdx)
	}
}

func TestBuildStorageIntegrityRuntimeUsesInjectedHouseKeeperControlPlane(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.Upstream = "127.0.0.1:56301"
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

func TestBuildStorageIntegrityRuntimeAppliesQueryTimeoutToPromotionExecutor(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.Upstream = "127.0.0.1:56301"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.UnsafeValidation.QueryTimeout = config.Duration{Duration: 45 * time.Second}
	cfg.StorageIntegrity.Workers = config.StorageIntegrityWorkersConfig{
		PollInterval: config.Duration{Duration: cfg.DialTimeout.Duration},
		Replay:       false,
		Promotion:    true,
		Rollback:     false,
		SafeAudit:    false,
		Finality:     false,
	}

	rt, err := buildStorageIntegrityRuntime(cfg.StorageIntegrity, Options{Config: cfg})
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntime: %v", err)
	}
	exec, ok := rt.Promotion.Executor.(*storageintegrity.ClickHousePromotionExecutor)
	if !ok {
		t.Fatalf("promotion executor = %T, want *ClickHousePromotionExecutor", rt.Promotion.Executor)
	}
	if exec.StatementTimeout != 45*time.Second || exec.ReadbackTimeout != 45*time.Second {
		t.Fatalf("promotion timeouts = statement %s readback %s, want 45s", exec.StatementTimeout, exec.ReadbackTimeout)
	}
}

type fakeStorageIntegrityExecutor struct{}

func (fakeStorageIntegrityExecutor) ExecPromotionSQL(context.Context, string) error { return nil }

func (fakeStorageIntegrityExecutor) ReadPromotionRows(context.Context, storageintegrity.PromotionReadbackSpec) (storageintegrity.PromotionReadbackResult, error) {
	return storageintegrity.PromotionReadbackResult{}, nil
}

func (fakeStorageIntegrityExecutor) ExecRollbackSQL(context.Context, string) error { return nil }
