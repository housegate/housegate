package housegate

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/replay"
)

func TestBuildServerStorageIntegrityAddsBackgroundRuntime(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers = config.StorageIntegrityWorkersConfig{
		PollInterval: config.Duration{Duration: cfg.DialTimeout.Duration},
		Replay:       false,
		Promotion:    false,
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

func TestBuildServerStorageIntegrityRequiresReplayDepsWhenReplayWorkerEnabled(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers.PollInterval = config.Duration{Duration: cfg.DialTimeout.Duration}
	cfg.StorageIntegrity.Workers.Replay = true
	cfg.StorageIntegrity.Workers.Promotion = false
	cfg.StorageIntegrity.Workers.SafeAudit = false
	cfg.StorageIntegrity.Workers.Finality = false

	_, err := buildServer(Options{
		Config:                     cfg,
		NetworkState:               network.NewInMemoryNetworkState(),
		StorageIntegrityReplayJobs: emptyReplayJobSource{},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity replay verifier") {
		t.Fatalf("buildServer error = %v, want replay verifier dependency error", err)
	}
}

type emptyReplayJobSource struct{}

func (emptyReplayJobSource) ClaimReplayJob(context.Context) (replay.ReplayJob, bool, error) {
	return replay.ReplayJob{}, false, nil
}
