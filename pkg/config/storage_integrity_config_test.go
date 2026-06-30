package config

import (
	"strings"
	"testing"
	"time"
)

func TestStorageIntegrityDefaultDisabled(t *testing.T) {
	cfg := Default()
	if cfg.StorageIntegrity.Enabled {
		t.Fatal("storage integrity must be disabled by default")
	}
}

func TestStorageIntegrityDefaultUnsafeTableSuffixIsEmpty(t *testing.T) {
	cfg := Default()
	if cfg.StorageIntegrity.UnsafeTableSuffix != "" {
		t.Fatalf("UnsafeTableSuffix = %q, want empty", cfg.StorageIntegrity.UnsafeTableSuffix)
	}
}

func TestStorageIntegrityValidateRequiresMockPayloadPathWhenEnabled(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = ""

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.mock_payload_store.path") {
		t.Fatalf("Validate() = %v, want mock payload store path error", err)
	}
}

func TestStorageIntegrityValidateAcceptsMockP0Config(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.Workers.PollInterval = Duration{Duration: 10 * time.Millisecond}
	cfg.StorageIntegrity.MockFinality.Delay = Duration{Duration: time.Millisecond}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestStorageIntegrityValidateAcceptsHouseKeeperControlPlaneConfig(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.HouseKeeper.Endpoints = []string{"127.0.0.1:9181", "127.0.0.1:9182", "127.0.0.1:9183"}
	cfg.StorageIntegrity.HouseKeeper.ReplayQuorum = 2

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestStorageIntegrityValidateRejectsAgentMode(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.Agent.Mode = true
	cfg.Agent.Upstream = "127.0.0.1:9000"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "server mode only") {
		t.Fatalf("Validate() = %v, want server mode only error", err)
	}
}

func TestStorageIntegrityValidateAcceptsSingleUnsafeReplicaForLocalSidecarMode(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.UnsafeValidation.Replicas = []StorageIntegrityUnsafeReplica{{
		ReplicaID: "r1",
		Addr:      "127.0.0.1:9000",
	}}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestStorageIntegrityValidateRejectsInvalidSafeAuditReplica(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.SafeAudit.Replicas = []StorageIntegritySafeAuditReplica{{}}

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.safe_audit.replicas[0].replica_id") {
		t.Fatalf("Validate() = %v, want safe audit replica_id error", err)
	}
}

func TestStorageIntegrityValidateRejectsInvalidMockPartRegistryPartition(t *testing.T) {
	cfg := Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.NetworkState.Source = "127.0.0.1:6379"
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.MockPayloadStore.Path = t.TempDir()
	cfg.StorageIntegrity.MockPartRegistry.PartitionIDs = []string{" "}

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.mock_part_registry.partition_ids[0]") {
		t.Fatalf("Validate() = %v, want mock part registry partition error", err)
	}
}
