package config

import (
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/plugins/agent"
)

func TestStorageIntegrityDisabledIsNoOp(t *testing.T) {
	cfg := Default()
	if cfg.StorageIntegrity.Ingress.Enabled {
		t.Fatal("storage_integrity.ingress.enabled defaulted true, want false")
	}
	if cfg.StorageIntegrity.Ingress.RequestTimeout.Duration <= 0 {
		t.Fatalf("storage_integrity.ingress.request_timeout = %s, want non-zero default", cfg.StorageIntegrity.Ingress.RequestTimeout)
	}
	if cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration != time.Minute {
		t.Fatalf("storage_integrity.ingress.max_token_age = %s, want 1m", cfg.StorageIntegrity.Ingress.MaxTokenAge)
	}
	if cfg.StorageIntegrity.Ingress.MaxPayloadBytes <= 0 {
		t.Fatalf("storage_integrity.ingress.max_payload_bytes = %d, want positive default", cfg.StorageIntegrity.Ingress.MaxPayloadBytes)
	}
	if cfg.StorageIntegrity.Runtime.Enabled {
		t.Fatal("storage_integrity.runtime.enabled defaulted true, want false")
	}
}

func TestConfigValidateStorageIntegrityIngress(t *testing.T) {
	base := minimalServerConfig(t)
	base.StorageIntegrity.Ingress.Enabled = true
	base.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	base.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	base.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
	base.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
	if err := base.Validate(); err != nil {
		t.Fatalf("storage integrity ingress server config should validate: %v", err)
	}

	t.Run("agent mode rejected", func(t *testing.T) {
		cfg := Config{
			Listen: ":9001",
			Agent:  agentConfigForStorageIntegrityTest(),
		}
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress is server mode only") {
			t.Fatalf("Validate err = %v, want server-mode rejection", err)
		}
	})

	t.Run("signer allowlist required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "allowed_addresses") {
			t.Fatalf("Validate err = %v, want allowlist rejection", err)
		}
	})

	t.Run("positive timeout required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 0
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "request_timeout") {
			t.Fatalf("Validate err = %v, want timeout rejection", err)
		}
	})

	t.Run("positive max payload required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 0
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "max_payload_bytes") {
			t.Fatalf("Validate err = %v, want max payload rejection", err)
		}
	})

	t.Run("runtime enabled requires ingress", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.enabled requires storage_integrity.ingress.enabled") {
			t.Fatalf("Validate err = %v, want runtime ingress dependency rejection", err)
		}
	})

	t.Run("runtime enabled requires expected source", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Runtime.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.expected_source") {
			t.Fatalf("Validate err = %v, want expected source rejection", err)
		}
	})

	t.Run("runtime enabled requires durable runtime config", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded without durable runtime config")
		}
		for _, want := range []string{
			"storage_integrity.runtime.journal_dir",
			"storage_integrity.runtime.payload_spool_dir",
			"storage_integrity.runtime.merge_guard.tables",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("runtime merge guard rejects empty table identifiers", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.MergeGuard.Tables = []StorageIntegrityRuntimeMergeTableConfig{
			{Database: "hg_safe"},
			{Table: "events"},
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with incomplete merge_guard tables")
		}
		for _, want := range []string{
			"storage_integrity.runtime.merge_guard.tables[0].table",
			"storage_integrity.runtime.merge_guard.tables[1].database",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("runtime enabled accepts complete durable config", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("storage integrity runtime config should validate: %v", err)
		}
	})
}

func storageIntegrityRuntimeConfigFixture(t *testing.T) Config {
	t.Helper()
	cfg := minimalServerConfig(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
	cfg.StorageIntegrity.Runtime.Enabled = true
	cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
	cfg.StorageIntegrity.Runtime.JournalDir = "/var/lib/housegate/storage-integrity/journal"
	cfg.StorageIntegrity.Runtime.PayloadSpoolDir = "/var/lib/housegate/storage-integrity/payload-spool"
	cfg.StorageIntegrity.Runtime.MergeGuard.Tables = []StorageIntegrityRuntimeMergeTableConfig{
		{Database: "hg_safe", Table: "events"},
		{Database: "hg_unsafe", Table: "events"},
	}
	return cfg
}

func TestConfigStorageIntegritySafeMerges(t *testing.T) {
	t.Run("defaults off", func(t *testing.T) {
		cfg := Default()
		if cfg.StorageIntegrity.SafeMerges.AllowNativeBackgroundMerges {
			t.Fatal("safe_merges.allow_native_background_merges defaulted true, want false")
		}
	})

	t.Run("enabling native merges rejected", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.SafeMerges.AllowNativeBackgroundMerges = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "allow_native_background_merges") {
			t.Fatalf("Validate err = %v, want native-merges rejection", err)
		}
	})
}

func agentConfigForStorageIntegrityTest() agent.Config {
	return agent.Config{
		Mode:          true,
		Upstream:      "proxy:9001",
		PrivateKeyHex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}
