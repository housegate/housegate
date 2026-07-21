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

	t.Run("enabling controlled compaction rejected in v1", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.SafeMerges.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "safe_merges is not runnable in v1") {
			t.Fatalf("Validate err = %v, want safe_merges v1 rejection", err)
		}
	})

	t.Run("safe_merges server mode only", func(t *testing.T) {
		cfg := Config{Listen: ":9001", Agent: agentConfigForStorageIntegrityTest()}
		cfg.StorageIntegrity.SafeMerges.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "safe_merges is server mode only") {
			t.Fatalf("Validate err = %v, want safe_merges server-mode rejection", err)
		}
	})

	t.Run("unsupported safe_merges mode rejected", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.SafeMerges.Enabled = true
		cfg.StorageIntegrity.SafeMerges.Mode = "bogus"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "is not supported") {
			t.Fatalf("Validate err = %v, want unsupported-mode rejection", err)
		}
	})
}

func TestConfigStorageIntegrityPartLtHashCache(t *testing.T) {
	t.Run("defaults off", func(t *testing.T) {
		if Default().StorageIntegrity.PartLtHashCache.Enabled {
			t.Fatal("storage_integrity.part_lthash_cache.enabled defaulted true, want false")
		}
	})

	t.Run("enabling is accepted (discardable local layer, no companion seam)", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.PartLtHashCache.Enabled = true
		cfg.StorageIntegrity.PartLtHashCache.Path = "/var/lib/housegate/part-lthash.db"
		cfg.StorageIntegrity.PartLtHashCache.MaxEntries = 1000000
		if err := cfg.Validate(); err != nil {
			t.Fatalf("part_lthash_cache is a runnable local layer and must validate: %v", err)
		}
	})
}

func TestConfigStorageIntegrityMutation(t *testing.T) {
	t.Run("defaults off", func(t *testing.T) {
		if Default().StorageIntegrity.Mutation.Enabled {
			t.Fatal("storage_integrity.mutation.enabled defaulted true, want false")
		}
	})

	t.Run("enabling mutation rejected in v1", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Mutation.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "mutation is not runnable in v1") {
			t.Fatalf("Validate err = %v, want mutation v1 rejection", err)
		}
	})

	t.Run("mutation server mode only", func(t *testing.T) {
		cfg := Config{Listen: ":9001", Agent: agentConfigForStorageIntegrityTest()}
		cfg.StorageIntegrity.Mutation.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "mutation is server mode only") {
			t.Fatalf("Validate err = %v, want mutation server-mode rejection", err)
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
