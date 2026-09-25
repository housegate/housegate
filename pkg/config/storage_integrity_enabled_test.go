package config

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func boolPtr(v bool) *bool { return &v }

func TestStorageIntegrityEnabledDefaultsToTables(t *testing.T) {
	var c StorageIntegrityConfig
	if c.IsEnabled() {
		t.Fatal("no tables and no switch must be disabled")
	}
	c.Tables = []string{"db1.t"}
	if !c.IsEnabled() {
		t.Fatal("configured tables with no switch must be enabled")
	}
	c.Enabled = boolPtr(false)
	if c.IsEnabled() {
		t.Fatal("an explicit false wins")
	}
	c.Tables = nil
	c.Enabled = boolPtr(true)
	if !c.IsEnabled() {
		t.Fatal("an explicit true with no tables is enabled (injected TableState)")
	}
}

func TestStorageIntegrityEnabledYAML(t *testing.T) {
	var c StorageIntegrityConfig
	if err := yaml.Unmarshal([]byte("enabled: true\n"), &c); err != nil {
		t.Fatal(err)
	}
	if c.Enabled == nil || !*c.Enabled {
		t.Fatalf("enabled: true decoded as %v", c.Enabled)
	}
	var absent StorageIntegrityConfig
	if err := yaml.Unmarshal([]byte("tables: [db1.t]\n"), &absent); err != nil {
		t.Fatal(err)
	}
	if absent.Enabled != nil || !absent.IsEnabled() {
		t.Fatalf("an absent switch must stay nil and default from tables, got %v", absent.Enabled)
	}
}

func TestStorageIntegrityEnabledValidation(t *testing.T) {
	t.Run("explicit false with tables is rejected", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Enabled = boolPtr(false)
		cfg.StorageIntegrity.Tables = []string{"db1.t"}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.tables requires storage_integrity.enabled (it is explicitly false)") {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("explicit true without tables is valid in server mode", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("explicit true is server mode only", func(t *testing.T) {
		cfg := Default()
		cfg.Agent = agentConfigForStorageIntegrityTest()
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.enabled is server mode only") {
			t.Fatalf("Validate err = %v", err)
		}
	})
	t.Run("read.default_mode requires enabled", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Read.DefaultMode = "safe"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.read.default_mode requires storage_integrity.enabled") {
			t.Fatalf("Validate err = %v", err)
		}
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("an explicit switch satisfies read.default_mode: %v", err)
		}
	})
	t.Run("runtime accepts an explicit switch without tables", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		cfg.StorageIntegrity.Enabled = boolPtr(true)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate err = %v", err)
		}
	})
}
