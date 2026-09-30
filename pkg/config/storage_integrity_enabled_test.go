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

// TestValidate_FailOpenOnUnavailableRejectedWithStorageIntegrity pins spec
// 2026-09-26 T8: the transport fail-open switch is a configuration error
// together with storage integrity, which must fail closed on every outage.
func TestValidate_FailOpenOnUnavailableRejectedWithStorageIntegrity(t *testing.T) {
	const want = "rewriter.fail_open_on_unavailable cannot be combined with storage_integrity.enabled"
	for name, enable := range map[string]func(*Config){
		"tables":          func(c *Config) { c.StorageIntegrity.Tables = []string{"db1.t"} },
		"explicit switch": func(c *Config) { c.StorageIntegrity.Enabled = boolPtr(true) },
	} {
		t.Run(name, func(t *testing.T) {
			c := minimalServerConfig(t)
			c.Rewriter.FailOpenOnUnavailable = true
			enable(&c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want %q", err, want)
			}
		})
	}
	t.Run("switch alone is valid", func(t *testing.T) {
		c := minimalServerConfig(t)
		c.Rewriter.FailOpenOnUnavailable = true
		if err := c.Validate(); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("default is off", func(t *testing.T) {
		if Default().Rewriter.FailOpenOnUnavailable {
			t.Fatal("rewriter.fail_open_on_unavailable must default to false")
		}
	})
	t.Run("yaml key", func(t *testing.T) {
		var c Config
		if err := yaml.Unmarshal([]byte("rewriter:\n  fail_open_on_unavailable: true\n"), &c); err != nil {
			t.Fatal(err)
		}
		if !c.Rewriter.FailOpenOnUnavailable {
			t.Fatal("rewriter.fail_open_on_unavailable did not decode")
		}
	})
}
