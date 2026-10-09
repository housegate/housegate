package config

import (
	"strings"
	"testing"
	"time"

	materializeplugin "github.com/housegate/housegate/pkg/plugins/materialize"
)

func agentSIBase() *Config {
	c := Default()
	c.Listen = "127.0.0.1:0"
	c.Agent.Mode = true
	c.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	c.Agent.Upstream = "127.0.0.1:9000"
	c.NetworkState.Source = "network_state.yaml"
	c.StorageIntegrity.Agent.Enabled = true
	c.StorageIntegrity.Agent.NetworkID = "testnet-v2"
	c.StorageIntegrity.Agent.StateDir = "/tmp/hg-si-agent"
	return &c
}

func TestStorageIntegrityAgentConfig_Defaults(t *testing.T) {
	d := Default()
	if d.StorageIntegrity.Agent.Enabled {
		t.Fatal("agent SI must default off")
	}
	if d.StorageIntegrity.Agent.MaxPayloadBytes != 64<<20 {
		t.Fatalf("default max_payload_bytes = %d", d.StorageIntegrity.Agent.MaxPayloadBytes)
	}
	if !d.StorageIntegrity.Agent.RequireNetworkState {
		t.Fatal("require_network_state must default true")
	}
	if d.StorageIntegrity.Agent.KeeperShardID != 0 {
		t.Fatal("keeper_shard_id must default 0")
	}
}

func TestStorageIntegrityAgentConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(*Config) {}, ""},
		{"server mode rejects", func(c *Config) {
			c.Agent.Mode = false
			c.Upstream = "127.0.0.1:9000"
			c.CkhManagerConfigPath = "/tmp/x.yaml"
		}, "agent mode only"},
		{"missing network_id", func(c *Config) { c.StorageIntegrity.Agent.NetworkID = " " }, "network_id"},
		{"non-zero shard", func(c *Config) { c.StorageIntegrity.Agent.KeeperShardID = 3 }, "keeper_shard_id"},
		{"missing state_dir uses the per-OS default", func(c *Config) { c.StorageIntegrity.Agent.StateDir = "" }, ""},
		{"rpc source discovers the network id", func(c *Config) {
			c.StorageIntegrity.Agent.NetworkID = ""
			c.NetworkState.Source = "http://node:10003"
		}, ""},
		{"host-injected state may discover the network id", func(c *Config) {
			c.StorageIntegrity.Agent.NetworkID = ""
			c.NetworkState.Source = ""
			c.StorageIntegrity.Agent.RequireNetworkState = false
		}, ""},
		{"bad lanes", func(c *Config) { c.StorageIntegrity.Agent.Lanes = "on" }, "storage_integrity.agent.lanes"},
		{"bad read_mode", func(c *Config) { c.StorageIntegrity.Agent.ReadMode = "fast" }, "storage_integrity.agent.read_mode"},
		{"lanes and read_mode", func(c *Config) {
			c.StorageIntegrity.Agent.Lanes = "off"
			c.StorageIntegrity.Agent.ReadMode = "unsafe_latest"
		}, ""},
		// The read-mode injector runs without the SI statement plugin, so the
		// values are checked even when the block is disabled (F20).
		{"bad read_mode with SI off", func(c *Config) {
			c.StorageIntegrity.Agent.Enabled = false
			c.StorageIntegrity.Agent.ReadMode = "fast"
		}, "storage_integrity.agent.read_mode"},
		{"bad lanes with SI off", func(c *Config) {
			c.StorageIntegrity.Agent.Enabled = false
			c.StorageIntegrity.Agent.Lanes = "on"
		}, "storage_integrity.agent.lanes"},
		{"zero payload limit", func(c *Config) { c.StorageIntegrity.Agent.MaxPayloadBytes = 0 }, "max_payload_bytes"},
		{"negative max_inflight_per_lane", func(c *Config) { c.StorageIntegrity.Agent.MaxInflightPerLane = -1 }, "storage_integrity.agent.max_inflight_per_lane"},
		{"missing network_state.source", func(c *Config) { c.NetworkState.Source = "" }, "network_state.source"},
		{"host-injected state allowed", func(c *Config) { c.NetworkState.Source = ""; c.StorageIntegrity.Agent.RequireNetworkState = false }, ""},
		{"disabled block ignored", func(c *Config) { c.StorageIntegrity.Agent = StorageIntegrityAgentConfig{} }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := agentSIBase()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func inlineValuesBase() *Config {
	c := agentSIBase() // agent SI on, materialize off
	c.Materialize.Enabled = true
	c.Materialize.Engine = "native"
	c.StorageIntegrity.Agent.InlineValues.Enabled = true
	return c
}

func TestStorageIntegrityInlineValuesConfig(t *testing.T) {
	iv := Default().StorageIntegrity.Agent.InlineValues
	if iv.Enabled || iv.EvaluationTimeout.Duration != 10*time.Second || iv.MaxRows != 65536 {
		t.Fatalf("defaults = %+v, want disabled with 10s / 65536", iv)
	}
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(*Config) {}, ""},
		{"materialize disabled", func(c *Config) { c.Materialize = materializeplugin.Config{} }, "requires materialize.enabled"},
		{"agent SI disabled", func(c *Config) { c.StorageIntegrity.Agent.Enabled = false }, "requires storage_integrity.agent.enabled"},
		{"sub-second timeout", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues.EvaluationTimeout = Duration{Duration: 900 * time.Millisecond}
		}, "evaluation_timeout must be at least 1s"},
		{"zero timeout", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues.EvaluationTimeout = Duration{}
		}, "evaluation_timeout must be at least 1s"},
		{"zero max_rows", func(c *Config) { c.StorageIntegrity.Agent.InlineValues.MaxRows = 0 }, "max_rows must be > 0"},
		{"disabled block ignores its own limits", func(c *Config) {
			c.StorageIntegrity.Agent.InlineValues = StorageIntegrityInlineValuesConfig{}
			c.Materialize = materializeplugin.Config{}
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := inlineValuesBase()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestAgentMaxInflightPerLaneDefault(t *testing.T) {
	if got := (StorageIntegrityAgentConfig{}).EffectiveMaxInflightPerLane(); got != 16 {
		t.Fatalf("unset max_inflight_per_lane = %d, want 16", got)
	}
	if got := (StorageIntegrityAgentConfig{MaxInflightPerLane: 4}).EffectiveMaxInflightPerLane(); got != 4 {
		t.Fatalf("explicit max_inflight_per_lane = %d, want 4", got)
	}
}
