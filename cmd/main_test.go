package main

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/config"
)

func TestValidateStandaloneRuntimeConfigRejectsStorageIntegrityIngress(t *testing.T) {
	cfg := config.Default()
	cfg.StorageIntegrity.Ingress.Enabled = true

	err := validateStandaloneRuntimeConfig(&cfg)
	if err == nil {
		t.Fatal("validateStandaloneRuntimeConfig returned nil, want storage-integrity ingress rejection")
	}
	if !strings.Contains(err.Error(), "standalone") {
		t.Fatalf("error = %q, want standalone context", err)
	}
	if !strings.Contains(err.Error(), "StorageIntegrityAdmissionConsumer") {
		t.Fatalf("error = %q, want consumer requirement", err)
	}
}

func TestValidateStandaloneRuntimeConfigAllowsDisabledStorageIntegrityIngress(t *testing.T) {
	cfg := config.Default()
	cfg.StorageIntegrity.Ingress.Enabled = false

	if err := validateStandaloneRuntimeConfig(&cfg); err != nil {
		t.Fatalf("validateStandaloneRuntimeConfig returned %v, want nil", err)
	}
}

// The agent-UX values resolve flag over env; an env var counts only when
// set, and a config file read by config.LoadFile is passed through (spec
// 2026-10-09 §6.4, plan decision P5).
func TestAgentQuickstartInputs(t *testing.T) {
	env := map[string]string{
		"HOUSEGATE_NETWORK":          "devnet2",
		"HOUSEGATE_SI":               "on",
		"HOUSEGATE_SI_STATE_DIR":     "/env/state",
		"HOUSEGATE_SI_LANES":         "off",
		"HOUSEGATE_SI_READ_MODE":     "safe",
		"HOUSEGATE_SI_INLINE_VALUES": "off",
	}
	getenv := func(k string) string { return env[k] }
	flags := agentQuickstartFlags{
		network: "flag-net", si: "auto", siStateDir: "/flag/state",
		siLanes: "auto", siReadMode: "unsafe_latest", siInlineValues: "on",
	}

	fromEnv := agentQuickstartInputs(true, map[string]bool{}, flags, getenv)
	want := config.AgentQuickstart{
		ConfigFileLoaded: true,
		Network:          "devnet2", SI: "on", SIStateDir: "/env/state",
		SILanes: "off", SIReadMode: "safe", SIInlineValues: "off",
	}
	if fromEnv != want {
		t.Fatalf("env only:\n got %+v\nwant %+v", fromEnv, want)
	}

	explicit := map[string]bool{
		"network": true, "si": true, "si-state-dir": true,
		"si-lanes": true, "si-read-mode": true, "si-inline-values": true,
		"agent": true, "listen": true,
	}
	fromFlags := agentQuickstartInputs(false, explicit, flags, getenv)
	want = config.AgentQuickstart{
		AgentModeSet: true, ListenSet: true,
		Network: "flag-net", SI: "auto", SIStateDir: "/flag/state",
		SILanes: "auto", SIReadMode: "unsafe_latest", SIInlineValues: "on",
	}
	if fromFlags != want {
		t.Fatalf("flags win:\n got %+v\nwant %+v", fromFlags, want)
	}

	none := agentQuickstartInputs(false, map[string]bool{}, flags, func(string) string { return "" })
	if none != (config.AgentQuickstart{}) {
		t.Fatalf("nothing given: %+v", none)
	}

	env = map[string]string{"HOUSEGATE_AGENT": "false", "HOUSEGATE_LISTEN": ":9100"}
	modeEnv := agentQuickstartInputs(false, map[string]bool{}, flags, getenv)
	if !modeEnv.AgentModeSet || !modeEnv.ListenSet {
		t.Fatalf("HOUSEGATE_AGENT / HOUSEGATE_LISTEN must count as given: %+v", modeEnv)
	}
}
