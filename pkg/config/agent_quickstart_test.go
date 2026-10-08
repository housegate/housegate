package config

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/log"
)

func quickstartBase() *Config {
	c := Default()
	c.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	return &c
}

func TestApplyAgentQuickstart_KeyOnly(t *testing.T) {
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if !cfg.Agent.Mode || cfg.NetworkState.Source != "http://64.38.144.158:32003" || cfg.Listen != "127.0.0.1:9000" {
		t.Fatalf("mode=%v source=%q listen=%q", cfg.Agent.Mode, cfg.NetworkState.Source, cfg.Listen)
	}
	if !cfg.StorageIntegrity.Agent.Enabled || cfg.StorageIntegrity.Agent.StateDir != "" || cfg.StorageIntegrity.Agent.NetworkID != "" {
		t.Fatalf("SI agent = %+v; want enabled with a discovered network and the default state dir", cfg.StorageIntegrity.Agent)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("an -agent-key-only config must validate: %v", err)
	}
}

func TestApplyAgentQuickstart_ExplicitAgentFalseWins(t *testing.T) {
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{AgentModeSet: true}); err != nil || cfg.Agent.Mode {
		t.Fatalf("-agent=false must keep server mode: mode=%v err=%v", cfg.Agent.Mode, err)
	}
	if cfg.Listen != Default().Listen || cfg.NetworkState.Source != "" || cfg.StorageIntegrity.Agent.Enabled {
		t.Fatalf("a server-mode config must not receive agent defaults: %+v", cfg)
	}
}

func TestApplyAgentQuickstart_ExplicitListenWins(t *testing.T) {
	cfg := quickstartBase()
	cfg.Listen = ":19000"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ListenSet: true}); err != nil || cfg.Listen != ":19000" {
		t.Fatalf("an explicit -listen must win over the agent default: %q %v", cfg.Listen, err)
	}
}

func TestApplyAgentQuickstart_ConfigFileKeepsItsValues(t *testing.T) {
	cfg := quickstartBase()
	cfg.Agent.Mode = true
	cfg.Agent.Upstream = "10.0.0.8:9001"
	cfg.Listen = ":9001"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ConfigFileLoaded: true, SILanes: "off"}); err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":9001" || cfg.NetworkState.Source != "" || cfg.StorageIntegrity.Agent.Enabled {
		t.Fatalf("a config file must not receive quickstart defaults: %+v", cfg)
	}
	if cfg.StorageIntegrity.Agent.Lanes != "off" {
		t.Fatalf("an explicitly set HOUSEGATE_SI_LANES must still apply: %q", cfg.StorageIntegrity.Agent.Lanes)
	}
}

// With a config file, an agent key does not imply agent mode: the file
// decides (plan decision P5).
func TestApplyAgentQuickstart_ConfigFileDecidesTheMode(t *testing.T) {
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ConfigFileLoaded: true}); err != nil || cfg.Agent.Mode {
		t.Fatalf("a config file without agent.mode must stay in server mode: mode=%v err=%v", cfg.Agent.Mode, err)
	}
}

func TestApplyAgentQuickstart_SourceAndUpstreamOverridesWin(t *testing.T) {
	cfg := quickstartBase()
	cfg.NetworkState.Source = "http://node:10003"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{}); err != nil || cfg.NetworkState.Source != "http://node:10003" {
		t.Fatalf("-state must win over the preset: %q %v", cfg.NetworkState.Source, err)
	}
	pinned := quickstartBase()
	pinned.Agent.Upstream = "10.0.0.8:9001"
	if err := ApplyAgentQuickstart(pinned, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if pinned.NetworkState.Source != "" || pinned.StorageIntegrity.Agent.Enabled {
		t.Fatalf("a pinned upstream needs no preset and -si auto cannot discover: %+v", pinned.StorageIntegrity.Agent)
	}
}

// -si auto enables the SI plugin only on an RPC network state, where the
// agent can discover the network id (plan decision P6); on enables it on any
// source.
func TestApplyAgentQuickstart_SIAutoNeedsAnRPCSource(t *testing.T) {
	yaml := quickstartBase()
	yaml.NetworkState.Source = "network_state.yaml"
	if err := ApplyAgentQuickstart(yaml, AgentQuickstart{}); err != nil || yaml.StorageIntegrity.Agent.Enabled {
		t.Fatalf("-si auto on a YAML source: enabled=%v err=%v", yaml.StorageIntegrity.Agent.Enabled, err)
	}
	on := quickstartBase()
	on.NetworkState.Source = "network_state.yaml"
	if err := ApplyAgentQuickstart(on, AgentQuickstart{SI: "on"}); err != nil || !on.StorageIntegrity.Agent.Enabled {
		t.Fatalf("-si on on a YAML source: enabled=%v err=%v", on.StorageIntegrity.Agent.Enabled, err)
	}
	fileAuto := quickstartBase()
	fileAuto.Agent.Mode = true
	fileAuto.NetworkState.Source = "http://node:10003"
	if err := ApplyAgentQuickstart(fileAuto, AgentQuickstart{ConfigFileLoaded: true, SI: "auto"}); err != nil || !fileAuto.StorageIntegrity.Agent.Enabled {
		t.Fatalf("an explicit -si auto with a config file and an RPC source: enabled=%v err=%v", fileAuto.StorageIntegrity.Agent.Enabled, err)
	}
}

func TestApplyAgentQuickstart_Values(t *testing.T) {
	for name, tc := range map[string]struct {
		q       AgentQuickstart
		wantErr string
		check   func(*Config) bool
	}{
		"si off":          {q: AgentQuickstart{SI: "off"}, check: func(c *Config) bool { return !c.StorageIntegrity.Agent.Enabled }},
		"si on":           {q: AgentQuickstart{SI: "on"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.Enabled }},
		"network devnet2": {q: AgentQuickstart{Network: "devnet2"}, check: func(c *Config) bool { return c.NetworkState.Source == AgentNetworkPresets["devnet2"] }},
		"state dir":       {q: AgentQuickstart{SIStateDir: "/var/lib/hg"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.StateDir == "/var/lib/hg" }},
		"lanes auto":      {q: AgentQuickstart{SILanes: "auto"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.Lanes == "auto" }},
		"read mode":       {q: AgentQuickstart{SIReadMode: "safe"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.ReadMode == "safe" }},
		"read mode latest": {q: AgentQuickstart{SIReadMode: "unsafe_latest"}, check: func(c *Config) bool {
			return c.StorageIntegrity.Agent.ReadMode == "unsafe_latest"
		}},
		"inline off":      {q: AgentQuickstart{SIInlineValues: "off"}, check: func(c *Config) bool { return !c.StorageIntegrity.Agent.InlineValues.Enabled }},
		"unknown network": {q: AgentQuickstart{Network: "mainnet-9"}, wantErr: `unknown -network "mainnet-9"`},
		"bad si":          {q: AgentQuickstart{SI: "yes"}, wantErr: "-si"},
		"bad lanes":       {q: AgentQuickstart{SILanes: "on"}, wantErr: "-si-lanes"},
		"bad read mode":   {q: AgentQuickstart{SIReadMode: "fast"}, wantErr: "-si-read-mode"},
		"bad inline":      {q: AgentQuickstart{SIInlineValues: "maybe"}, wantErr: "-si-inline-values"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := quickstartBase()
			err := ApplyAgentQuickstart(cfg, tc.q)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || !tc.check(cfg) {
				t.Fatalf("err=%v cfg=%+v", err, cfg.StorageIntegrity.Agent)
			}
		})
	}
}

// An invalid value is refused even where the quickstart applies nothing (a
// server-mode config), so a mistyped flag never passes silently (F20).
func TestApplyAgentQuickstart_ValuesAreCheckedInServerMode(t *testing.T) {
	cfg := Default()
	if err := ApplyAgentQuickstart(&cfg, AgentQuickstart{SIReadMode: "fast"}); err == nil || !strings.Contains(err.Error(), "-si-read-mode") {
		t.Fatalf("err = %v, want the -si-read-mode refusal", err)
	}
}

func TestDefaultAgentStateBase(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	for _, tc := range []struct {
		goos, xdg, home, want string
		ok                    bool
	}{
		{"linux", "/xdg", "/home/u", "/xdg/housegate", true},
		{"linux", "", "/home/u", "/home/u/.local/state/housegate", true},
		{"linux", "relative", "/home/u", "/home/u/.local/state/housegate", true},
		{"darwin", "", "/Users/u", "/Users/u/Library/Application Support/housegate", true},
		{"darwin", "/xdg", "/Users/u", "/Users/u/Library/Application Support/housegate", true},
		{"windows", "", `C:\Users\u`, "", false},
		{"linux", "", "", "", false},
		{"darwin", "", "", "", false},
	} {
		got, ok := DefaultAgentStateBase(tc.goos, env(map[string]string{"XDG_STATE_HOME": tc.xdg}), tc.home)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s xdg=%q home=%q = %q,%v; want %q,%v", tc.goos, tc.xdg, tc.home, got, ok, tc.want, tc.ok)
		}
	}
	if got := AgentSIStateDir("/b", "devnet2", "0xAB"); got != filepath.Join("/b", "si", "devnet2", "0xab") {
		t.Fatalf("AgentSIStateDir = %q", got)
	}
}

func TestApplyAgentQuickstart_InlineValuesAuto(t *testing.T) {
	supported := quickstartBase()
	if err := ApplyAgentQuickstart(supported, AgentQuickstart{GOOS: "linux", GOARCH: "amd64"}); err != nil {
		t.Fatal(err)
	}
	m := supported.Materialize
	if !supported.StorageIntegrity.Agent.InlineValues.Enabled || !m.Enabled || m.Engine != "native" || m.NativeLibraryRelease != "v0.17.0" || !m.Optional || !m.Implicit {
		t.Fatalf("inline=%v materialize=%+v; want the optional native default", supported.StorageIntegrity.Agent.InlineValues.Enabled, m)
	}
	if err := supported.Validate(); err != nil {
		t.Fatalf("auto inline values must validate: %v", err)
	}

	unsupported := quickstartBase()
	if err := ApplyAgentQuickstart(unsupported, AgentQuickstart{GOOS: "linux", GOARCH: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if unsupported.StorageIntegrity.Agent.InlineValues.Enabled || unsupported.Materialize.Enabled {
		t.Fatal("without a prebuilt engine inline values stay off")
	}

	explicit := quickstartBase()
	explicit.Materialize.Enabled = true
	explicit.Materialize.Engine = "grpc"
	explicit.Materialize.ServiceAddr = "127.0.0.1:50051"
	if err := ApplyAgentQuickstart(explicit, AgentQuickstart{GOOS: "linux", GOARCH: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if explicit.Materialize.Engine != "grpc" || explicit.Materialize.Optional || explicit.Materialize.Implicit {
		t.Fatalf("an explicit materializer must be kept and stay fail-fast: %+v", explicit.Materialize)
	}

	forced := quickstartBase()
	if err := ApplyAgentQuickstart(forced, AgentQuickstart{GOOS: "linux", GOARCH: "arm64", SIInlineValues: "on"}); err != nil {
		t.Fatal(err)
	}
	if !forced.StorageIntegrity.Agent.InlineValues.Enabled || !forced.Materialize.Enabled || forced.Materialize.Optional || !forced.Materialize.Implicit {
		t.Fatalf("-si-inline-values on must enable a fail-fast materializer: %+v", forced.Materialize)
	}
}

// captureQuickstartLogs swaps the package-default logger for one writing
// text records into the returned buffer, restoring it on cleanup.
func captureQuickstartLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Default()
	log.SetDefault(log.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { log.SetDefault(prev) })
	return &buf
}

// -si auto turns signing on only with an RPC network state. When it resolves
// to off, one warning says so and names the way to sign anyway (final review
// M6): an unsigned INSERT into an Active table is refused by the server as a
// retryable stale-state error the user would otherwise retry forever.
func TestApplyAgentQuickstart_SIAutoOffWarns(t *testing.T) {
	logs := captureQuickstartLogs(t)
	pinned := quickstartBase()
	pinned.Agent.Upstream = "10.0.0.8:9001"
	if err := ApplyAgentQuickstart(pinned, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if pinned.StorageIntegrity.Agent.Enabled {
		t.Fatal("-si auto without an RPC network state must stay off")
	}
	out := logs.String()
	if strings.Count(out, "level=WARN") != 1 || !strings.Contains(out, "storage-integrity signing is off") || !strings.Contains(out, "-si on") || !strings.Contains(out, "network_id") {
		t.Fatalf("want one warning naming why SI is off and the -si on + network_id escape, got:\n%s", out)
	}

	logs.Reset()
	rpc := quickstartBase()
	if err := ApplyAgentQuickstart(rpc, AgentQuickstart{}); err != nil || !rpc.StorageIntegrity.Agent.Enabled {
		t.Fatalf("an RPC network state enables SI: enabled=%v err=%v", rpc.StorageIntegrity.Agent.Enabled, err)
	}
	off := quickstartBase()
	off.Agent.Upstream = "10.0.0.8:9001"
	if err := ApplyAgentQuickstart(off, AgentQuickstart{SI: "off"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "storage-integrity signing is off") {
		t.Fatalf("no warning when SI is on, or off on purpose:\n%s", logs.String())
	}
}

// An explicitly given -network (or HOUSEGATE_NETWORK) that a configured
// network_state.source or agent.upstream overrides is reported, not silently
// dropped (final review M7).
func TestApplyAgentQuickstart_OverriddenNetworkWarns(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*Config)
		q      AgentQuickstart
		want   string
	}{
		"config source":   {func(c *Config) { c.Agent.Mode = true; c.NetworkState.Source = "http://node:10003" }, AgentQuickstart{ConfigFileLoaded: true, Network: "devnet2"}, "network_state.source"},
		"config upstream": {func(c *Config) { c.Agent.Mode = true; c.Agent.Upstream = "10.0.0.8:9001" }, AgentQuickstart{ConfigFileLoaded: true, Network: "devnet2"}, "agent.upstream"},
		"flag upstream":   {func(c *Config) { c.Agent.Upstream = "10.0.0.8:9001" }, AgentQuickstart{Network: "devnet2", SI: "off"}, "agent.upstream"},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureQuickstartLogs(t)
			cfg := quickstartBase()
			tc.mutate(cfg)
			if err := ApplyAgentQuickstart(cfg, tc.q); err != nil {
				t.Fatal(err)
			}
			out := logs.String()
			if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "-network devnet2 is ignored") || !strings.Contains(out, tc.want) {
				t.Fatalf("want a warning that -network lost to %s, got:\n%s", tc.want, out)
			}
		})
	}

	logs := captureQuickstartLogs(t)
	cfg := quickstartBase()
	cfg.Agent.Mode = true
	cfg.NetworkState.Source = "http://node:10003"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ConfigFileLoaded: true}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "is ignored") {
		t.Fatalf("no -network given, no warning:\n%s", logs.String())
	}
}

// A loaded config file disables the quickstart; the info log names that file,
// so a stray ./config.json is visible (final review M8).
func TestApplyAgentQuickstart_LogsTheConfigFileThatDisablesIt(t *testing.T) {
	logs := captureQuickstartLogs(t)
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ConfigFileLoaded: true, ConfigFile: "config.json"}); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "agent quickstart defaults are off") || !strings.Contains(out, "config_file=config.json") {
		t.Fatalf("want an info line naming config.json, got:\n%s", out)
	}

	logs.Reset()
	if err := ApplyAgentQuickstart(quickstartBase(), AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "agent quickstart defaults are off") {
		t.Fatalf("without a config file the quickstart applies:\n%s", logs.String())
	}
}
