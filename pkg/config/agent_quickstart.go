package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// DefaultAgentNetwork is the preset an -agent-key-only agent joins (spec
// 2026-10-09 R2); DefaultAgentListen is the agent's loopback listener, which
// also keeps clickhouse-client uncompressed by default (§3.6).
const (
	DefaultAgentNetwork = "devnet2"
	DefaultAgentListen  = "127.0.0.1:9000"
)

// AgentNetworkPresets maps -network names to their bootstrap storage RPC.
var AgentNetworkPresets = map[string]string{
	"devnet2": "http://64.38.144.158:32003",
}

// AgentQuickstart carries the agent-UX flags and env values of spec
// 2026-10-09 §6.4 (flag over env; empty = not given) and what the binary
// knows about its config file.
type AgentQuickstart struct {
	ConfigFileLoaded bool // a config file (-config / HOUSEGATE_CONFIG / ./config.json) was read
	AgentModeSet     bool // -agent or HOUSEGATE_AGENT was given, either value
	ListenSet        bool // -listen or HOUSEGATE_LISTEN was given
	Network          string
	SI               string
	SIStateDir       string
	SILanes          string
	SIReadMode       string
	SIInlineValues   string
}

// ApplyAgentQuickstart applies the agent defaults. Without a config file an
// agent key implies agent mode (unless -agent was given) and every
// quickstart default applies; with a config file only the values the
// operator passed change it (plan decision P5). Every passed value is
// checked first, so a mistyped one is refused even where nothing applies.
func ApplyAgentQuickstart(cfg *Config, q AgentQuickstart) error {
	if err := q.validate(); err != nil {
		return err
	}
	if !q.ConfigFileLoaded && !q.AgentModeSet && cfg.Agent.PrivateKeyHex != "" {
		cfg.Agent.Mode = true
	}
	if cfg.Mode() != ModeAgent {
		return nil
	}
	quick := !q.ConfigFileLoaded
	if quick || q.Network != "" {
		name := q.Network
		if name == "" {
			name = DefaultAgentNetwork
		}
		// -state, a config value and a pinned upstream all win over the
		// preset; a pinned agent needs no network state to route.
		if cfg.Agent.Upstream == "" && cfg.NetworkState.Source == "" {
			cfg.NetworkState.Source = AgentNetworkPresets[name]
		}
	}
	if quick && !q.ListenSet {
		cfg.Listen = DefaultAgentListen
	}
	si := q.SI
	if si == "" && quick {
		si = "auto"
	}
	switch si {
	case "off":
		cfg.StorageIntegrity.Agent.Enabled = false
	case "on":
		cfg.StorageIntegrity.Agent.Enabled = true
	case "auto":
		// Discovery needs an RPC network state (plan decision P6).
		cfg.StorageIntegrity.Agent.Enabled = cfg.NetworkState.IsRpcSource()
	}
	if q.SIStateDir != "" {
		cfg.StorageIntegrity.Agent.StateDir = q.SIStateDir
	}
	if q.SILanes != "" {
		cfg.StorageIntegrity.Agent.Lanes = q.SILanes
	}
	if q.SIReadMode != "" {
		cfg.StorageIntegrity.Agent.ReadMode = q.SIReadMode
	}
	switch q.SIInlineValues {
	case "on":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
	case "off":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = false
	}
	// "auto" (the quickstart default) leaves inline_values as configured.
	return nil
}

func (q AgentQuickstart) validate() error {
	var errs []error
	if q.Network != "" {
		if _, ok := AgentNetworkPresets[q.Network]; !ok {
			names := make([]string, 0, len(AgentNetworkPresets))
			for n := range AgentNetworkPresets {
				names = append(names, n)
			}
			sort.Strings(names)
			errs = append(errs, fmt.Errorf("unknown -network %q (known: %s)", q.Network, strings.Join(names, ", ")))
		}
	}
	switch q.SI {
	case "", "auto", "on", "off":
	default:
		errs = append(errs, fmt.Errorf("-si %q is invalid (want auto, on or off)", q.SI))
	}
	switch q.SILanes {
	case "", "auto", "off":
	default:
		errs = append(errs, fmt.Errorf("-si-lanes %q is invalid (want auto or off)", q.SILanes))
	}
	switch q.SIReadMode {
	case "", "safe", "unsafe_latest":
	default:
		errs = append(errs, fmt.Errorf("-si-read-mode %q is invalid (want safe or unsafe_latest)", q.SIReadMode))
	}
	switch q.SIInlineValues {
	case "", "auto", "on", "off":
	default:
		errs = append(errs, fmt.Errorf("-si-inline-values %q is invalid (want auto, on or off)", q.SIInlineValues))
	}
	return errors.Join(errs...)
}
