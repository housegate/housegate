package config

import (
	"path/filepath"
	"strings"
)

// DefaultAgentStateBase is the per-OS base of the agent's state (spec
// 2026-10-09 §6.4): Linux $XDG_STATE_HOME/housegate (an absolute
// XDG_STATE_HOME only, as the XDG spec requires), else
// ~/.local/state/housegate; macOS ~/Library/Application Support/housegate.
// Other platforms have none: they need an explicit
// storage_integrity.agent.state_dir, and the client_seq lock refuses them
// anyway.
func DefaultAgentStateBase(goos string, getenv func(string) string, home string) (string, bool) {
	switch goos {
	case "linux":
		if xdg := getenv("XDG_STATE_HOME"); xdg != "" && filepath.IsAbs(xdg) {
			return filepath.Join(xdg, "housegate"), true
		}
		if home == "" {
			return "", false
		}
		return filepath.Join(home, ".local", "state", "housegate"), true
	case "darwin":
		if home == "" {
			return "", false
		}
		return filepath.Join(home, "Library", "Application Support", "housegate"), true
	default:
		return "", false
	}
}

// AgentSIStateDir is the SI subtree <base>/si/<network_id>/<signer> that
// holds the legacy client_seq counter when no state_dir is configured (plan
// decision P4). The signer is lowercased, like the counter's file name.
func AgentSIStateDir(base, networkID, signer string) string {
	return filepath.Join(base, "si", networkID, strings.ToLower(signer))
}
