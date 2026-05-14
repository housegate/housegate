package network

import (
	"path/filepath"
	"testing"
)

// TestLoadExampleConfig is a structural smoke test: the example
// network-state config in configs/local.network_state.yaml must load
// cleanly and produce non-empty backing maps. It deliberately does
// not assert specific keys (`analytics_db`, `coinbase`, etc.) — that
// fixture file is treated as live operator-tunable example, and
// hard-coding its contents here would make every YAML tweak a
// test-edit cascade.
func TestLoadExampleConfig(t *testing.T) {
	path, err := filepath.Abs("../../configs/local.network_state.yaml")
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	s, err := LoadNetworkStateFromYAML(path)
	if err != nil {
		t.Fatalf("LoadNetworkStateFromYAML(%s): %v", path, err)
	}

	// Each section must have at least one entry — otherwise the
	// example is no longer demonstrating the section's purpose.
	if len(s.IndexerInfos) == 0 {
		t.Error("indexer_infos: expected at least one entry")
	}
	if len(s.ProcessorAllocations) == 0 {
		t.Error("processor_allocations: expected at least one entry")
	}
	if len(s.DatabaseInfos) == 0 {
		t.Error("database_infos: expected at least one entry")
	}
	if len(s.DatabasePermissions) == 0 {
		t.Error("database_permissions: expected at least one entry")
	}

	// All must echo what was loaded.
	all := s.All()
	if len(all) != len(s.DatabaseInfos) {
		t.Errorf("All count=%d, want %d", len(all), len(s.DatabaseInfos))
	}
}
