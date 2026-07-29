package network

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
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
	if len(s.TableSchemas) == 0 {
		t.Error("table_schemas: expected at least one entry")
	}
	for key, declared := range s.TableSchemas {
		var schema payloadexec.TableSchema
		if err := json.Unmarshal([]byte(declared.SchemaJson), &schema); err != nil {
			t.Fatalf("table_schemas[%q] schema_json: %v", key, err)
		}
		if got := payloadexec.TableSchemaHash("local-devnet", schema); got != declared.SchemaHash {
			t.Fatalf("table_schemas[%q] schema_hash=%q, want %q for local-devnet", key, declared.SchemaHash, got)
		}
	}

	// All must echo what was loaded.
	all := s.All()
	if len(all) != len(s.DatabaseInfos) {
		t.Errorf("All count=%d, want %d", len(all), len(s.DatabaseInfos))
	}
}

func TestInMemoryTableSchemas(t *testing.T) {
	s := NewInMemoryNetworkState()
	for version := uint32(1); version <= 3; version++ {
		key := fmt.Sprintf("db_1/t@%d", version)
		s.TableSchemas[key] = TableSchemaInfo{
			DatabaseId: "db_1",
			TableId:    "t",
			Version:    version,
			SchemaHash: "0xhash",
			SchemaJson: `{}`,
		}
	}

	got, ok := s.TableSchema("db_1", "t", 2)
	if !ok || got.Version != 2 || got.DatabaseId != "db_1" || got.TableId != "t" {
		t.Fatalf("TableSchema hit = %+v, %v", got, ok)
	}
	if _, ok := s.TableSchema("db_1", "t", 4); ok {
		t.Fatal("TableSchema miss returned ok")
	}
	latest, ok := s.LatestTableSchema("db_1", "t")
	if !ok || latest.Version != 3 {
		t.Fatalf("LatestTableSchema = %+v, %v; want version 3", latest, ok)
	}
}
