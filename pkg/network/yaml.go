package network

import (
	"fmt"
	"os"
	"strings"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"

	"go.yaml.in/yaml/v3"
)

// networkStateYAML is the on-disk shape LoadNetworkStateFromYAML accepts.
type networkStateYAML struct {
	IndexerInfos         map[uint64]IndexerInfo                 `yaml:"indexer_infos"`
	ProcessorAllocations map[string][]ProcessorAllocation       `yaml:"processor_allocations"`
	ProcessorInfos       map[string]ProcessorInfo               `yaml:"processor_infos"`
	DatabaseInfos        map[Database]DatabaseInfo              `yaml:"database_infos"`
	DatabasePermissions  map[AccountAddress]databasePermYAMLMap `yaml:"database_permissions"`
}

// databasePermYAMLMap is map[Database]dbAuthYAML — one indirection so
// the inner permission can be either a YAML list (["read", "write"])
// or an integer bitmap (matches the upstream wire encoding).
type databasePermYAMLMap map[Database]dbAuthYAML

// dbAuthYAML accepts permissions in three forms:
//   - YAML list of named flags:    ["read", "write", "admin"]
//   - YAML scalar string flag:     "admin"
//   - YAML integer bitmap:         3   (= read|write, upstream wire format)
//
// All three resolve to the same registry.DbAuth bitmap. Unknown flag
// names error out at load time so a typo ("rread") doesn't silently
// degrade to no permission.
type dbAuthYAML registry.DbAuth

func (a *dbAuthYAML) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		// Try integer first; fall back to a single named flag.
		var n int64
		if err := node.Decode(&n); err == nil {
			*a = dbAuthYAML(n)
			return nil
		}
		var s string
		if err := node.Decode(&s); err != nil {
			return fmt.Errorf("permission must be int, string, or list of strings: %w", err)
		}
		auth, err := parseAuthFlag(s)
		if err != nil {
			return err
		}
		*a = dbAuthYAML(auth)
		return nil
	case yaml.SequenceNode:
		var flags []string
		if err := node.Decode(&flags); err != nil {
			return fmt.Errorf("permission list: %w", err)
		}
		var combined registry.DbAuth
		for _, f := range flags {
			auth, err := parseAuthFlag(f)
			if err != nil {
				return err
			}
			combined |= auth
		}
		*a = dbAuthYAML(combined)
		return nil
	default:
		return fmt.Errorf("permission must be int, string, or list of strings (got yaml kind %d)", node.Kind)
	}
}

// parseAuthFlag maps a human-readable permission name to its bit. Names
// are lower-cased for ergonomics ("Read", "READ", and "read" all work).
func parseAuthFlag(name string) (registry.DbAuth, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "read":
		return registry.DbAuthRead, nil
	case "write":
		return registry.DbAuthWrite, nil
	case "admin":
		return registry.DbAuthAdmin, nil
	default:
		return 0, fmt.Errorf("unknown permission flag %q (want read|write|admin or an integer bitmap)", name)
	}
}

// LoadNetworkStateFromYAML loads an InMemoryNetworkState from a YAML
// file. Used for local development and integration tests that do not
// want to stand up a Redis statemirror.
func LoadNetworkStateFromYAML(path string) (*InMemoryNetworkState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read network state file %s: %w", path, err)
	}

	var raw networkStateYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse network state YAML %s: %w", path, err)
	}

	s := NewInMemoryNetworkState()
	for id, info := range raw.IndexerInfos {
		s.IndexerInfos[id] = info
	}
	for pid, allocs := range raw.ProcessorAllocations {
		s.ProcessorAllocations[pid] = allocs
	}
	for pid, info := range raw.ProcessorInfos {
		s.ProcessorInfos[pid] = info
	}
	for db, info := range raw.DatabaseInfos {
		s.DatabaseInfos[db] = info
	}
	for account, perms := range raw.DatabasePermissions {
		cp := make(DatabasePermissions, len(perms))
		for db, auth := range perms {
			cp[db] = registry.DbAuth(auth)
		}
		s.DatabasePermissions[account] = cp
	}

	log.Infow("loaded network state",
		"path", path,
		"indexers", len(s.IndexerInfos),
		"processor_allocations", len(s.ProcessorAllocations),
		"processor_infos", len(s.ProcessorInfos),
		"databases", len(s.DatabaseInfos),
		"database_permissions", len(s.DatabasePermissions))

	return s, nil
}
