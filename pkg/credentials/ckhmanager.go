package credentials

// ckhmanager.go — CredentialProvider that parses the sentio-core
// ckhmanager YAML config format directly. housegate only consumes the
// `credential.subgraph` block (username + password), so we drop the
// transitive sentio-core dependency and own the parse here.
//
// The full ckhmanager schema (roles, settings, shards, addresses,
// tiers, ...) is ignored on purpose — both legacy GetConnInfo paths
// (default shard and per-indexer NewShardByStateIndexer) resolved to
// the same credentials["subgraph"] entry and discarded everything else.
// See git history for the previous ckhmanager.Manager-backed impl.

import (
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"

	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/secretsload"
)

// ckhManagerYAML captures the only fields housegate consumes from a
// sentio-core ckhmanager config file. Unknown fields (roles, shards,
// settings, ...) are silently ignored by yaml.Unmarshal, so the same
// config files keep working unchanged.
//
// The `<<: [*alias]` merge-key syntax used in production configs is
// resolved by go.yaml.in/yaml/v3 before our struct sees the value, so
// `credential.subgraph` ends up populated with the username/password
// merged from the referenced role anchor.
type ckhManagerYAML struct {
	Credential struct {
		Subgraph struct {
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		} `yaml:"subgraph"`
	} `yaml:"credential"`
}

// staticCredentialProvider returns the same (username, password) pair
// for every call. Matches the legacy ckhmanager behaviour: the manager
// reused one global credential map across default- and per-indexer
// shards, and our GetConnInfo calls discarded the per-shard address.
type staticCredentialProvider struct {
	username string
	password string
}

// LoadCkhManagerYAMLProvider parses the sentio-core ckhmanager config
// at `path` and returns a CredentialProvider seeded with
// `credential.subgraph.{username, password}`. age-encrypted files
// (`.age` suffix) are decrypted transparently via pkg/secretsload
// using HOUSEGATE_AGE_IDENTITY[_FILE].
//
// Returns an error when the path is unreadable, the YAML is malformed,
// or the `credential.subgraph.username` field is empty (treated as
// misconfigured rather than silently returning anonymous creds).
func LoadCkhManagerYAMLProvider(path string) (CredentialProvider, error) {
	resolved, err := secretsload.Resolve(path)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", path, err)
	}
	defer resolved.Cleanup()

	data, err := os.ReadFile(resolved.Path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var doc ckhManagerYAML
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if doc.Credential.Subgraph.Username == "" {
		return nil, fmt.Errorf("%s: missing credential.subgraph.username", path)
	}

	log.Infow("loaded ckh manager credentials",
		"path", path,
		"user", doc.Credential.Subgraph.Username,
	)
	return &staticCredentialProvider{
		username: doc.Credential.Subgraph.Username,
		password: doc.Credential.Subgraph.Password,
	}, nil
}

func (p *staticCredentialProvider) GetDefaultCredential() (string, string, error) {
	return p.username, p.password, nil
}

func (p *staticCredentialProvider) GetCredentialForIndexer(_ network.IndexerInfo) (string, string, error) {
	return p.username, p.password, nil
}
