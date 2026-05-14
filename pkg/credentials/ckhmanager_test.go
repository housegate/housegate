package credentials

import (
	"os"
	"path/filepath"
	"testing"
)

// sampleYAML mirrors the on-disk layout of
// configs/local.clickhouse_manager.yaml — exercises the anchor/merge-key
// path so a future yaml-library regression around `<<: [*alias]` fails
// here loudly instead of silently shipping anonymous credentials.
const sampleYAML = `pick_lb_strategy: "hash"

roles:
  admin: &admin_role
    username: admin_user
    password: admin_pw
  default_viewer: &default_role
    username: viewer
    password: viewer_pw

credential:
  sentio: { <<: [*admin_role] }
  sentio_default_viewer: { <<: [*default_role] }
  subgraph: { <<: [*admin_role] }
  subgraph_default_viewer: { <<: [*default_role] }

settings:
  max_memory_usage: 40000000000

shards:
  - index: 0
    name: 'local-0'
    addresses:
      internal_tcp_addr: localhost:9000
`

func writeTempYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "ckh.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write tmp yaml: %v", err)
	}
	return p
}

func TestLoadCkhManagerYAMLProvider_ResolvesSubgraphMergeKey(t *testing.T) {
	p := writeTempYAML(t, sampleYAML)

	cp, err := LoadCkhManagerYAMLProvider(p)
	if err != nil {
		t.Fatalf("LoadCkhManagerYAMLProvider: %v", err)
	}

	u, pw, err := cp.GetDefaultCredential()
	if err != nil {
		t.Fatalf("GetDefaultCredential: %v", err)
	}
	if u != "admin_user" || pw != "admin_pw" {
		t.Fatalf("GetDefaultCredential = (%q, %q), want (admin_user, admin_pw) — merge key likely not resolved", u, pw)
	}

	u2, pw2, err := cp.GetCredentialForIndexer(42)
	if err != nil {
		t.Fatalf("GetCredentialForIndexer: %v", err)
	}
	if u2 != u || pw2 != pw {
		t.Fatalf("GetCredentialForIndexer returned (%q, %q); legacy ckhmanager behaviour returned the same credentials as default", u2, pw2)
	}
}

func TestLoadCkhManagerYAMLProvider_DirectFields(t *testing.T) {
	// No anchor/merge keys — operators are free to inline if they want.
	const body = `credential:
  subgraph:
    username: u
    password: p
`
	p := writeTempYAML(t, body)
	cp, err := LoadCkhManagerYAMLProvider(p)
	if err != nil {
		t.Fatalf("LoadCkhManagerYAMLProvider: %v", err)
	}
	u, pw, _ := cp.GetDefaultCredential()
	if u != "u" || pw != "p" {
		t.Fatalf("got (%q, %q)", u, pw)
	}
}

func TestLoadCkhManagerYAMLProvider_MissingSubgraphErrors(t *testing.T) {
	// Empty credential.subgraph.username = misconfigured; fail loudly so
	// the proxy doesn't silently fall back to anonymous CH auth.
	const body = `credential:
  sentio:
    username: u
    password: p
`
	p := writeTempYAML(t, body)
	if _, err := LoadCkhManagerYAMLProvider(p); err == nil {
		t.Fatalf("expected error when credential.subgraph is missing, got nil")
	}
}

func TestLoadCkhManagerYAMLProvider_BadPath(t *testing.T) {
	if _, err := LoadCkhManagerYAMLProvider("/nonexistent/path/to/ckh.yaml"); err == nil {
		t.Fatalf("expected error for missing file, got nil")
	}
}
