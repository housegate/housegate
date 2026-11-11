package housegate

import (
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/chgrants"
	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
)

type fakeCreds struct{ err error }

func (f fakeCreds) GetDefaultCredential() (string, string, error)          { return "gw", "pw", f.err }
func (f fakeCreds) GetCredentialForIndexer(uint64) (string, string, error) { return "gw", "pw", f.err }

// TestBuildGrantsChecker pins when the upstream grants check
// (housegate/housegate#341) runs and which policy it applies.
func TestBuildGrantsChecker(t *testing.T) {
	base := func() *config.Config {
		c := config.Default()
		c.Upstream = "ch:9000"
		c.Rewriter.PhysicalDatabase = "phys"
		return &c
	}
	t.Run("default warn on the single upstream", func(t *testing.T) {
		c := buildGrantsChecker(base(), fakeCreds{}, false)
		if c == nil || c.Mode != chgrants.ModeWarn {
			t.Fatalf("checker = %+v, want warn mode", c)
		}
		if len(c.Upstreams) != 1 || c.Upstreams[0].Addr != "ch:9000" {
			t.Fatalf("upstreams = %+v", c.Upstreams)
		}
		if c.Policy.PhysicalDatabase != "phys" || c.Policy.StorageIntegrity || c.Policy.ReplicaCopies || c.Policy.EngineAllowed == nil {
			t.Fatalf("policy = %+v", c.Policy)
		}
		if !c.Policy.EngineAllowed("ReplicatedMergeTree") || c.Policy.EngineAllowed("URL") {
			t.Fatal("engine allowlist does not follow the rewriter's")
		}
	})
	t.Run("every shard replica", func(t *testing.T) {
		cfg := base()
		cfg.Upstream = ""
		cfg.Shard = &cluster.ShardConfig{Name: "s", Replicas: []cluster.ReplicaConfig{{Host: "a", Port: 9000}, {Host: "b", Port: 9000}}}
		cfg.UpstreamGrantsCheck = chgrants.Config{Mode: "enforce", ReplicaCopies: true}
		c := buildGrantsChecker(cfg, fakeCreds{}, false)
		if c == nil || c.Mode != chgrants.ModeEnforce || len(c.Upstreams) != 2 || !c.Policy.ReplicaCopies {
			t.Fatalf("checker = %+v", c)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		creds  fakeCreds
		nilCP  bool
		inject bool
	}{
		{name: "off", mutate: func(c *config.Config) { c.UpstreamGrantsCheck.Mode = "off" }},
		{name: "injected cluster", inject: true},
		{name: "router only", mutate: func(c *config.Config) { c.Upstream = "" }},
		{name: "no credential provider", nilCP: true},
		{name: "no default credential", creds: fakeCreds{err: errors.New("missing")}},
	} {
		t.Run("skipped: "+tc.name, func(t *testing.T) {
			cfg := base()
			if tc.mutate != nil {
				tc.mutate(cfg)
			}
			var c *chgrants.Checker
			if tc.nilCP {
				c = buildGrantsChecker(cfg, nil, tc.inject)
			} else {
				c = buildGrantsChecker(cfg, tc.creds, tc.inject)
			}
			if c != nil {
				t.Fatalf("checker = %+v, want nil", c)
			}
		})
	}
}
