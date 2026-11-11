package config

import (
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/ffifetch"
	materializeplugin "github.com/housegate/housegate/pkg/plugins/materialize"
	"github.com/housegate/housegate/pkg/rewriter"
)

const testAutoSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"

func autoServerConfig(t *testing.T) Config {
	t.Helper()
	c := minimalServerConfig(t)
	c.Rewriter.Engine = rewriter.EngineAuto
	c.Rewriter.ServiceAddr = "localhost:50051"
	c.Rewriter.NativeLibraryRelease = ffifetch.DefaultRelease
	c.Rewriter.NativeLibrarySHA256 = testAutoSHA256
	return c
}

func TestValidateRewriterAuto(t *testing.T) {
	enabled := true
	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   string // "" = valid
	}{
		"pinned fallback is valid": {mutate: func(*Config) {}},
		"service_addr required":    {mutate: func(c *Config) { c.Rewriter.ServiceAddr = "" }, want: "requires rewriter.service_addr"},
		"native_library_path refused": {
			mutate: func(c *Config) { c.Rewriter.NativeLibraryPath = "/opt/lib.so" },
			want:   "refuses rewriter.native_library_path",
		},
		"release required": {mutate: func(c *Config) { c.Rewriter.NativeLibraryRelease = "" }, want: "requires rewriter.native_library_release and rewriter.native_library_sha256"},
		"sha256 required":  {mutate: func(c *Config) { c.Rewriter.NativeLibrarySHA256 = "" }, want: "requires rewriter.native_library_release and rewriter.native_library_sha256"},
		"release must match the binding": {
			mutate: func(c *Config) { c.Rewriter.NativeLibraryRelease = ffifetch.DefaultRelease + "-other" },
			want:   "the release this binary's native binding is built against",
		},
		"storage integrity refused": {
			mutate: func(c *Config) { c.StorageIntegrity.Enabled = &enabled },
			want:   `"auto" cannot be combined with storage_integrity.enabled`,
		},
		"negative grpc_wait refused": {
			mutate: func(c *Config) { c.Rewriter.Auto.GRPCWait = Duration{Duration: -time.Second} },
			want:   "rewriter.auto.grpc_wait must be >= 0",
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := autoServerConfig(t)
			tc.mutate(&c)
			err := c.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestDefaultRewriterAutoGRPCWait(t *testing.T) {
	if got := Default().Rewriter.Auto.GRPCWait.Duration; got != rewriter.DefaultAutoGRPCWait {
		t.Fatalf("default grpc_wait = %s, want %s", got, rewriter.DefaultAutoGRPCWait)
	}
}

func TestMaterializeRefusesAutoEngine(t *testing.T) {
	c := materializeplugin.Config{Enabled: true, Engine: rewriter.EngineAuto}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "materialize.engine") {
		t.Fatalf("Validate = %v, want materialize.engine auto refused", err)
	}
}
