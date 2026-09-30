package config

import (
	"path/filepath"
	"testing"

	"github.com/housegate/housegate/pkg/rewriter"
)

// TestSampleServerConfigsNameARewriter: a server that rewrites refuses to
// start when its rewriter cannot be built, unless
// rewriter.fail_open_on_unavailable is set (spec 2026-09-26 T8). Every
// server sample under configs/ must therefore name a rewriter backend: the
// native engine, or the gRPC engine with a service address.
func TestSampleServerConfigsNameARewriter(t *testing.T) {
	for _, name := range []string{"local.server.json", "local.server.yaml", "local.server-mock-remote.yaml"} {
		t.Run(name, func(t *testing.T) {
			path, err := filepath.Abs(filepath.Join("..", "..", "configs", name))
			if err != nil {
				t.Fatal(err)
			}
			cfg := Load(path)
			if cfg.Mode() != ModeServer {
				t.Fatalf("mode = %s, want server", cfg.Mode())
			}
			if cfg.Shard == nil && cfg.Upstream == "" {
				return // router-only: no rewriter by design
			}
			if cfg.Rewriter.FailOpenOnUnavailable {
				t.Fatal("a sample must not opt into fail_open_on_unavailable")
			}
			switch cfg.Rewriter.Engine {
			case rewriter.EngineNative:
			case "", rewriter.EngineGRPC:
				if cfg.Rewriter.ServiceAddr == "" {
					t.Fatal("grpc engine with an empty rewriter.service_addr cannot start")
				}
			default:
				t.Fatalf("unknown engine %q", cfg.Rewriter.Engine)
			}
		})
	}
}
