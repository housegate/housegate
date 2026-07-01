package materialize

import (
	"errors"
	"fmt"

	"housegate/housegate/pkg/cfgtypes"
	"housegate/housegate/pkg/rewriter"
)

// Config is the operator-tunable surface for the agent-mode materialize
// plugin. Read only in agent mode; default off.
type Config struct {
	// Enabled turns Phase-1 materialization on. Default false.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// Engine selects the backend: "grpc" dials ServiceAddr; "native" runs
	// the in-process rewriter-go engine. Required (no silent default) when
	// Enabled.
	Engine string `json:"engine" yaml:"engine"`

	// ServiceAddr is the sql-rewriter gRPC address (grpc engine).
	ServiceAddr string `json:"service_addr" yaml:"service_addr"`

	// NativeLibraryPath / NativeLibraryRelease / NativeLibrarySHA256 /
	// NativeLibraryReleaseBaseURL resolve the FFI library for the native
	// engine (same semantics as the rewriter block).
	NativeLibraryPath           string `json:"native_library_path" yaml:"native_library_path"`
	NativeLibraryRelease        string `json:"native_library_release" yaml:"native_library_release"`
	NativeLibrarySHA256         string `json:"native_library_sha256" yaml:"native_library_sha256"`
	NativeLibraryReleaseBaseURL string `json:"native_library_release_base_url" yaml:"native_library_release_base_url"`

	// Timeout caps each materialize call (grpc engine).
	Timeout cfgtypes.Duration `json:"timeout" yaml:"timeout"`

	// RandomPoolSize is how many random/uuid values are supplied per call.
	// <= 0 falls back to 16 at build time.
	RandomPoolSize int `json:"random_pool_size" yaml:"random_pool_size"`

	// ProfileID selects the materialization profile ("" → engine default).
	ProfileID string `json:"profile_id" yaml:"profile_id"`
}

// Validate is a no-op when disabled. When enabled it requires an explicit
// engine and (for grpc) a service address.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	switch c.Engine {
	case rewriter.EngineGRPC:
		if c.ServiceAddr == "" {
			errs = append(errs, errors.New("materialize.service_addr is required when materialize.engine is \"grpc\""))
		}
	case rewriter.EngineNative:
		// ok
	case "":
		errs = append(errs, errors.New("materialize.engine is required when materialize.enabled (\"grpc\" or \"native\")"))
	default:
		errs = append(errs, fmt.Errorf("materialize.engine %q is invalid (want %q or %q)",
			c.Engine, rewriter.EngineGRPC, rewriter.EngineNative))
	}
	if c.RandomPoolSize < 0 {
		errs = append(errs, fmt.Errorf("materialize.random_pool_size must be >= 0, got %d", c.RandomPoolSize))
	}
	return errors.Join(errs...)
}
