package rewrite

import "housegate/housegate/pkg/cfgtypes"

// Config is the operator-tunable surface for the rewrite plugin. The
// constructor in cmd combines these fields with cross-cutting
// values (Listen, Upstream) to produce a rewriter.Options.
type Config struct {
	// ServiceAddr is the gRPC address of the sql-rewriter service.
	ServiceAddr string `json:"service_addr" yaml:"service_addr"`

	// Timeout caps each gRPC call.
	Timeout cfgtypes.Duration `json:"timeout" yaml:"timeout"`

	// PhysicalDatabase is the single physical ClickHouse database that
	// hosts every logical database in this deployment. Used as both
	// the rewriter's database_map target value and the wire-level
	// substitution for hello.Database. Empty disables both.
	PhysicalDatabase string `json:"physical_database" yaml:"physical_database"`

	// Delimiter is the character used to separate logical database names
	// in the rewriter's output. Defaults to ".".
	Delimiter string `json:"delimiter" yaml:"delimiter"`

	// Engine selects the rewriter implementation: "grpc" (default, also
	// the empty value) calls the external sql-rewriter service at
	// ServiceAddr; "native" runs the in-process rewriter-go engine and
	// ignores ServiceAddr.
	Engine string `json:"engine" yaml:"engine"`

	// NativeLibraryPath is the path to libpolyglot_sql_ffi.{so,dylib}
	// for the native engine. Empty falls back to the
	// POLYGLOT_SQL_FFI_PATH env var, then standard install locations.
	NativeLibraryPath string `json:"native_library_path" yaml:"native_library_path"`

	// NativeLibraryRelease names a rewriter-go release tag (e.g. "v0.2.0")
	// to fetch the FFI library from when the native engine is selected and
	// NativeLibraryPath is empty. The library is cached under the user
	// cache dir and downloaded only on miss; fetch failure disables
	// rewriting (fail-open), like any other backend-unavailable case.
	NativeLibraryRelease string `json:"native_library_release" yaml:"native_library_release"`

	// NativeLibrarySHA256 optionally pins the library's sha256 (64 hex
	// chars). Verified against cached copies too; mismatch re-downloads
	// once, then fails. Without a pin, the release's SHA256SUMS asset is
	// used when present (TLS-only warning otherwise).
	NativeLibrarySHA256 string `json:"native_library_sha256" yaml:"native_library_sha256"`

	// NativeLibraryReleaseBaseURL overrides the download root (default:
	// rewriter-go's GitHub releases) for mirrors / internal artifact
	// servers. URL shape: <base>/<tag>/<asset>.
	NativeLibraryReleaseBaseURL string `json:"native_library_release_base_url" yaml:"native_library_release_base_url"`
}
