// Package version holds build-time information injected by the linker.
package version

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// These variables are set at build time via -ldflags -X.
// When building with Bazel, x_defs in cmd/BUILD.bazel supplies them.
var (
	Version   = "(devel)"
	GitCommit = ""
	BuildTime = ""
)

// Info returns a human-readable version string.
func Info() string {
	commit := GitCommit
	if commit == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					commit = s.Value
					break
				}
			}
		}
	}

	parts := []string{
		"housegate " + Version,
	}
	if commit != "" {
		parts = append(parts, "commit "+commit)
	}
	parts = append(parts, runtime.Version())

	return strings.Join(parts, " ")
}
