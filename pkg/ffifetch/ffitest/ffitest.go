// Package ffitest gates tests that need the native rewriter FFI library.
//
// Locally such tests skip when POLYGLOT_SQL_FFI_PATH is unset. CI sets
// HOUSEGATE_REQUIRE_NATIVE_FFI=1 next to the fetched library, which turns a
// missing, unreadable or empty library into a test failure, so a green run
// proves the native engine was exercised rather than skipped.
package ffitest

import (
	"os"
	"strings"
	"testing"
)

const (
	// PathEnv names the FFI library the native engine loads.
	PathEnv = "POLYGLOT_SQL_FFI_PATH"
	// RequireEnv, set to 1, makes a missing library fail instead of skip.
	RequireEnv = "HOUSEGATE_REQUIRE_NATIVE_FFI"
)

// LibOrSkip returns the FFI library path from POLYGLOT_SQL_FFI_PATH. When
// the variable is unset it skips the test, or fails it under
// HOUSEGATE_REQUIRE_NATIVE_FFI=1. Under the requirement the path must also
// name a non-empty regular file.
func LibOrSkip(t testing.TB) string {
	t.Helper()
	path, problem := check(os.Getenv(PathEnv), os.Getenv(RequireEnv) == "1", os.Stat)
	switch {
	case problem == "":
		return path
	case path == "" && os.Getenv(RequireEnv) != "1":
		t.Skip(problem)
	default:
		t.Fatal(problem)
	}
	return ""
}

// check is LibOrSkip's decision, pure for testing: it returns the trimmed
// path and, when the test cannot use it, the reason.
func check(raw string, required bool, stat func(string) (os.FileInfo, error)) (string, string) {
	path := strings.TrimSpace(raw)
	if path == "" {
		if required {
			return "", PathEnv + " is unset but " + RequireEnv + "=1 requires the native engine FFI library"
		}
		return "", PathEnv + " not set; native engine FFI lib unavailable"
	}
	if !required {
		return path, ""
	}
	fi, err := stat(path)
	switch {
	case err != nil:
		return path, PathEnv + "=" + path + ": " + err.Error()
	case !fi.Mode().IsRegular():
		return path, PathEnv + "=" + path + " is not a regular file"
	case fi.Size() == 0:
		return path, PathEnv + "=" + path + " is empty"
	}
	return path, ""
}
