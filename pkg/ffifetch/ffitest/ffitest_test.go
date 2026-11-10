package ffitest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	dir := t.TempDir()
	lib := filepath.Join(dir, "lib.so")
	if err := os.WriteFile(lib, []byte("elf"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.so")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		raw      string
		required bool
		wantPath string
		want     string // substring of the problem; "" means usable
	}{
		"unset, optional":        {raw: "", want: "not set"},
		"unset, required":        {raw: "  ", required: true, want: RequireEnv + "=1"},
		"set, optional":          {raw: lib, wantPath: lib},
		"set, required":          {raw: " " + lib + "\n", required: true, wantPath: lib},
		"missing, required":      {raw: filepath.Join(dir, "nope.so"), required: true, wantPath: filepath.Join(dir, "nope.so"), want: "no such file"},
		"empty, required":        {raw: empty, required: true, wantPath: empty, want: "is empty"},
		"directory, required":    {raw: dir, required: true, wantPath: dir, want: "not a regular file"},
		"missing, optional runs": {raw: filepath.Join(dir, "nope.so"), wantPath: filepath.Join(dir, "nope.so")},
	} {
		t.Run(name, func(t *testing.T) {
			path, problem := check(tc.raw, tc.required, os.Stat)
			if path != tc.wantPath {
				t.Errorf("path = %q, want %q", path, tc.wantPath)
			}
			if (tc.want == "") != (problem == "") || !strings.Contains(problem, tc.want) {
				t.Errorf("problem = %q, want %q", problem, tc.want)
			}
		})
	}
}
