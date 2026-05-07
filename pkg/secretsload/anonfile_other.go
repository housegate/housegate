//go:build !linux

package secretsload

import (
	"fmt"
	"os"
)

// writeToAnonFile on non-Linux platforms (primarily macOS for local dev)
// falls back to a tempfile with restrictive permissions. Unlike the Linux
// memfd path, plaintext briefly appears on the filesystem — acceptable for
// development but not the intended production substrate. Production runs on
// Linux, where the memfd implementation is used.
func writeToAnonFile(data []byte) (string, func(), error) {
	f, err := os.CreateTemp("", "housegate_secret_*.yaml")
	if err != nil {
		return "", nil, fmt.Errorf("tempfile: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", nil, fmt.Errorf("chmod tempfile: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", nil, fmt.Errorf("write tempfile: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", nil, fmt.Errorf("close tempfile: %w", err)
	}
	name := f.Name()
	return name, func() {
		_ = os.Remove(name)
	}, nil
}
