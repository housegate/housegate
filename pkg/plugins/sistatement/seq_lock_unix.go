//go:build linux || darwin

package sistatement

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking flock on path for the caller's
// lifetime. The kernel releases it if the process dies, so a crashed agent
// never wedges its successor.
func lockFile(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("sistatement: open lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrSeqLocked, path)
		}
		return nil, fmt.Errorf("sistatement: lock %s: %w", path, err)
	}
	return func() error {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return f.Close()
	}, nil
}
