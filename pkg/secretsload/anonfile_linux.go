//go:build linux

package secretsload

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// writeToAnonFile stages plaintext in an anonymous memfd. The returned path
// (/proc/self/fd/N) is valid only within this process — opening it produces a
// fresh fd against the same in-memory inode, which is what downstream loaders
// that expect a filesystem path need. Nothing is ever written to disk; the
// fd is MFD_CLOEXEC so child processes don't inherit it.
func writeToAnonFile(data []byte) (string, func(), error) {
	fd, err := unix.MemfdCreate("housegate_secret", unix.MFD_CLOEXEC)
	if err != nil {
		return "", nil, fmt.Errorf("memfd_create: %w", err)
	}
	if _, err := unix.Write(fd, data); err != nil {
		_ = unix.Close(fd)
		return "", nil, fmt.Errorf("memfd write: %w", err)
	}
	// Rewind so re-opens via /proc/self/fd/N start at offset 0. Not strictly
	// required — opening /proc/self/fd/N creates an independent file offset —
	// but makes the fd usable for direct Read too.
	if _, err := unix.Seek(fd, 0, 0); err != nil {
		_ = unix.Close(fd)
		return "", nil, fmt.Errorf("memfd seek: %w", err)
	}
	path := fmt.Sprintf("/proc/self/fd/%d", fd)
	return path, func() { _ = unix.Close(fd) }, nil
}
