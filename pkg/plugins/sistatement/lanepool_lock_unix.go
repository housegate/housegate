//go:build linux || darwin

package sistatement

// lanesSupported reports whether A1's lockFile has a real flock here; the
// build tags match A1's seq_lock_unix.go / seq_lock_other.go exactly.
const lanesSupported = true
