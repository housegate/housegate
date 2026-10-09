package sistatement

import (
	"errors"
	"fmt"
)

// ErrSeqLocked means another agent process holds the client_seq lock for the
// same state directory and account (spec 2026-10-09 D15): two processes must
// never draw from one counter.
var ErrSeqLocked = errors.New("sistatement: another housegate agent holds the client_seq lock")

// ErrSeqLockUnsupported refuses a client_seq store on a platform without
// flock (spec 2026-10-09 D15, U9).
var ErrSeqLockUnsupported = errors.New("sistatement: client_seq locking is not supported on this platform")

func lockUnsupported(path string) (func() error, error) {
	return nil, fmt.Errorf("%w: %s", ErrSeqLockUnsupported, path)
}
