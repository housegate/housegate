package sistatement

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrClientSeqExhausted means the durable uint64 sequence space has no next
// value. Callers must fail closed; the counter never wraps to zero.
var ErrClientSeqExhausted = errors.New("sistatement: client_seq exhausted")

// SeqCounter is the durable per-account client_seq source (spec §5.1 /
// D6). Next and AdvanceTo write and fsync a newly reserved value BEFORE
// returning, so neither an agent-generated nor an SDK-supplied seq can be
// issued again across restarts; a crash between fsync and submission wastes
// one seq (a gap the accumulator's K=64 budget absorbs).
// One process per (state_dir, account) — sharing a key across agents is out
// of scope.
type SeqCounter struct {
	path string
	mu   sync.Mutex
	last uint64
}

// OpenSeqCounter loads <stateDir>/<lowercase account>.seq (0 when absent).
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error) {
	stateDir = strings.TrimSpace(stateDir)
	account = strings.ToLower(strings.TrimSpace(account))
	if stateDir == "" {
		return nil, errors.New("sistatement: state dir is required")
	}
	if account == "" {
		return nil, errors.New("sistatement: account is required")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("sistatement: create state dir: %w", err)
	}
	c := &SeqCounter{path: filepath.Join(stateDir, account+".seq")}
	b, err := os.ReadFile(c.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		c.last = 0
	case err != nil:
		return nil, fmt.Errorf("sistatement: read %s: %w", c.path, err)
	default:
		last, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("sistatement: corrupt seq file %s: %w", c.path, perr)
		}
		c.last = last
	}
	return c, nil
}

// Path returns the backing file (for logs/tests).
func (c *SeqCounter) Path() string { return c.path }

// Last returns the last issued seq (0 before the first Next).
func (c *SeqCounter) Last() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *SeqCounter) persistLocked(next uint64) error {
	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("sistatement: open %s: %w", tmp, err)
	}
	if _, err := f.WriteString(strconv.FormatUint(next, 10) + "\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sistatement: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("sistatement: rename %s: %w", tmp, err)
	}
	if dir, err := os.Open(filepath.Dir(c.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	c.last = next
	return nil
}

// AdvanceTo reserves seq if it is above the current durable high watermark.
// Lower/equal SDK values never move the counter backwards.
func (c *SeqCounter) AdvanceTo(seq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq == ^uint64(0) {
		return ErrClientSeqExhausted
	}
	if seq <= c.last {
		return nil
	}
	return c.persistLocked(seq)
}

// Next issues last+1 after durably recording it (temp file, fsync, rename,
// directory fsync).
func (c *SeqCounter) Next() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == ^uint64(0) {
		return 0, ErrClientSeqExhausted
	}
	next := c.last + 1
	if err := c.persistLocked(next); err != nil {
		return 0, err
	}
	return next, nil
}
