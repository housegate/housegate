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

// ErrClientSeqReused means an SDK supplied a sequence at or below the durable
// high watermark. The counter intentionally cannot distinguish an unused gap
// from a previously signed value, so accepting either would make sequence
// reuse possible after restart.
var ErrClientSeqReused = errors.New("sistatement: client_seq was already reserved")

// SeqCounter is the durable per-account client_seq source (spec §5.1 /
// D6). Next and AdvanceTo write and fsync a newly reserved value BEFORE
// returning, so neither an agent-generated nor an SDK-supplied seq can be
// issued again across restarts; a crash between fsync and submission wastes
// one seq (a gap the accumulator's K=64 budget absorbs).
// One process per (state_dir, account) — sharing a key across agents is out
// of scope.
type SeqCounter struct {
	path    string
	openDir func(string) (seqDir, error)
	mu      sync.Mutex
	last    uint64
}

type seqDir interface {
	Sync() error
	Close() error
}

func openSeqDir(path string) (seqDir, error) { return os.Open(path) }

// OpenSeqCounter loads <stateDir>/<lowercase account>.seq (0 when absent).
// stateDir must already exist: deployment owns creating it durably before the
// agent starts, because syncing only stateDir cannot make a newly created
// stateDir entry durable in its parent directory.
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error) {
	stateDir = strings.TrimSpace(stateDir)
	account = strings.ToLower(strings.TrimSpace(account))
	if stateDir == "" {
		return nil, errors.New("sistatement: state dir is required")
	}
	if account == "" {
		return nil, errors.New("sistatement: account is required")
	}
	info, err := os.Stat(stateDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("sistatement: state dir %s must already exist", stateDir)
	}
	if err != nil {
		return nil, fmt.Errorf("sistatement: stat state dir %s: %w", stateDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("sistatement: state dir %s is not a directory", stateDir)
	}
	c := &SeqCounter{path: filepath.Join(stateDir, account+".seq"), openDir: openSeqDir}
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
	dirPath := filepath.Dir(c.path)
	dir, err := c.openDir(dirPath)
	if err != nil {
		return fmt.Errorf("sistatement: open state dir %s for fsync: %w", dirPath, err)
	}
	var durabilityErrs []error
	if err := dir.Sync(); err != nil {
		durabilityErrs = append(durabilityErrs, fmt.Errorf("fsync state dir %s: %w", dirPath, err))
	}
	if err := dir.Close(); err != nil {
		durabilityErrs = append(durabilityErrs, fmt.Errorf("close state dir %s: %w", dirPath, err))
	}
	if err := errors.Join(durabilityErrs...); err != nil {
		// Rename may already be visible, but its crash durability is unproven.
		// Keep last unchanged so an in-process retry repeats the full durable
		// reservation. A restart may observe the renamed value; that only burns a
		// sequence whose failed caller never received.
		return fmt.Errorf("sistatement: persist client_seq %d: %w", next, err)
	}
	c.last = next
	return nil
}

// ReserveSupplied atomically validates and durably reserves a fresh
// SDK-supplied seq above the current high
// watermark. Lower/equal values fail closed: a high-watermark-only store cannot
// prove that a gap was never signed, and zero cannot be recorded distinctly
// from the initial state.
func (c *SeqCounter) ReserveSupplied(seq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq == ^uint64(0) {
		return ErrClientSeqExhausted
	}
	if seq <= c.last {
		return fmt.Errorf("%w: supplied %d, durable high watermark %d", ErrClientSeqReused, seq, c.last)
	}
	return c.persistLocked(seq)
}

// AdvanceTo is retained for callers of the original counter API. Its stricter
// semantics are identical to ReserveSupplied: supplied sequences must be fresh.
func (c *SeqCounter) AdvanceTo(seq uint64) error { return c.ReserveSupplied(seq) }

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
