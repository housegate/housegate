package sistatement

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
// D6). Reserve and AdvanceTo write and fsync a newly reserved value BEFORE
// returning, so neither an agent-generated nor an SDK-supplied seq can be
// issued again across restarts; a crash between fsync and submission wastes
// one seq (a gap the accumulator's K=64 budget absorbs).
// One process per (state_dir, account) is enforced by an exclusive flock on
// <account>.seq.lock (spec 2026-10-09 D15).
type SeqCounter struct {
	path     string
	freePath string
	openDir  func(string) (seqDir, error)
	unlock   func() error
	mu       sync.Mutex
	last     uint64
	free     []uint64 // ascending, each <= last
}

type seqDir interface {
	Sync() error
	Close() error
}

func openSeqDir(path string) (seqDir, error) { return os.Open(path) }

// MaxFreeSeqs caps the durable free list (spec 2026-10-09 §6.5).
const MaxFreeSeqs = 64

type freeListFile struct {
	Version int      `json:"version"`
	Free    []uint64 `json:"free"`
}

// OpenSeqCounter creates stateDir durably when missing (mode 0700), locks
// <account>.seq.lock, and loads <account>.seq (0 when absent) and the free
// list <account>.seq.free (empty when absent). Close releases the lock.
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error) {
	stateDir = strings.TrimSpace(stateDir)
	account = strings.ToLower(strings.TrimSpace(account))
	if stateDir == "" {
		return nil, errors.New("sistatement: state dir is required")
	}
	if account == "" {
		return nil, errors.New("sistatement: account is required")
	}
	if err := mkdirAllDurable(stateDir, 0o700, openSeqDir); err != nil {
		return nil, err
	}
	base := filepath.Join(stateDir, account+".seq")
	unlock, err := lockFile(base + ".lock")
	if err != nil {
		return nil, err
	}
	c := &SeqCounter{path: base, freePath: base + ".free", openDir: openSeqDir, unlock: unlock}
	if err := c.load(); err != nil {
		_ = unlock()
		return nil, err
	}
	return c, nil
}

func (c *SeqCounter) load() error {
	b, err := os.ReadFile(c.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		c.last = 0
	case err != nil:
		return fmt.Errorf("sistatement: read %s: %w", c.path, err)
	default:
		last, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return fmt.Errorf("sistatement: corrupt seq file %s: %w", c.path, perr)
		}
		c.last = last
	}
	fb, err := os.ReadFile(c.freePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sistatement: read %s: %w", c.freePath, err)
	}
	var file freeListFile
	if err := json.Unmarshal(fb, &file); err != nil {
		return fmt.Errorf("sistatement: corrupt free list %s: decode: %w", c.freePath, err)
	}
	if file.Version != 1 {
		return fmt.Errorf("sistatement: corrupt free list %s: unsupported version %d", c.freePath, file.Version)
	}
	if len(file.Free) > MaxFreeSeqs {
		return fmt.Errorf("sistatement: corrupt free list %s: %d entries exceed the cap of %d", c.freePath, len(file.Free), MaxFreeSeqs)
	}
	for i, seq := range file.Free {
		if seq == 0 || seq > c.last || (i > 0 && seq <= file.Free[i-1]) {
			return fmt.Errorf("sistatement: corrupt free list %s: entry %d is not an ascending issued seq", c.freePath, seq)
		}
	}
	c.free = file.Free
	return nil
}

// Close releases the lock. It is idempotent.
func (c *SeqCounter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unlock == nil {
		return nil
	}
	err := c.unlock()
	c.unlock = nil
	return err
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
	if err := writeDurable(c.openDir, c.path, []byte(strconv.FormatUint(next, 10)+"\n")); err != nil {
		// Rename may already be visible, but its crash durability is unproven.
		// Keep last unchanged so an in-process retry repeats the full durable
		// reservation. A restart may observe the renamed value; that only burns a
		// sequence whose failed caller never received.
		return fmt.Errorf("sistatement: persist client_seq %d: %w", next, err)
	}
	c.last = next
	return nil
}

func (c *SeqCounter) persistFreeLocked(free []uint64) error {
	data, err := json.Marshal(freeListFile{Version: 1, Free: free})
	if err != nil {
		return err
	}
	if err := writeDurable(c.openDir, c.freePath, data); err != nil {
		return fmt.Errorf("sistatement: persist free list: %w", err)
	}
	c.free = free
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

// Reserve returns the smallest free seq, removed durably first, else issues
// last+1 durably. Ascending reuse is always a sequential append or a
// start-edge fill at the arbiter, which never opens a gap range.
func (c *SeqCounter) Reserve() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.free) > 0 {
		seq := c.free[0]
		if err := c.persistFreeLocked(append([]uint64(nil), c.free[1:]...)); err != nil {
			return 0, err
		}
		return seq, nil
	}
	if c.last == ^uint64(0) {
		return 0, ErrClientSeqExhausted
	}
	next := c.last + 1
	if err := c.persistLocked(next); err != nil {
		return 0, err
	}
	return next, nil
}

// Next is Reserve; kept for existing callers.
func (c *SeqCounter) Next() (uint64, error) { return c.Reserve() }

// Release returns a provably unspent seq to the free list (spec 2026-10-09
// D16). It may only make a seq reusable for a NEW statement id: the agent never
// re-presents a statement id, and ReserveSupplied keeps refusing seq <= last.
// The list is capped at MaxFreeSeqs; past it the largest entry is dropped and
// stays burned (overflow = true).
func (c *SeqCounter) Release(seq uint64) (overflow bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq == 0 || seq > c.last {
		return false, fmt.Errorf("sistatement: release of never-issued client_seq %d (last %d)", seq, c.last)
	}
	idx := sort.Search(len(c.free), func(i int) bool { return c.free[i] >= seq })
	if idx < len(c.free) && c.free[idx] == seq {
		return false, fmt.Errorf("sistatement: client_seq %d is already free", seq)
	}
	next := make([]uint64, 0, len(c.free)+1)
	next = append(next, c.free[:idx]...)
	next = append(next, seq)
	next = append(next, c.free[idx:]...)
	if len(next) > MaxFreeSeqs {
		next = next[:MaxFreeSeqs]
		overflow = true
	}
	return overflow, c.persistFreeLocked(next)
}
