package sistatement

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ErrLanesUnsupported means this platform has no exclusive file lock, so a
// lane could be shared by two processes; lanes are refused there (spec
// 2026-10-09 D15).
var ErrLanesUnsupported = errors.New("sistatement: client lanes need flock, which this platform does not provide")

var errLaneLocked = errors.New("sistatement: lane is held by another process")

const (
	laneFileVersion = 1
	lanesDirName    = "lanes"
)

// AcquireReason says why Acquire returned its lane (metrics label).
type AcquireReason string

const (
	AcquireReused     AcquireReason = "reused"
	AcquireNewProcess AcquireReason = "new_process"
	AcquireLostState  AcquireReason = "lost_state"
)

// laneFile is <si_dir>/lanes/<lane>.json.
type laneFile struct {
	Version   int      `json:"version"`
	Lane      string   `json:"lane"`
	Next      uint64   `json:"next"`
	Free      []uint64 `json:"free"`
	Abandoned bool     `json:"abandoned"`
}

// LanePoolOptions configures a pool; every field is optional.
type LanePoolOptions struct {
	Rand      io.Reader                    // nil = crypto/rand
	OnCorrupt func(lane string, err error) // a lane file that cannot be trusted
	OnBurn    func(reason string)          // a provably unspent seq dropped: "free_list_overflow"
}

// LanePool manages this agent's client_seq lanes under <si_dir>/lanes (spec
// 2026-10-09 D15). Each lane is a JSON file plus an exclusive flock on
// <lane>.lock held for the process lifetime, so two processes never share a
// lane and a restarted agent reuses its lane.
type LanePool struct {
	dir     string
	opts    LanePoolOptions
	openDir func(string) (seqDir, error) // A1's directory-fsync seam

	// acquireMu serializes Acquire (and the Rand reads of mint) and guards
	// excluded.
	acquireMu sync.Mutex
	// excluded names lanes this process must never hand out again: a lane
	// whose Abandon could not be persisted is closed and excluded instead.
	excluded map[string]bool
}

// OpenLanePool creates <siDir>/lanes (mode 0700, durably) when missing.
func OpenLanePool(siDir string, opts LanePoolOptions) (*LanePool, error) {
	if !lanesSupported {
		return nil, ErrLanesUnsupported
	}
	if opts.Rand == nil {
		opts.Rand = rand.Reader
	}
	dir := filepath.Join(siDir, lanesDirName)
	// A1's mkdirAllDurable creates <siDir> too when the whole state directory
	// was lost (spec §9.2 "lost state dir"), fsyncing every new entry.
	if err := mkdirAllDurable(dir, 0o700, openSeqDir); err != nil {
		return nil, err
	}
	return &LanePool{dir: dir, opts: opts, openDir: openSeqDir, excluded: map[string]bool{}}, nil
}

// exclude keeps lane out of every later Acquire of this pool.
func (p *LanePool) exclude(lane string) {
	p.acquireMu.Lock()
	defer p.acquireMu.Unlock()
	p.excluded[lane] = true
}

// Acquire returns the first unlocked, intact, not-abandoned lane in sorted
// order, or mints a new one. A file that cannot be trusted is reported and
// skipped, never reused and never deleted.
func (p *LanePool) Acquire() (*LanedStore, AcquireReason, error) {
	p.acquireMu.Lock()
	defer p.acquireMu.Unlock()
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, "", fmt.Errorf("sistatement: list %s: %w", p.dir, err)
	}
	var lanes []string
	found := 0 // every lane file, excluded ones too: none at all means lost state
	for _, e := range entries {
		if lane, ok := strings.CutSuffix(e.Name(), ".json"); ok && sicore.ValidClientLane(lane) && !e.IsDir() {
			found++
			if !p.excluded[lane] {
				lanes = append(lanes, lane)
			}
		}
	}
	slices.Sort(lanes)
	for _, lane := range lanes {
		lock, err := tryLockLaneFile(filepath.Join(p.dir, lane+".lock"))
		if errors.Is(err, errLaneLocked) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		st, err := readLaneFile(filepath.Join(p.dir, lane+".json"), lane)
		if err != nil {
			_ = lock.Unlock()
			if p.opts.OnCorrupt != nil {
				p.opts.OnCorrupt(lane, err)
			}
			continue
		}
		if st.Abandoned {
			_ = lock.Unlock()
			continue
		}
		return &LanedStore{pool: p, lane: lane, lock: lock, state: st}, AcquireReused, nil
	}
	reason := AcquireNewProcess
	if found == 0 {
		reason = AcquireLostState
	}
	s, err := p.mint()
	return s, reason, err
}

func (p *LanePool) mint() (*LanedStore, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var b [8]byte
		if _, err := io.ReadFull(p.opts.Rand, b[:]); err != nil {
			return nil, fmt.Errorf("sistatement: lane id: %w", err)
		}
		lane := hex.EncodeToString(b[:])
		lock, err := tryLockLaneFile(filepath.Join(p.dir, lane+".lock"))
		if errors.Is(err, errLaneLocked) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// Check under the lock: a check before it could race a process that
		// minted the same id, wrote its file and already closed, and a
		// rewrite with next=1 would re-issue that lane's seqs.
		if _, err := os.Lstat(filepath.Join(p.dir, lane+".json")); !errors.Is(err, fs.ErrNotExist) {
			_ = lock.Unlock()
			if err != nil {
				return nil, fmt.Errorf("sistatement: stat lane %s: %w", lane, err)
			}
			continue
		}
		st := laneFile{Version: laneFileVersion, Lane: lane, Next: 1, Free: []uint64{}}
		s := &LanedStore{pool: p, lane: lane, lock: lock}
		if err := s.persist(st); err != nil {
			_ = lock.Unlock()
			return nil, err
		}
		s.state = st
		return s, nil
	}
	return nil, errors.New("sistatement: could not mint an unused client lane")
}

func readLaneFile(path, lane string) (laneFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return laneFile{}, err
	}
	var st laneFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return laneFile{}, fmt.Errorf("decode: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return laneFile{}, errors.New("trailing data after the lane document")
	}
	switch {
	case st.Version != laneFileVersion:
		return laneFile{}, fmt.Errorf("version %d", st.Version)
	case st.Lane != lane:
		return laneFile{}, fmt.Errorf("file names lane %q", st.Lane)
	case st.Next == 0:
		return laneFile{}, errors.New("next is 0")
	case len(st.Free) > MaxFreeSeqs:
		return laneFile{}, fmt.Errorf("%d free seqs", len(st.Free))
	}
	for i, seq := range st.Free {
		if seq == 0 || seq >= st.Next || (i > 0 && seq <= st.Free[i-1]) {
			return laneFile{}, fmt.Errorf("free list %v is not ascending, unique and below next %d", st.Free, st.Next)
		}
	}
	if st.Free == nil {
		st.Free = []uint64{}
	}
	return st, nil
}

// LanedStore is one acquired lane. Every mutation is durable before it
// returns (temp file, fsync, rename, directory fsync).
type LanedStore struct {
	pool   *LanePool
	lane   string
	lock   laneLock
	mu     sync.Mutex
	state  laneFile
	closed bool
	// retired marks a lane this process gave up after an Abandon that could
	// not be persisted: it is closed and excluded like an abandoned one.
	retired bool
}

func (s *LanedStore) Lane() string { return s.lane }

// Abandoned reports whether Abandon durably marked this lane abandoned.
func (s *LanedStore) Abandoned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Abandoned
}

// Retired reports whether this process gave the lane up: abandoned durably,
// or after an Abandon that failed (see retire). No later statement of this
// process uses a retired lane.
func (s *LanedStore) Retired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Abandoned || s.retired
}

func (s *LanedStore) retire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
}

// laneError keeps a lane error's own text while wrapping A1's sentinel, so
// callers match it with errors.Is (ErrSeqClosed, ErrClientSeqExhausted,
// ErrClientSeqReused) exactly as they do for the legacy counter.
type laneError struct {
	msg string
	err error
}

func (e *laneError) Error() string { return e.msg }
func (e *laneError) Unwrap() error { return e.err }

// errClosed refuses every mutation after Close or Abandon: the lock is gone,
// so a successor may own the lane file and a write would corrupt its state.
func (s *LanedStore) errClosed() error {
	return &laneError{msg: fmt.Sprintf("sistatement: lane %s is closed", s.lane), err: ErrSeqClosed}
}

func (s *LanedStore) errExhausted() error {
	return &laneError{msg: fmt.Sprintf("sistatement: lane %s client_seq exhausted", s.lane), err: ErrClientSeqExhausted}
}

// Reserve returns the smallest free seq, else next (then next+1 is
// persisted). Ascending reuse is a sequential append or a start-edge fill,
// which never adds a gap range at the arbiter.
func (s *LanedStore) Reserve() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, s.errClosed()
	}
	next := s.state
	next.Free = slices.Clone(s.state.Free)
	var seq uint64
	if len(next.Free) > 0 {
		seq, next.Free = next.Free[0], next.Free[1:]
	} else {
		if next.Next == math.MaxUint64 {
			return 0, s.errExhausted()
		}
		seq = next.Next
		next.Next++
	}
	if err := s.persist(next); err != nil {
		return 0, err
	}
	s.state = next
	return seq, nil
}

// ReserveSupplied accepts an SDK-supplied seq at or above next.
func (s *LanedStore) ReserveSupplied(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.errClosed()
	}
	if seq == math.MaxUint64 {
		return s.errExhausted() // next would wrap to 0
	}
	if seq < s.state.Next {
		return &laneError{msg: fmt.Sprintf("sistatement: supplied client_seq %d is below lane %s next %d", seq, s.lane, s.state.Next), err: ErrClientSeqReused}
	}
	next := s.state
	next.Next = seq + 1
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = next
	return nil
}

// Release returns a provably unspent seq to the free list; releasing a seq
// already on it is a no-op. A full list drops its largest entry, which only
// burns that seq; OnBurn runs after the store's lock is released.
func (s *LanedStore) Release(seq uint64) error {
	burned, err := s.release(seq)
	if burned && s.pool.opts.OnBurn != nil {
		s.pool.opts.OnBurn("free_list_overflow")
	}
	return err
}

func (s *LanedStore) release(seq uint64) (burned bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, s.errClosed()
	}
	if seq == 0 || seq >= s.state.Next {
		return false, fmt.Errorf("sistatement: client_seq %d was never reserved on lane %s", seq, s.lane)
	}
	if _, found := slices.BinarySearch(s.state.Free, seq); found {
		return false, nil
	}
	next := s.state
	next.Free = slices.Clone(s.state.Free)
	i, _ := slices.BinarySearch(next.Free, seq)
	next.Free = slices.Insert(next.Free, i, seq)
	if len(next.Free) > MaxFreeSeqs {
		next.Free = next.Free[:MaxFreeSeqs]
		burned = true
	}
	if err := s.persist(next); err != nil {
		return false, err
	}
	s.state = next
	return burned, nil
}

// Abandon marks the lane durably abandoned (its gap budget is exhausted) and
// releases its lock; the caller acquires a new lane for later statements.
func (s *LanedStore) Abandon() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.errClosed()
	}
	next := s.state
	next.Abandoned = true
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = next
	s.closed = true
	return s.lock.Unlock()
}

// Close releases the lane's lock; the lane stays reusable.
func (s *LanedStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.lock.Unlock()
}

func (s *LanedStore) persist(st laneFile) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	// Plan A1 Task 11's writeDurable: temp file, fsync, rename, directory fsync.
	return writeDurable(s.pool.openDir, filepath.Join(s.pool.dir, s.lane+".json"), append(b, '\n'))
}

// laneLock is an exclusive lock held for the process lifetime.
type laneLock interface{ Unlock() error }

type unlockFunc func() error

func (f unlockFunc) Unlock() error { return f() }

// tryLockLaneFile takes Plan A1's lockFile (flock LOCK_EX|LOCK_NB on path,
// created when missing) and maps "held by another process" to errLaneLocked.
func tryLockLaneFile(path string) (laneLock, error) {
	unlock, err := lockFile(path)
	switch {
	case errors.Is(err, ErrSeqLocked):
		return nil, errLaneLocked
	case errors.Is(err, ErrSeqLockUnsupported):
		return nil, ErrLanesUnsupported
	case err != nil:
		return nil, err
	}
	return unlockFunc(unlock), nil
}
