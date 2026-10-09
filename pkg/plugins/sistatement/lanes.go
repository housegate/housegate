package sistatement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// LaneMode is storage_integrity.agent.lanes / -si-lanes / HOUSEGATE_SI_LANES.
type LaneMode string

const (
	// LaneModeAuto uses a client lane when the hosting indexer reports
	// client_lanes_enabled (spec 2026-10-09 §6.4).
	LaneModeAuto LaneMode = "auto"
	// LaneModeOff keeps legacy ids (the driver sidecar, R8).
	LaneModeOff LaneMode = "off"
)

// ParseLaneMode maps the configured value onto a LaneMode; "" is auto.
func ParseLaneMode(s string) (LaneMode, error) {
	switch LaneMode(s) {
	case "", LaneModeAuto:
		return LaneModeAuto, nil
	case LaneModeOff:
		return LaneModeOff, nil
	}
	return "", fmt.Errorf("storage_integrity.agent.lanes must be %q or %q, got %q", LaneModeAuto, LaneModeOff, s)
}

// seqLane is the reservation surface shared by the legacy lane (Plan A) and a
// client lane (*LanedStore).
type seqLane interface {
	Lane() string
	Reserve() (uint64, error)
	ReserveSupplied(seq uint64) error
	Release(seq uint64) error
}

// laneSelector picks the lane for each SI statement and bounds the number of
// statements in flight per client lane, which bounds the gap ranges
// out-of-order arrival opens transiently at the arbiter (spec 2026-10-09
// §6.5). The legacy lane is never capped. One
// selector serves one network; it holds at most one client lane at a time.
type laneSelector struct {
	mode        LaneMode
	openPool    func() (*LanePool, error)
	maxInflight int // per client lane; <= 0: unbounded
	observer    LaneObserver

	mu        sync.Mutex
	cond      *sync.Cond
	pool      *LanePool
	current   *LanedStore
	legacyPin string // non-empty: this process stays on the legacy lane
	// rotated is set when a rotation was counted; the acquire that follows it
	// is the same lane change and is not counted again.
	rotated  bool
	closed   bool
	inflight map[string]int // by lane ("" = legacy)
}

func newLaneSelector(mode LaneMode, openPool func() (*LanePool, error), maxInflight int, observer LaneObserver) *laneSelector {
	s := &laneSelector{mode: mode, openPool: openPool, maxInflight: maxInflight, observer: observer, inflight: map[string]int{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// pick returns the lane for one statement and a done func that must be called
// once the statement's outcome is known (success, Exception, abort or session
// close); further calls are no-ops. It blocks while a client lane is at its
// in-flight cap, until ctx ends; a waiter whose lane rotates away moves to the
// new lane. No seq is reserved by pick itself.
func (s *laneSelector) pick(ctx context.Context, lanesEnabled bool, legacy seqLane) (seqLane, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { s.mu.Lock(); s.cond.Broadcast(); s.mu.Unlock() })
	defer stop()
	for {
		if s.closed {
			return nil, nil, fmt.Errorf("storage_integrity agent: client lanes: %w", ErrSeqClosed)
		}
		var lane seqLane = legacy
		if s.mode == LaneModeAuto && lanesEnabled && s.legacyPin == "" {
			if s.current == nil {
				if err := s.acquireLocked(ctx); err != nil {
					return nil, nil, err
				}
			}
			lane = s.current
		}
		key := lane.Lane()
		// Only client lanes are capped (spec 2026-10-09 §6.5): the legacy lane
		// stays unbounded as in Plan A1, which keeps the driver sidecar and
		// lanes-off agents unchanged. It is still counted for the gauge.
		if key == "" || s.maxInflight <= 0 || s.inflight[key] < s.maxInflight {
			s.inflight[key]++
			if s.observer != nil {
				s.observer.SIInflight(1)
			}
			return lane, s.doneFunc(key), nil
		}
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("storage_integrity agent: lane %q has %d statements in flight; retry", key, s.inflight[key])
		}
		s.cond.Wait()
	}
}

func (s *laneSelector) doneFunc(key string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if s.inflight[key]--; s.inflight[key] <= 0 {
				delete(s.inflight, key)
			}
			s.cond.Broadcast()
			s.mu.Unlock()
			if s.observer != nil {
				s.observer.SIInflight(-1)
			}
		})
	}
}

func (s *laneSelector) acquireLocked(ctx context.Context) error {
	if s.pool == nil {
		pool, err := s.openPool()
		if err != nil {
			return fmt.Errorf("storage_integrity agent: open client lanes: %w", err)
		}
		s.pool = pool
	}
	store, reason, err := s.pool.Acquire()
	if err != nil {
		return fmt.Errorf("storage_integrity agent: acquire a client lane: %w", err)
	}
	s.current = store
	logged := string(reason)
	switch {
	case s.rotated:
		// Already counted as gap_budget; the new_process (or reused) acquire
		// that completes the rotation is the same lane change.
		s.rotated = false
		logged = "gap_budget"
	case reason != AcquireReused && s.observer != nil:
		s.observer.LaneRotated(string(reason))
	}
	// Spec 2026-10-09 §6.5, §10: the lane is logged at info on start and on
	// rotation; SDK users read their current lane (R9) here.
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: client lane acquired", "lane", store.Lane(), "reason", logged, "acquire", string(reason))
	return nil
}

// rotate abandons lane after GAP_BUDGET_EXCEEDED; the next pick acquires a
// new lane. A rotation for a lane that is no longer current is a no-op. When
// Abandon fails the selector rotates anyway: the store is closed, releasing
// its lock, and the pool never hands the lane out again in this process; the
// error is returned for logging.
func (s *laneSelector) rotate(lane string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.Lane() != lane {
		return nil
	}
	store := s.current
	s.current = nil
	s.rotated = true
	if s.observer != nil {
		s.observer.LaneRotated("gap_budget")
	}
	s.cond.Broadcast() // waiters on the abandoned lane move to the next one
	if err := store.Abandon(); err != nil {
		s.pool.exclude(lane)
		store.retire()
		return errors.Join(fmt.Errorf("storage_integrity agent: abandon client lane %s: %w", lane, err), store.Close())
	}
	return nil
}

// pinLegacy keeps this process on the legacy lane after LANE_BUDGET_EXCEEDED.
func (s *laneSelector) pinLegacy(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacyPin = reason
}

func (s *laneSelector) legacyPinned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.legacyPin != ""
}

// close releases the current lane's lock (the lane stays reusable) and
// refuses every later pick. It is idempotent.
func (s *laneSelector) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Broadcast()
	if s.current == nil {
		return nil
	}
	err := s.current.Close()
	s.current = nil
	return err
}

// legacyLane is the legacy (lane-less) client_seq counter of Plan A1 as a
// seqLane. open is the plugin's seqFor bound to the statement's network; it is
// called only when the legacy lane is actually used, so a process that uses a
// client lane never takes the legacy counter's lock (spec 2026-10-09 §9.2).
type legacyLane struct {
	open   func() (*SeqCounter, error)
	onBurn func(reason string) // SeqObserver.SeqBurned; nil = no metric
}

func (l legacyLane) Lane() string { return "" }

func (l legacyLane) Reserve() (uint64, error) {
	c, err := l.open()
	if err != nil {
		return 0, err
	}
	return c.Reserve()
}

func (l legacyLane) ReserveSupplied(seq uint64) error {
	c, err := l.open()
	if err != nil {
		return err
	}
	return c.ReserveSupplied(seq)
}

// Release returns seq to the legacy free list. An overflow burns the list's
// largest entry, not necessarily seq, so it is counted here and the caller
// still counts seq as recycled.
func (l legacyLane) Release(seq uint64) error {
	c, err := l.open()
	if err != nil {
		return err
	}
	overflow, err := c.Release(seq)
	if overflow && l.onBurn != nil {
		l.onBurn("free_list_overflow")
	}
	return err
}

// laneRotation is what an upstream refusal asks the agent to do.
type laneRotation int

const (
	rotationNone laneRotation = iota
	rotationGapBudget
	rotationLaneBudget
	rotationLanesDisabled
)

// laneRotationFor reads the coded terminal rejection the ingress composes for
// statementID ("storage_integrity: statement <id> rejected by the arbiter:
// <CODE>", possibly after a prefix and before the unspent marker) and the
// ingress pre-activation refusal. The budget codes are matched only as that
// exact phrase for this statement at the end of the message, so a code name
// inside user-controlled text, or another statement's rejection, never
// rotates a lane. The lanes-disabled refusal only expires cached info, so it
// is matched anywhere.
func laneRotationFor(message, statementID string) laneRotation {
	text := strings.TrimSpace(message)
	if chproto.HasSeqUnspentSuffix(text) {
		text = strings.TrimSpace(strings.TrimSuffix(text, chproto.SeqUnspentSuffix))
	}
	rejected := "statement " + statementID + " rejected by the arbiter: "
	switch {
	case statementID != "" && strings.HasSuffix(text, rejected+sicore.AdmissionCodeGapBudgetExceeded):
		return rotationGapBudget
	case statementID != "" && strings.HasSuffix(text, rejected+sicore.AdmissionCodeLaneBudgetExceeded):
		return rotationLaneBudget
	case strings.Contains(message, sicore.ErrClientLanesNotEnabled.Error()):
		return rotationLanesDisabled
	}
	return rotationNone
}

// withRetryHint appends "; <hint>" to an upstream refusal. A message carrying
// the unspent marker keeps it as its suffix (the hint goes before it), so a
// client that reads the marker still sees it.
func withRetryHint(message, hint string) string {
	if chproto.HasSeqUnspentSuffix(message) {
		trimmed := strings.TrimRight(message, " \t\r\n")
		base := strings.TrimSuffix(trimmed, chproto.SeqUnspentSuffix)
		return base + "; " + hint + chproto.SeqUnspentSuffix + message[len(trimmed):]
	}
	return message + "; " + hint
}

var errSDKLegacyWhileLanesOff = errors.New("SDK statement ids must use the legacy form while client lanes are off")

// errSDKLegacyWhilePinned replaces errSDKLegacyWhileLanesOff when lanes are on
// but LANE_BUDGET_EXCEEDED pinned this process to its legacy lane.
var errSDKLegacyWhilePinned = errors.New("SDK statement ids must use the legacy form: this agent is pinned to its legacy client_seq lane")

// ownLanedStatementID applies spec 2026-10-09 R9 to a client-supplied query
// id. ok is false (and err nil) when the id is not this account's statement
// id, in which case the agent mints one; err is non-nil when it is this
// account's id in the wrong lane form. A malformed own id keeps the legacy
// behaviour (minted over). Only the account segment is case-folded; the lane
// must already be lowercase and the nonce is kept byte for byte.
func ownLanedStatementID(queryID, ownAccount, lane string) (sicore.StatementID, bool, error) {
	account, rest, found := strings.Cut(strings.TrimSpace(queryID), ":")
	if !found || !strings.EqualFold(account, ownAccount) {
		return sicore.StatementID{}, false, nil
	}
	// segments counts the parts after the account: two for a legacy id,
	// three for a laned id.
	segments := strings.Split(rest, ":")
	switch {
	case lane != "" && (len(segments) != 3 || segments[0] != lane):
		return sicore.StatementID{}, false, fmt.Errorf("SDK statement ids must use lane %s", lane)
	case lane == "" && len(segments) == 3:
		return sicore.StatementID{}, false, errSDKLegacyWhileLanesOff
	}
	id, err := sicore.ParseStatementID(strings.ToLower(account) + ":" + rest)
	if err != nil || id.Account != strings.ToLower(ownAccount) {
		return sicore.StatementID{}, false, nil
	}
	return id, true, nil
}
