//go:build linux || darwin

package sistatement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeLegacyLane struct{ next uint64 }

func (f *fakeLegacyLane) Lane() string                 { return "" }
func (f *fakeLegacyLane) Reserve() (uint64, error)     { f.next++; return f.next, nil }
func (f *fakeLegacyLane) ReserveSupplied(uint64) error { return nil }
func (f *fakeLegacyLane) Release(uint64) error         { return nil }

func TestOwnSuppliedStatementIDLaneRule(t *testing.T) {
	const own = "0x00000000000000000000000000000000000000aa"
	const lane = "5e1f0a2b7c9d3e4f"
	for _, tc := range []struct {
		name, queryID, lane string
		wantOK              bool
		wantErr             string
	}{
		{"foreign id is minted over", "0xbb:1:n", lane, false, ""},
		{"non-SI query id", "my-query", lane, false, ""},
		{"own id on the current lane", own + ":" + lane + ":7:n", lane, true, ""},
		{"own id, account case-folded", strings.ToUpper(own[:4]) + own[4:] + ":" + lane + ":7:n", lane, true, ""},
		{"legacy own id while lanes are on", own + ":7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"own id on another lane", own + ":ffffffffffffffff:7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"own id with an uppercase lane", own + ":5E1F0A2B7C9D3E4F:7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"laned own id while lanes are off", own + ":" + lane + ":7:n", "", false, "SDK statement ids must use the legacy form while client lanes are off"},
		{"legacy own id while lanes are off", own + ":7:n", "", true, ""},
		{"malformed own id is minted over (unchanged legacy behaviour)", own + ":07:n", "", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, ok, err := ownLanedStatementID(tc.queryID, own, tc.lane)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || ok != tc.wantOK {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if ok && (id.Account != own || id.Lane != tc.lane || id.Seq != 7) {
				t.Fatalf("id = %+v", id)
			}
		})
	}
}

func TestLaneRotationIsReadFromTheCodedRejection(t *testing.T) {
	for msg, want := range map[string]laneRotation{
		"storage_integrity: statement 0xaa:5e1f0a2b7c9d3e4f:9:n rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED [client_seq unspent]":  rotationGapBudget,
		"storage_integrity: statement 0xaa:5e1f0a2b7c9d3e4f:1:n rejected by the arbiter: ADMISSION_CODE_LANE_BUDGET_EXCEEDED [client_seq unspent]": rotationLaneBudget,
		"storage_integrity: client lanes are not enabled on this network [client_seq unspent]":                                                     rotationLanesDisabled,
		"storage_integrity: statement 0xaa:1:n rejected by the arbiter: ADMISSION_CODE_DUPLICATE_CLIENT_SEQ":                                       rotationNone,
		"some other error": rotationNone,
	} {
		if got := laneRotationFor(msg); got != want {
			t.Errorf("laneRotationFor(%q) = %v, want %v", msg, got, want)
		}
	}
}

type laneMetrics struct {
	mu        sync.Mutex
	rotations map[string]int
	inflight  int
}

func (m *laneMetrics) LaneRotated(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rotations == nil {
		m.rotations = map[string]int{}
	}
	m.rotations[reason]++
}

func (m *laneMetrics) SIInflight(delta int) { m.mu.Lock(); m.inflight += delta; m.mu.Unlock() }

func (m *laneMetrics) snapshot() (map[string]int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for k, v := range m.rotations {
		out[k] = v
	}
	return out, m.inflight
}

func newSelectorWith(t *testing.T, mode LaneMode, maxInflight int, observer LaneObserver) (*laneSelector, *LanePool) {
	t.Helper()
	pool := openPool(t, t.TempDir(), LanePoolOptions{})
	sel := newLaneSelector(mode, func() (*LanePool, error) { return pool, nil }, maxInflight, observer)
	t.Cleanup(func() { _ = sel.close() })
	return sel, pool
}

func newSelector(t *testing.T, mode LaneMode, maxInflight int) (*laneSelector, *fakeLegacyLane) {
	t.Helper()
	sel, _ := newSelectorWith(t, mode, maxInflight, nil)
	return sel, &fakeLegacyLane{}
}

func TestSelectorUsesLanesOnlyWhenEnabledAndAuto(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 16)
	lane, done, err := sel.pick(context.Background(), false, legacy)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes disabled on the network: %v %v", lane, err)
	}
	done()
	lane, done, err = sel.pick(context.Background(), true, legacy)
	if err != nil || len(lane.Lane()) != 16 {
		t.Fatalf("lanes enabled: %v %v", lane, err)
	}
	done()
	off, legacy2 := newSelector(t, LaneModeOff, 16)
	lane, done, err = off.pick(context.Background(), true, legacy2)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes off (driver sidecar): %v %v", lane, err)
	}
	done()
}

func TestSelectorRotatesOnGapBudgetAndPinsLegacyOnLaneBudget(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 16)
	first, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if err := sel.rotate(first.Lane()); err != nil {
		t.Fatal(err)
	}
	second, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if second.Lane() == first.Lane() || second.Lane() == "" {
		t.Fatalf("rotation kept lane %q", second.Lane())
	}
	sel.pinLegacy("lane budget exceeded")
	third, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if third.Lane() != "" {
		t.Fatal("after LANE_BUDGET_EXCEEDED the process must stay on the legacy lane")
	}
}

func TestSelectorCapsInflightPerLane(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 2)
	_, d1, _ := sel.pick(context.Background(), true, legacy)
	_, d2, _ := sel.pick(context.Background(), true, legacy)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := sel.pick(ctx, true, legacy); err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Fatalf("third pick = %v, want an in-flight refusal", err)
	}
	d1()
	if _, d3, err := sel.pick(context.Background(), true, legacy); err != nil {
		t.Fatal(err)
	} else {
		d3()
	}
	d2()
}

// A waiter blocked on a full lane is woken by a slot freeing up, and a done
// func called twice frees only one slot.
func TestSelectorWaiterWakesAndDoneIsIdempotent(t *testing.T) {
	m := &laneMetrics{}
	sel, _ := newSelectorWith(t, LaneModeAuto, 1, m)
	legacy := &fakeLegacyLane{}
	_, d1, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() {
		_, d2, err := sel.pick(context.Background(), true, legacy)
		if err == nil {
			d2()
		}
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("second pick did not wait: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	d1()
	d1()
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	if _, inflight := m.snapshot(); inflight != 0 {
		t.Fatalf("inflight gauge = %d after every done, want 0", inflight)
	}
	_, d3, _ := sel.pick(context.Background(), true, legacy)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := sel.pick(ctx, true, legacy); err == nil {
		t.Fatal("a double done freed two slots")
	}
	d3()
}

// A waiter on a lane that rotates away proceeds on the new lane at once.
func TestSelectorWaiterMovesToTheRotatedLane(t *testing.T) {
	sel, _ := newSelectorWith(t, LaneModeAuto, 1, nil)
	legacy := &fakeLegacyLane{}
	first, d1, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	defer d1()
	got := make(chan seqLane, 1)
	go func() {
		lane, d2, err := sel.pick(context.Background(), true, legacy)
		if err != nil {
			got <- nil
			return
		}
		d2()
		got <- lane
	}()
	time.Sleep(20 * time.Millisecond)
	if err := sel.rotate(first.Lane()); err != nil {
		t.Fatal(err)
	}
	select {
	case lane := <-got:
		if lane == nil || lane.Lane() == first.Lane() || lane.Lane() == "" {
			t.Fatalf("waiter got %v, want the new lane", lane)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter stayed blocked on the abandoned lane")
	}
}

// A rotation is counted once: the acquire that follows it reports
// new_process, which must not be counted again.
func TestSelectorCountsARotationOnce(t *testing.T) {
	m := &laneMetrics{}
	sel, _ := newSelectorWith(t, LaneModeAuto, 0, m)
	legacy := &fakeLegacyLane{}
	first, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if got, _ := m.snapshot(); got["lost_state"] != 1 || len(got) != 1 {
		t.Fatalf("first acquire counted %v, want lost_state once", got)
	}
	if err := sel.rotate(first.Lane()); err != nil {
		t.Fatal(err)
	}
	if err := sel.rotate(first.Lane()); err != nil {
		t.Fatal(err)
	}
	_, done, err = sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if got, _ := m.snapshot(); got["gap_budget"] != 1 || got["new_process"] != 0 || got["lost_state"] != 1 {
		t.Fatalf("rotation counted %v, want gap_budget once and nothing for the acquire that followed", got)
	}
}

// An Abandon that fails still rotates: the store is closed (its lock
// released) and the lane is never handed out again by this process.
func TestSelectorRotatesWhenAbandonFails(t *testing.T) {
	m := &laneMetrics{}
	sel, pool := newSelectorWith(t, LaneModeAuto, 0, m)
	legacy := &fakeLegacyLane{}
	first, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	done()
	store := first.(*LanedStore)
	// A directory where Abandon's temp file goes fails the write before the
	// rename, so nothing on disk marks the lane abandoned.
	tmp := filepath.Join(pool.dir, first.Lane()+".json.tmp")
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := sel.rotate(first.Lane()); err == nil {
		t.Fatal("rotate hid the Abandon failure")
	}
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	if store.Abandoned() {
		t.Fatal("the failed Abandon is recorded as abandoned")
	}
	if _, err := store.Reserve(); !errors.Is(err, ErrSeqClosed) {
		t.Fatalf("the store that failed to abandon is still usable: %v", err)
	}
	second, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if second.Lane() == first.Lane() {
		t.Fatalf("the lane whose Abandon failed was reused: %s", second.Lane())
	}
	if got, _ := m.snapshot(); got["gap_budget"] != 1 {
		t.Fatalf("rotations = %v, want gap_budget once", got)
	}
}

func TestSelectorCloseRefusesLaterPicks(t *testing.T) {
	sel, pool := newSelectorWith(t, LaneModeAuto, 0, nil)
	legacy := &fakeLegacyLane{}
	lane, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if err := sel.close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sel.pick(context.Background(), true, legacy); !errors.Is(err, ErrSeqClosed) {
		t.Fatalf("pick after close = %v, want ErrSeqClosed", err)
	}
	// The lock is released: a fresh pool reuses the lane.
	again, reason := acquire(t, pool)
	if again.Lane() != lane.Lane() || reason != AcquireReused {
		t.Fatalf("after close: lane %s reason %s, want %s reused", again.Lane(), reason, lane.Lane())
	}
}

func TestParseLaneMode(t *testing.T) {
	for in, want := range map[string]LaneMode{"": LaneModeAuto, "auto": LaneModeAuto, "off": LaneModeOff} {
		if got, err := ParseLaneMode(in); err != nil || got != want {
			t.Errorf("ParseLaneMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"on", "OFF", "yes"} {
		if _, err := ParseLaneMode(bad); err == nil {
			t.Errorf("ParseLaneMode(%q) accepted", bad)
		}
	}
}

// The legacy counter must not be opened (and its lock not taken) by a process
// that uses a client lane: a second agent sharing the state directory would
// otherwise fail on ErrSeqLocked before acquiring its own lane.
func TestLegacyLaneOpensTheCounterOnlyWhenUsed(t *testing.T) {
	opened := 0
	dir := t.TempDir()
	var counters []*SeqCounter
	t.Cleanup(func() {
		for _, c := range counters {
			_ = c.Close()
		}
	})
	legacy := legacyLane{open: func() (*SeqCounter, error) {
		opened++
		c, err := OpenSeqCounter(dir, "0x00000000000000000000000000000000000000aa")
		if err == nil {
			counters = append(counters, c)
		}
		return c, err
	}}
	sel, _ := newSelector(t, LaneModeAuto, 16)
	lane, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil || lane.Lane() == "" {
		t.Fatalf("lanes enabled: %v %v", lane, err)
	}
	if _, err := lane.Reserve(); err != nil {
		t.Fatal(err)
	}
	done()
	if opened != 0 {
		t.Fatalf("a laned statement opened the legacy counter %d times", opened)
	}
	lane, done, err = sel.pick(context.Background(), false, legacy)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes disabled: %v %v", lane, err)
	}
	if seq, err := lane.Reserve(); err != nil || seq != 1 || opened != 1 {
		t.Fatalf("legacy Reserve = %d, %v (opened %d)", seq, err, opened)
	}
	done()
}

// Task 15 carry: the lane store wraps A1's sentinels so callers use errors.Is.
func TestLaneStoreErrorsWrapTheSeqSentinels(t *testing.T) {
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{}))
	if reserve(t, s) != 1 {
		t.Fatal("first seq")
	}
	if err := s.ReserveSupplied(1); !errors.Is(err, ErrClientSeqReused) || !strings.Contains(err.Error(), "is below lane") {
		t.Fatalf("supplied below next = %v, want ErrClientSeqReused", err)
	}
	if err := s.ReserveSupplied(^uint64(0)); !errors.Is(err, ErrClientSeqExhausted) || !strings.Contains(err.Error(), "client_seq exhausted") {
		t.Fatalf("supplied max = %v, want ErrClientSeqExhausted", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(); !errors.Is(err, ErrSeqClosed) || err.Error() != "sistatement: lane "+s.Lane()+" is closed" {
		t.Fatalf("reserve after close = %v, want ErrSeqClosed with the unchanged text", err)
	}
	if s.Abandoned() {
		t.Fatal("a closed lane is not abandoned")
	}
}

// Ruling C1: max_inflight_per_lane caps client lanes only. The legacy lane
// (the driver sidecar, lanes off, a network without lanes) stays unbounded,
// as in Plan A1.
func TestSelectorNeverCapsTheLegacyLane(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         LaneMode
		lanesEnabled bool
	}{{"lanes off", LaneModeOff, true}, {"lanes not enabled", LaneModeAuto, false}} {
		t.Run(tc.name, func(t *testing.T) {
			m := &laneMetrics{}
			sel, _ := newSelectorWith(t, tc.mode, 1, m)
			legacy := &fakeLegacyLane{}
			const n = 20
			var (
				wg    sync.WaitGroup
				mu    sync.Mutex
				dones []func()
				errs  []error
			)
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
					defer cancel()
					lane, done, err := sel.pick(ctx, tc.lanesEnabled, legacy)
					mu.Lock()
					defer mu.Unlock()
					if err != nil || lane.Lane() != "" {
						errs = append(errs, err)
						return
					}
					dones = append(dones, done)
				}()
			}
			wg.Wait()
			if len(errs) != 0 || len(dones) != n {
				t.Fatalf("%d of %d concurrent legacy picks failed or waited: %v", len(errs), n, errs)
			}
			if _, inflight := m.snapshot(); inflight != n {
				t.Fatalf("inflight gauge = %d, want %d (the legacy lane is still counted)", inflight, n)
			}
			for _, d := range dones {
				d()
			}
		})
	}
}
