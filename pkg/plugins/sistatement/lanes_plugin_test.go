//go:build linux || darwin

package sistatement

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// lanedMetrics observes the seq, lane and discovery surfaces.
type lanedMetrics struct {
	seqMetrics
	lanes  laneMetrics
	failMu sync.Mutex
	failed map[string]int
}

func (m *lanedMetrics) SIDiscoveryFailed(step string) {
	m.failMu.Lock()
	defer m.failMu.Unlock()
	if m.failed == nil {
		m.failed = map[string]int{}
	}
	m.failed[step]++
}

func (m *lanedMetrics) failedSteps() map[string]int {
	m.failMu.Lock()
	defer m.failMu.Unlock()
	out := map[string]int{}
	for k, v := range m.failed {
		out[k] = v
	}
	return out
}

func (m *lanedMetrics) LaneRotated(reason string) { m.lanes.LaneRotated(reason) }
func (m *lanedMetrics) SIInflight(delta int)      { m.lanes.SIInflight(delta) }

func (m *lanedMetrics) seqSnapshot() (int, map[string]int) {
	m.seqMetrics.mu.Lock()
	defer m.seqMetrics.mu.Unlock()
	burned := map[string]int{}
	for k, v := range m.burned {
		burned[k] = v
	}
	return m.recycled, burned
}

type lanedFixture struct {
	p       *Plugin
	legacy  *SeqCounter
	metrics *lanedMetrics
	laneDir string
}

// newLanedFixture builds a plugin whose network reports client lanes enabled
// through Options.ClientLanesEnabled (the agent without RPC discovery).
func newLanedFixture(t *testing.T, edit func(*Options)) *lanedFixture {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	laneDir := t.TempDir()
	metrics := &lanedMetrics{}
	opts := Options{
		Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: legacy, MaxPayloadBytes: 1 << 20, Observer: metrics,
		Lanes:              LaneModeAuto,
		ClientLanesEnabled: func() bool { return true },
		LaneDir:            func(string) (string, error) { return laneDir, nil },
	}
	if edit != nil {
		edit(&opts)
	}
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return &lanedFixture{p: p, legacy: legacy, metrics: metrics, laneDir: laneDir}
}

func idOf(t *testing.T, flat string) sicore.StatementID {
	t.Helper()
	id, err := sicore.ParseStatementID(flat)
	if err != nil {
		t.Fatalf("statement id %q: %v", flat, err)
	}
	return id
}

func (f *lanedFixture) laneFile(t *testing.T, lane string) laneFile {
	t.Helper()
	st, err := readLaneFile(filepath.Join(f.laneDir, lanesDirName, lane+".json"), lane)
	if err != nil {
		t.Fatalf("lane %s: %v", lane, err)
	}
	return st
}

// claim runs OnQuery and the strict hook for one INSERT on sess.
func (f *lanedFixture) claim(t *testing.T, sess *fakeSession) (*plugin.QueryContext, sicore.StatementID) {
	t.Helper()
	q := insertQctx(sess, lateSQL)
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	return q, idOf(t, signDeferred(t, f.p, q))
}

func markedException(message string) *chproto.Exception {
	return &chproto.Exception{Code: 392, Message: message + chproto.SeqUnspentSuffix}
}

// Spec 2026-10-09 §6.5 with max_inflight_per_lane: 1: every outcome of a
// statement frees its lane's in-flight slot, so the next statement reserves
// on the same lane without waiting.
func TestLanes_EveryOutcomeFreesTheInflightSlot(t *testing.T) {
	f := newLanedFixture(t, func(o *Options) { o.MaxInflightPerLane = 1 })
	f.p.inflightWait = 200 * time.Millisecond // a leaked slot fails the next claim
	outcomes := []struct {
		name   string
		settle func(t *testing.T, sess *fakeSession, q *plugin.QueryContext)
	}{
		{"success", func(t *testing.T, sess *fakeSession, q *plugin.QueryContext) {
			f.p.OnQuerySuccess(context.Background(), sess, q.Query.ID)
			f.p.OnQueryComplete(context.Background(), sess)
		}},
		{"marked unspent Exception", func(t *testing.T, sess *fakeSession, q *plugin.QueryContext) {
			if err := f.p.OnException(context.Background(), sess, markedException("storage_integrity: back-pressure: retry later")); err != nil {
				t.Fatal(err)
			}
			f.p.OnQueryComplete(context.Background(), sess)
		}},
		{"pre-send abort", func(t *testing.T, sess *fakeSession, q *plugin.QueryContext) {
			q.UpstreamQueryUnsent = true
			f.p.OnQueryAbort(context.Background(), q)
			f.p.OnQueryComplete(context.Background(), sess)
		}},
		{"ambiguous abort", func(t *testing.T, sess *fakeSession, q *plugin.QueryContext) {
			f.p.OnQueryAbort(context.Background(), q)
			f.p.OnQueryComplete(context.Background(), sess)
		}},
		{"connection close", func(t *testing.T, sess *fakeSession, q *plugin.QueryContext) {
			f.p.OnClose(sess)
		}},
	}
	var lane string
	for i, o := range outcomes {
		sess := newSession(int64(i+1), "")
		start := time.Now()
		q, id := f.claim(t, sess)
		if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
			t.Fatalf("%s: the claim waited %s for an in-flight slot", o.name, elapsed)
		}
		if !id.IsLaned() || (lane != "" && id.Lane != lane) {
			t.Fatalf("%s: id %+v, want lane %q", o.name, id, lane)
		}
		lane = id.Lane
		o.settle(t, sess, q)
		if o.name == "pre-send abort" {
			if free := f.laneFile(t, lane).Free; !slices.Contains(free, id.Seq) {
				t.Fatalf("pre-send abort: lane %s free list %v does not hold the released seq %d", lane, free, id.Seq)
			}
		}
	}
	// One more statement after the last outcome proves the slot is free too.
	sess := newSession(99, "")
	_, id := f.claim(t, sess)
	f.p.OnQuerySuccess(context.Background(), sess, id.Flat())
	f.p.OnQueryComplete(context.Background(), sess)
	if id.Lane != lane {
		t.Fatalf("final statement moved to lane %s, want %s", id.Lane, lane)
	}
	if _, inflight := f.metrics.lanes.snapshot(); inflight != 0 {
		t.Fatalf("inflight gauge = %d after every outcome, want 0", inflight)
	}
	if f.legacy.Last() != 0 {
		t.Fatalf("a laned process reserved on the legacy counter (last=%d)", f.legacy.Last())
	}
}

// A statement that fails after the lane was picked but before its reservation
// is tracked (an SDK id refused by R9, a reused supplied seq, a signing
// failure) frees the slot directly.
func TestLanes_FailuresBeforeTrackingFreeTheSlot(t *testing.T) {
	f := newLanedFixture(t, func(o *Options) { o.MaxInflightPerLane = 1 })
	f.p.inflightWait = 200 * time.Millisecond
	sess := newSession(1, "")
	_, first := f.claim(t, sess)
	f.p.OnQuerySuccess(context.Background(), sess, first.Flat())
	f.p.OnQueryComplete(context.Background(), sess)
	lane := first.Lane

	strict := func(q *plugin.QueryContext) error {
		if err := f.p.OnQuery(context.Background(), q); err != nil {
			t.Fatalf("OnQuery: %v", err)
		}
		if err := f.p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err != nil {
			t.Fatal(err)
		}
		return f.p.OnQueryInputCompleteStrict(context.Background(), q)
	}
	// R9: an own legacy id while lanes are on.
	legacyID := insertQctx(newSession(2, ""), lateSQL)
	legacyID.Query.ID = f.p.account + ":41:sdk"
	if err := strict(legacyID); err == nil || !strings.Contains(err.Error(), "SDK statement ids must use lane "+lane) {
		t.Fatalf("R9 refusal = %v, want the lane named", err)
	}
	// R9: an own id on another lane.
	otherLane := insertQctx(newSession(3, ""), lateSQL)
	otherLane.Query.ID = f.p.account + ":ffffffffffffffff:41:sdk"
	if err := strict(otherLane); err == nil || !strings.Contains(err.Error(), "SDK statement ids must use lane "+lane) {
		t.Fatalf("R9 refusal = %v, want the lane named", err)
	}
	// A supplied seq below the lane's next.
	reused := insertQctx(newSession(4, ""), lateSQL)
	reused.Query.ID = f.p.account + ":" + lane + ":1:sdk"
	if err := strict(reused); err == nil || !strings.Contains(err.Error(), "below lane") {
		t.Fatalf("reused supplied seq = %v", err)
	}
	// A signing failure releases the seq to the lane and frees the slot.
	relay, _ := auth.NewRelaySigner(testKey)
	good := f.p.signer
	f.p.signer = failingStatementSigner{RelaySigner: relay}
	if err := strict(insertQctx(newSession(5, ""), lateSQL)); err == nil || !strings.Contains(err.Error(), "hsm unavailable") {
		t.Fatalf("signing failure = %v", err)
	}
	f.p.signer = good
	if st := f.laneFile(t, lane); st.Next != 3 || !slices.Equal(st.Free, []uint64{2}) {
		t.Fatalf("lane %s after the refusals = %+v; want next 3 with the signing failure's seq 2 free", lane, st)
	}
	// R9: an own id on the current lane is kept.
	sdk := insertQctx(newSession(6, ""), lateSQL)
	sdk.Query.ID = f.p.account + ":" + lane + ":41:sdk"
	if err := strict(sdk); err != nil {
		t.Fatalf("own laned id: %v", err)
	}
	if sdk.Query.ID != f.p.account+":"+lane+":41:sdk" || f.laneFile(t, lane).Next != 42 {
		t.Fatalf("own laned id = %q next=%d; want it kept with next 42", sdk.Query.ID, f.laneFile(t, lane).Next)
	}
	if recycled, _ := f.metrics.seqSnapshot(); recycled != 1 {
		t.Fatalf("recycled = %d, want the signing failure's release", recycled)
	}
}

func gapBudgetRefusal(id sicore.StatementID) *chproto.Exception {
	return markedException("storage_integrity: statement " + id.Flat() + " rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED")
}

// GAP_BUDGET_EXCEEDED on a client lane abandons it: the refused seq is
// released first, the client is told to retry, the next statement uses a new
// lane, and a release that arrives later for the abandoned lane is dropped.
func TestLanes_GapBudgetRotatesTheLane(t *testing.T) {
	f := newLanedFixture(t, nil)
	s1, s2 := newSession(1, ""), newSession(2, "")
	_, id1 := f.claim(t, s1)
	_, id2 := f.claim(t, s2)
	if id1.Lane != id2.Lane || id1.Seq != 1 || id2.Seq != 2 {
		t.Fatalf("ids %+v %+v", id1, id2)
	}
	exc := gapBudgetRefusal(id1)
	if err := f.p.OnException(context.Background(), s1, exc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(exc.Message, "; retry: the agent moved to a new client_seq lane") || !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("client message = %q; want the retry hint and the unspent marker kept as the suffix", exc.Message)
	}
	old := f.laneFile(t, id1.Lane)
	if !old.Abandoned || !slices.Equal(old.Free, []uint64{1}) {
		t.Fatalf("old lane = %+v; want abandoned with the refused seq released before the rotation", old)
	}
	f.p.OnQueryComplete(context.Background(), s1)
	// s2's statement on the abandoned lane is refused later and marked unspent:
	// its release has nowhere to go and is dropped, neither recycled nor burned.
	if err := f.p.OnException(context.Background(), s2, markedException("storage_integrity: back-pressure: retry later")); err != nil {
		t.Fatal(err)
	}
	f.p.OnQueryComplete(context.Background(), s2)
	if recycled, burned := f.metrics.seqSnapshot(); recycled != 1 || len(burned) != 0 {
		t.Fatalf("recycled=%d burned=%v; want only the GAP_BUDGET release counted", recycled, burned)
	}
	s3 := newSession(3, "")
	_, id3 := f.claim(t, s3)
	if id3.Lane == id1.Lane || id3.Seq != 1 {
		t.Fatalf("after the rotation id = %+v; want a new lane at seq 1", id3)
	}
	if rotations, _ := f.metrics.lanes.snapshot(); rotations["gap_budget"] != 1 || rotations["lost_state"] != 1 || rotations["new_process"] != 0 {
		t.Fatalf("rotations = %v; want lost_state (first lane) and gap_budget once", rotations)
	}
}

// GAP_BUDGET_EXCEEDED on the legacy lane keeps Plan A's behaviour: no
// rotation, no hint.
func TestLanes_LegacyGapBudgetKeepsPlanABehaviour(t *testing.T) {
	f := newLanedFixture(t, func(o *Options) { o.ClientLanesEnabled = func() bool { return false } })
	sess := newSession(1, "")
	_, id := f.claim(t, sess)
	exc := gapBudgetRefusal(id)
	before := exc.Message
	if err := f.p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatal(err)
	}
	if id.IsLaned() || exc.Message != before {
		t.Fatalf("legacy id %+v message %q", id, exc.Message)
	}
	if rotations, _ := f.metrics.lanes.snapshot(); len(rotations) != 0 {
		t.Fatalf("rotations = %v", rotations)
	}
}

// LANE_BUDGET_EXCEEDED pins the process to the legacy lane.
func TestLanes_LaneBudgetPinsTheLegacyLane(t *testing.T) {
	f := newLanedFixture(t, nil)
	sess := newSession(1, "")
	_, id := f.claim(t, sess)
	exc := markedException("storage_integrity: statement " + id.Flat() + " rejected by the arbiter: " + sicore.AdmissionCodeLaneBudgetExceeded)
	if err := f.p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(exc.Message, "; retry: the agent switched to its legacy client_seq lane") || !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("client message = %q", exc.Message)
	}
	f.p.OnQueryComplete(context.Background(), sess)
	_, next := f.claim(t, newSession(2, ""))
	if next.IsLaned() || next.Seq != 1 || f.legacy.Last() != 1 {
		t.Fatalf("after LANE_BUDGET_EXCEEDED id = %+v (legacy last %d); want the legacy lane", next, f.legacy.Last())
	}
}

// With the legacy pin active and the legacy counter held by another process,
// SI writes are refused with a message naming both conditions.
func TestLanes_PinnedLegacyHeldByAnotherProcessRefuses(t *testing.T) {
	legacyDir := t.TempDir()
	signer, _ := auth.NewRelaySigner(testKey)
	f := newLanedFixture(t, func(o *Options) {
		o.Seq = nil
		o.OpenSeq = func(string) (*SeqCounter, error) { return OpenSeqCounter(legacyDir, signer.Address()) }
	})
	holder, err := OpenSeqCounter(legacyDir, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	sess := newSession(1, "")
	_, id := f.claim(t, sess)
	_ = f.p.OnException(context.Background(), sess, markedException("storage_integrity: statement "+id.Flat()+" rejected by the arbiter: "+sicore.AdmissionCodeLaneBudgetExceeded))
	f.p.OnQueryComplete(context.Background(), sess)
	q := insertQctx(newSession(2, ""), lateSQL)
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := f.p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err != nil {
		t.Fatal(err)
	}
	err = f.p.OnQueryInputCompleteStrict(context.Background(), q)
	if err == nil || err.Error() != "storage_integrity agent: client lane budget exhausted and the legacy client_seq lane is held by another process" {
		t.Fatalf("err = %v", err)
	}
}

func TestLanes_OffKeepsLegacyIDs(t *testing.T) {
	f := newLanedFixture(t, func(o *Options) { o.Lanes = LaneModeOff })
	_, id := f.claim(t, newSession(1, ""))
	if id.IsLaned() || id.Seq != 1 {
		t.Fatalf("lanes off produced %+v", id)
	}
}

func TestLanes_EnabledWithoutALaneDirRefuses(t *testing.T) {
	f := newLanedFixture(t, func(o *Options) { o.LaneDir = nil })
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	_ = f.p.OnClientDataStrict(context.Background(), q, encodeRows(t))
	if err := f.p.OnQueryInputCompleteStrict(context.Background(), q); err == nil || !strings.Contains(err.Error(), "no client lane directory") {
		t.Fatalf("err = %v", err)
	}
	if f.legacy.Last() != 0 {
		t.Fatal("a refused laned statement fell back to the legacy lane")
	}
}

// switchingDiscovery answers client_lanes_enabled from a flag the test flips.
type switchingDiscovery struct {
	fakeDiscovery
	lanes bool
}

func (d *switchingDiscovery) StorageIntegrityInfo(ctx context.Context, db string) (registry.StorageIntegrityInfo, error) {
	info, err := d.fakeDiscovery.StorageIntegrityInfo(ctx, db)
	d.mu.Lock()
	info.ClientLanesEnabled = d.lanes
	d.mu.Unlock()
	return info, err
}

func (d *switchingDiscovery) setLanes(v bool) { d.mu.Lock(); d.lanes = v; d.mu.Unlock() }

func (d *switchingDiscovery) calls() int { d.mu.Lock(); defer d.mu.Unlock(); return d.infoCalls }

func discoveryLanedFixture(t *testing.T, d registry.StorageIntegrityDiscovery) (*lanedFixture, *time.Time) {
	t.Helper()
	clock := time.Unix(1_760_000_000, 0)
	f := newLanedFixture(t, func(o *Options) {
		o.NetworkID = ""
		o.Discovery = d
		o.ClientLanesEnabled = nil
		o.Now = func() time.Time { return clock }
	})
	f.p.now = func() time.Time { return clock }
	return f, &clock
}

// F21 / spec 2026-10-09 §6.4: a successful info answer is reused for 60 s and
// then re-read, so an agent that cached client_lanes_enabled=false before
// activation starts using a lane without a restart.
func TestLanes_InfoIsRefreshedAfterTheTTLSoActivationIsPickedUp(t *testing.T) {
	d := &switchingDiscovery{fakeDiscovery: fakeDiscovery{info: goodInfo(), writer: true}}
	f, clock := discoveryLanedFixture(t, d)
	settle := func(sess *fakeSession, id sicore.StatementID) {
		f.p.OnQuerySuccess(context.Background(), sess, id.Flat())
		f.p.OnQueryComplete(context.Background(), sess)
	}
	s1 := newSession(1, "")
	_, first := f.claim(t, s1)
	settle(s1, first)
	if first.IsLaned() || d.calls() != 1 {
		t.Fatalf("before activation id = %+v calls=%d", first, d.calls())
	}
	d.setLanes(true)
	*clock = clock.Add(infoSuccessTTL - time.Second)
	s2 := newSession(2, "")
	_, second := f.claim(t, s2)
	settle(s2, second)
	if second.IsLaned() || d.calls() != 1 {
		t.Fatalf("inside the TTL id = %+v calls=%d; want the cached answer", second, d.calls())
	}
	*clock = clock.Add(2 * time.Second)
	s3 := newSession(3, "")
	_, third := f.claim(t, s3)
	settle(s3, third)
	if !third.IsLaned() || d.calls() != 2 {
		t.Fatalf("after the TTL id = %+v calls=%d; want a refreshed answer and a laned id", third, d.calls())
	}
}

// The ingress's pre-activation refusal expires the cached info at once, so the
// next statement re-reads client_lanes_enabled from the hosting indexer.
func TestLanes_LanesDisabledRefusalExpiresTheCachedInfo(t *testing.T) {
	d := &switchingDiscovery{fakeDiscovery: fakeDiscovery{info: goodInfo(), writer: true}, lanes: true}
	f, clock := discoveryLanedFixture(t, d)
	sess := newSession(1, "")
	_, id := f.claim(t, sess)
	if !id.IsLaned() {
		t.Fatalf("id = %+v, want laned", id)
	}
	if err := f.p.OnException(context.Background(), sess, &chproto.Exception{Code: 403, Message: sicore.ErrClientLanesNotEnabled.Error() + chproto.SeqUnspentSuffix}); err != nil {
		t.Fatal(err)
	}
	f.p.OnQueryComplete(context.Background(), sess)
	f.p.mu.Lock()
	entry, cached := f.p.infos["shop"]
	f.p.mu.Unlock()
	if !cached || !entry.at.IsZero() || entry.info.ClientLanesEnabled || entry.info.NetworkID != testNetworkID {
		t.Fatalf("after the lanes-disabled refusal the cached info is %+v (present %v); want it expired with lanes cleared and the network id kept", entry, cached)
	}
	// The re-read fails: the stale answer keeps the network id and the legacy lane.
	d.mu.Lock()
	d.infoErr = errors.New("indexer unreachable")
	d.mu.Unlock()
	s2 := newSession(2, "")
	_, next := f.claim(t, s2)
	f.p.OnQuerySuccess(context.Background(), s2, next.Flat())
	f.p.OnQueryComplete(context.Background(), s2)
	if next.IsLaned() || d.calls() != 2 {
		t.Fatalf("next id = %+v calls=%d; want a refresh attempt and a legacy id", next, d.calls())
	}
	d.mu.Lock()
	d.infoErr = nil
	d.mu.Unlock()
	d.setLanes(false)
	*clock = clock.Add(infoRefreshRetry) // the failed refresh backs off
	_, third := f.claim(t, newSession(3, ""))
	if third.IsLaned() || d.calls() != 3 {
		t.Fatalf("third id = %+v calls=%d; want a re-read answer and a legacy id", third, d.calls())
	}
	if recycled, _ := f.metrics.seqSnapshot(); recycled != 1 {
		t.Fatalf("recycled = %d; the refused laned seq is released to its lane", recycled)
	}
}

// scriptedDiscovery answers from a script the test controls: an error,
// or an answer with client_lanes_enabled.
type scriptedDiscovery struct {
	mu    sync.Mutex
	lanes bool
	err   error
	calls int
}

func (d *scriptedDiscovery) StorageIntegrityInfo(context.Context, string) (registry.StorageIntegrityInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.err != nil {
		return registry.StorageIntegrityInfo{}, d.err
	}
	info := goodInfo()
	info.ClientLanesEnabled = d.lanes
	return info, nil
}

func (d *scriptedDiscovery) StorageIntegrityWriterCheck(context.Context, string, string) (bool, error) {
	return true, nil
}

func (d *scriptedDiscovery) set(lanes bool, err error) {
	d.mu.Lock()
	d.lanes, d.err = lanes, err
	d.mu.Unlock()
}

func (d *scriptedDiscovery) count() int { d.mu.Lock(); defer d.mu.Unlock(); return d.calls }

// Ruling C2 (stale-while-error): an expired answer triggers a refresh, but a
// failed refresh keeps serving the last good answer (never the configured
// network id or a refusal) and the next statement retries; a later
// successful refresh replaces it.
func TestLanes_ExpiredInfoIsServedWhileTheRefreshFails(t *testing.T) {
	d := &scriptedDiscovery{}
	f, clock := discoveryLanedFixture(t, d)
	claimSettled := func(id int64) sicore.StatementID {
		sess := newSession(id, "")
		_, sid := f.claim(t, sess)
		f.p.OnQuerySuccess(context.Background(), sess, sid.Flat())
		f.p.OnQueryComplete(context.Background(), sess)
		return sid
	}
	if first := claimSettled(1); first.IsLaned() || d.count() != 1 {
		t.Fatalf("first id %+v calls %d", first, d.count())
	}
	*clock = clock.Add(infoSuccessTTL + time.Second)
	d.set(true, errors.New("indexer unreachable"))
	// No network id is configured: dropping the stale answer would refuse.
	if second := claimSettled(2); second.IsLaned() || d.count() != 2 {
		t.Fatalf("refresh failed: id %+v calls %d; want the stale answer served", second, d.count())
	}
	// Inside the retry interval the stale answer is served without a lookup.
	*clock = clock.Add(infoRefreshRetry - time.Second)
	if third := claimSettled(3); third.IsLaned() || d.count() != 2 {
		t.Fatalf("inside the retry interval: id %+v calls %d; want no new refresh", third, d.count())
	}
	*clock = clock.Add(2 * time.Second)
	if third := claimSettled(30); third.IsLaned() || d.count() != 3 {
		t.Fatalf("after the retry interval: id %+v calls %d; want another refresh attempt", third, d.count())
	}
	if f.metrics.failedSteps()["info"] != 2 {
		t.Fatalf("discovery failures = %v, want both failed refreshes counted", f.metrics.failedSteps())
	}
	*clock = clock.Add(infoRefreshRetry + time.Second)
	d.set(true, nil)
	if fourth := claimSettled(4); !fourth.IsLaned() || d.count() != 4 {
		t.Fatalf("refresh recovered: id %+v calls %d; want lanes to engage", fourth, d.count())
	}
	if fifth := claimSettled(5); !fifth.IsLaned() || d.count() != 4 {
		t.Fatalf("fresh answer: id %+v calls %d; want it cached", fifth, d.count())
	}
}

// blockingDiscovery parks every lookup until release is closed.
type blockingDiscovery struct {
	scriptedDiscovery
	entered chan struct{}
	release chan struct{}
}

func (d *blockingDiscovery) StorageIntegrityInfo(ctx context.Context, db string) (registry.StorageIntegrityInfo, error) {
	d.entered <- struct{}{}
	<-d.release
	return d.scriptedDiscovery.StorageIntegrityInfo(ctx, db)
}

// Final review: a burst of statements after expiry triggers at most one
// in-flight refresh; the others are served the stale answer at once.
func TestLanes_ExpiredInfoRefreshIsSingleFlight(t *testing.T) {
	d := &blockingDiscovery{entered: make(chan struct{}, 8), release: make(chan struct{})}
	f, clock := discoveryLanedFixture(t, d)
	first := make(chan error, 1)
	go func() { first <- f.p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL)) }()
	<-d.entered
	close(d.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	f.p.OnQueryAbort(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	f.p.OnClose(newSession(1, ""))

	d.release = make(chan struct{})
	*clock = clock.Add(infoSuccessTTL + time.Second)
	refresher := make(chan error, 1)
	go func() { refresher <- f.p.OnQuery(context.Background(), insertQctx(newSession(2, ""), lateSQL)) }()
	<-d.entered // the refresh is in flight and parked
	for i := int64(3); i < 8; i++ {
		done := make(chan error, 1)
		go func() { done <- f.p.OnQuery(context.Background(), insertQctx(newSession(i, ""), lateSQL)) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("statement %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("statement %d waited for the in-flight refresh", i)
		}
	}
	select {
	case <-d.entered:
		t.Fatal("a second refresh started while one was in flight")
	default:
	}
	close(d.release)
	if err := <-refresher; err != nil {
		t.Fatal(err)
	}
	if d.count() != 2 {
		t.Fatalf("lookups = %d, want the first and one refresh", d.count())
	}
}

// Final review m7: after LANE_BUDGET_EXCEEDED pins the process, an own laned
// SDK id is refused with wording that names the pin, not "lanes are off".
func TestLanes_PinnedR9RefusalNamesThePin(t *testing.T) {
	f := newLanedFixture(t, nil)
	sess := newSession(1, "")
	_, id := f.claim(t, sess)
	_ = f.p.OnException(context.Background(), sess, markedException("storage_integrity: statement "+id.Flat()+" rejected by the arbiter: "+sicore.AdmissionCodeLaneBudgetExceeded))
	f.p.OnQueryComplete(context.Background(), sess)
	q := insertQctx(newSession(2, ""), lateSQL)
	q.Query.ID = f.p.account + ":" + id.Lane + ":41:sdk"
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	_ = f.p.OnClientDataStrict(context.Background(), q, encodeRows(t))
	err := f.p.OnQueryInputCompleteStrict(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "SDK statement ids must use the legacy form: this agent is pinned to its legacy client_seq lane") || strings.Contains(err.Error(), "lanes are off") {
		t.Fatalf("err = %v", err)
	}
}

// Final review m8: when Abandon cannot be persisted the lane is still retired
// in this process: a late release for it is dropped (not counted burned) and
// the next statement moves to a new lane.
func TestLanes_FailedAbandonRetiresTheLane(t *testing.T) {
	f := newLanedFixture(t, nil)
	s1, s2 := newSession(1, ""), newSession(2, "")
	_, id1 := f.claim(t, s1)
	_, id2 := f.claim(t, s2)
	if err := os.Mkdir(filepath.Join(f.laneDir, lanesDirName, id1.Lane+".json.tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	_ = f.p.OnException(context.Background(), s1, gapBudgetRefusal(id1))
	f.p.OnQueryComplete(context.Background(), s1)
	if err := os.Remove(filepath.Join(f.laneDir, lanesDirName, id1.Lane+".json.tmp")); err != nil {
		t.Fatal(err)
	}
	_ = f.p.OnException(context.Background(), s2, markedException("storage_integrity: back-pressure: retry later"))
	f.p.OnQueryComplete(context.Background(), s2)
	// The obstruction also fails s1's own release (a lane-file write), which is
	// burned; s2's later release on the retired lane is dropped, not burned.
	if recycled, burned := f.metrics.seqSnapshot(); recycled != 0 || burned["unknown_outcome"] != 1 || len(burned) != 1 {
		t.Fatalf("recycled=%d burned=%v; want only s1's failed release burned and the late release dropped", recycled, burned)
	}
	_, id3 := f.claim(t, newSession(3, ""))
	if id3.Lane == id1.Lane || id2.Lane != id1.Lane {
		t.Fatalf("ids %+v %+v %+v; want a new lane after the failed abandon", id1, id2, id3)
	}
}

// Final review m4 (spec §6.5, §10): the lane is logged at info when it is
// acquired and when a rotation replaces it.
func TestLanes_AcquiredLaneIsLogged(t *testing.T) {
	f := newLanedFixture(t, nil)
	var buf bytes.Buffer
	ctx := log.WithContext(context.Background(), log.New(slog.NewTextHandler(&buf, nil)))
	claim := func(id int64) sicore.StatementID {
		sess := newSession(id, "")
		q := insertQctx(sess, lateSQL)
		if err := f.p.OnQuery(ctx, q); err != nil {
			t.Fatal(err)
		}
		if err := f.p.OnClientDataStrict(ctx, q, encodeRows(t)); err != nil {
			t.Fatal(err)
		}
		if err := f.p.OnQueryInputCompleteStrict(ctx, q); err != nil {
			t.Fatal(err)
		}
		sid := idOf(t, q.Query.ID)
		return sid
	}
	first := claim(1)
	if !strings.Contains(buf.String(), "level=INFO") || !strings.Contains(buf.String(), "client lane acquired") || !strings.Contains(buf.String(), "lane="+first.Lane) || !strings.Contains(buf.String(), "reason=lost_state") {
		t.Fatalf("log = %q; want the acquired lane at info", buf.String())
	}
	_ = f.p.OnException(ctx, newSession(1, ""), gapBudgetRefusal(first))
	buf.Reset()
	second := claim(2)
	if !strings.Contains(buf.String(), "lane="+second.Lane) || !strings.Contains(buf.String(), "reason=gap_budget") {
		t.Fatalf("log = %q; want the replacement lane with reason gap_budget", buf.String())
	}
}
