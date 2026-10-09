//go:build linux || darwin

package sistatement

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openPool(t *testing.T, dir string, opts LanePoolOptions) *LanePool {
	t.Helper()
	p, err := OpenLanePool(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func acquire(t *testing.T, p *LanePool) (*LanedStore, AcquireReason) {
	t.Helper()
	s, reason, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, reason
}

func reserve(t *testing.T, s *LanedStore) uint64 {
	t.Helper()
	seq, err := s.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestAcquireMintsThenReusesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if reason != AcquireLostState || len(s.Lane()) != 16 {
		t.Fatalf("first acquire: lane %q reason %s", s.Lane(), reason)
	}
	if reserve(t, s) != 1 || reserve(t, s) != 2 {
		t.Fatal("a new lane starts at seq 1")
	}
	lane := s.Lane()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if again.Lane() != lane || reason != AcquireReused || reserve(t, again) != 3 {
		t.Fatalf("restart: lane %q reason %s", again.Lane(), reason)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "lanes", lane+".json"))
	var f map[string]any
	if err := json.Unmarshal(raw, &f); err != nil || f["version"] != float64(1) || f["lane"] != lane || f["next"] != float64(4) || f["abandoned"] != false {
		t.Fatalf("lane file %s (%v)", raw, err)
	}
	if _, ok := f["free"].([]any); !ok {
		t.Fatalf("free must be a JSON array, got %s", raw)
	}
}

func TestTwoHoldersGetDistinctLanes(t *testing.T) {
	dir := t.TempDir()
	a, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	b, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if a.Lane() == b.Lane() || reason != AcquireNewProcess {
		t.Fatalf("lanes %s / %s reason %s: a held lane must never be shared", a.Lane(), b.Lane(), reason)
	}
}

func TestAbandonedLanesAreNeverReused(t *testing.T) {
	dir := t.TempDir()
	p := openPool(t, dir, LanePoolOptions{})
	s, _ := acquire(t, p)
	old := s.Lane()
	if err := s.Abandon(); err != nil {
		t.Fatal(err)
	}
	next, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if next.Lane() == old {
		t.Fatal("an abandoned lane was reused")
	}
	if _, err := s.Reserve(); err == nil {
		t.Fatal("an abandoned store still reserves")
	}
}

func TestAcquireSkipsCorruptLaneFiles(t *testing.T) {
	dir := t.TempDir()
	lanes := filepath.Join(dir, "lanes")
	if err := os.MkdirAll(lanes, 0o700); err != nil {
		t.Fatal(err)
	}
	corrupt := map[string][]byte{
		"000000000000000a": nil,
		"000000000000000b": []byte("{"),
		"000000000000000c": []byte(`{"version":1,"lane":"000000000000000d","next":5,"free":[],"abandoned":false}`),
		"000000000000000e": []byte(`{"version":1,"lane":"000000000000000e","next":3,"free":[3],"abandoned":false}`),
	}
	for lane, body := range corrupt {
		if err := os.WriteFile(filepath.Join(lanes, lane+".json"), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var reported []string
	s, _ := acquire(t, openPool(t, dir, LanePoolOptions{OnCorrupt: func(lane string, _ error) { reported = append(reported, lane) }}))
	if _, bad := corrupt[s.Lane()]; bad {
		t.Fatalf("reused corrupt lane %s", s.Lane())
	}
	if len(reported) != len(corrupt) {
		t.Fatalf("reported %v, want every corrupt lane", reported)
	}
	for lane, body := range corrupt {
		got, err := os.ReadFile(filepath.Join(lanes, lane+".json"))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("corrupt lane %s was modified or deleted", lane)
		}
	}
}

func TestFreeListOrderingAndCap(t *testing.T) {
	var burned int
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{OnBurn: func(reason string) {
		if reason == "free_list_overflow" {
			burned++
		}
	}}))
	for i := 0; i < 70; i++ {
		reserve(t, s)
	}
	for seq := uint64(70); seq >= 1; seq-- {
		if err := s.Release(seq); err != nil {
			t.Fatal(err)
		}
	}
	if burned != 6 {
		t.Fatalf("burned %d, want 6 (65..70 dropped, the largest first)", burned)
	}
	for want := uint64(1); want <= 64; want++ {
		if got := reserve(t, s); got != want {
			t.Fatalf("reserve = %d, want %d (smallest free first)", got, want)
		}
	}
	if got := reserve(t, s); got != 71 {
		t.Fatalf("after the free list, reserve = %d, want 71", got)
	}
}

func TestReleaseRules(t *testing.T) {
	dir := t.TempDir()
	s, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	reserve(t, s)
	reserve(t, s)
	for _, seq := range []uint64{0, 3, 99} {
		if err := s.Release(seq); err == nil {
			t.Fatalf("Release(%d) of a never-reserved seq accepted", seq)
		}
	}
	if err := s.Release(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(1); err != nil {
		t.Fatalf("a repeated release must be a no-op: %v", err)
	}
	lane := s.Lane()
	_ = s.Close()
	again, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if again.Lane() != lane || reserve(t, again) != 1 || reserve(t, again) != 3 {
		t.Fatal("a released seq must survive a restart and be reused first")
	}
}

func TestReserveSupplied(t *testing.T) {
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{}))
	reserve(t, s)
	if err := s.ReserveSupplied(1); err == nil {
		t.Fatal("a supplied seq below next must be refused")
	}
	if err := s.ReserveSupplied(10); err != nil {
		t.Fatal(err)
	}
	if got := reserve(t, s); got != 11 {
		t.Fatalf("next after supplied 10 = %d", got)
	}
}

func TestOpenLanePoolRefusesAFileAsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "lanes"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLanePool(dir, LanePoolOptions{}); err == nil || errors.Is(err, ErrLanesUnsupported) {
		t.Fatalf("err = %v", err)
	}
}
