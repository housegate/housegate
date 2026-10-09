//go:build linux || darwin

package sistatement

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func readLane(t *testing.T, dir, lane string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "lanes", lane+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeLane(t *testing.T, dir, lane, body string) {
	t.Helper()
	lanes := filepath.Join(dir, "lanes")
	if err := os.MkdirAll(lanes, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lanes, lane+".json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A closed or abandoned store no longer holds the flock, so a successor may
// own the lane file: every mutator must refuse without touching it, or a
// late Release would put a successor's issued seq back on the free list.
func TestLanedStoreNeverWritesAfterCloseOrAbandon(t *testing.T) {
	dir := t.TempDir()
	old, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	reserve(t, old)
	reserve(t, old)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	successor, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if successor.Lane() != old.Lane() || reason != AcquireReused {
		t.Fatalf("successor lane %s reason %s", successor.Lane(), reason)
	}
	if got := reserve(t, successor); got != 3 {
		t.Fatalf("successor reserve = %d, want 3", got)
	}
	before := readLane(t, dir, old.Lane())
	if err := old.Release(1); err == nil || !strings.Contains(err.Error(), "is closed") {
		t.Fatalf("Release after Close = %v, want a closed refusal", err)
	}
	if err := old.ReserveSupplied(50); err == nil || !strings.Contains(err.Error(), "is closed") {
		t.Fatalf("ReserveSupplied after Close = %v, want a closed refusal", err)
	}
	if err := old.Abandon(); err == nil || !strings.Contains(err.Error(), "is closed") {
		t.Fatalf("Abandon after Close = %v, want a closed refusal", err)
	}
	if after := readLane(t, dir, old.Lane()); !bytes.Equal(before, after) {
		t.Fatalf("a closed store rewrote its successor's lane file:\n%s\n%s", before, after)
	}

	if err := successor.Abandon(); err != nil {
		t.Fatal(err)
	}
	abandoned := readLane(t, dir, successor.Lane())
	if err := successor.Release(3); err == nil {
		t.Fatal("Release after Abandon accepted")
	}
	if err := successor.Abandon(); err == nil {
		t.Fatal("a second Abandon accepted")
	}
	if err := successor.Close(); err != nil {
		t.Fatalf("Close after Abandon must be a no-op: %v", err)
	}
	if after := readLane(t, dir, successor.Lane()); !bytes.Equal(abandoned, after) {
		t.Fatalf("an abandoned store rewrote its lane file:\n%s\n%s", abandoned, after)
	}
}

func TestAcquireRefusesLaneFilesWithTrailingData(t *testing.T) {
	dir := t.TempDir()
	const lane = "00000000000000aa"
	body := `{"version":1,"lane":"00000000000000aa","next":5,"free":[],"abandoned":false}` + "\n" + `{"next":1}`
	writeLane(t, dir, lane, body)
	var reported []string
	s, _ := acquire(t, openPool(t, dir, LanePoolOptions{OnCorrupt: func(l string, _ error) { reported = append(reported, l) }}))
	if s.Lane() == lane || len(reported) != 1 || reported[0] != lane {
		t.Fatalf("lane %s reported %v: a file holding two documents must not be trusted", s.Lane(), reported)
	}
	if got := string(readLane(t, dir, lane)); got != body {
		t.Fatalf("corrupt lane rewritten: %s", got)
	}
}

// fixedRand yields the given 8-byte lane ids in order.
type fixedRand struct{ ids [][8]byte }

func (r *fixedRand) Read(p []byte) (int, error) {
	if len(r.ids) == 0 {
		return 0, errors.New("fixedRand exhausted")
	}
	n := copy(p, r.ids[0][:])
	r.ids = r.ids[1:]
	return n, nil
}

func TestMintNeverOverwritesAnExistingLaneFile(t *testing.T) {
	dir := t.TempDir()
	abandoned := `{"version":1,"lane":"0000000000000001","next":9,"free":[],"abandoned":true}`
	corrupt := `{"version":1,"lane":"0000000000000002","next":0,"free":[],"abandoned":false}`
	writeLane(t, dir, "0000000000000001", abandoned)
	writeLane(t, dir, "0000000000000002", corrupt)
	r := &fixedRand{ids: [][8]byte{{0, 0, 0, 0, 0, 0, 0, 1}, {0, 0, 0, 0, 0, 0, 0, 2}, {0, 0, 0, 0, 0, 0, 0, 3}}}
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{Rand: r}))
	if s.Lane() != "0000000000000003" || reason != AcquireNewProcess || reserve(t, s) != 1 {
		t.Fatalf("lane %s reason %s", s.Lane(), reason)
	}
	if got := string(readLane(t, dir, "0000000000000001")); got != abandoned {
		t.Fatalf("abandoned lane overwritten: %s", got)
	}
	if got := string(readLane(t, dir, "0000000000000002")); got != corrupt {
		t.Fatalf("corrupt lane overwritten: %s", got)
	}

	r = &fixedRand{ids: [][8]byte{{0, 0, 0, 0, 0, 0, 0, 1}}}
	p := openPool(t, dir, LanePoolOptions{Rand: r})
	// Lane 3 is still held, so this acquire must mint, and the only id offered
	// already has a file.
	if _, _, err := p.Acquire(); err == nil {
		t.Fatal("minted over an existing lane file")
	}
}

func TestLaneSeqExhaustionNeverWraps(t *testing.T) {
	dir := t.TempDir()
	const lane = "00000000000000ff"
	body := fmt.Sprintf(`{"version":1,"lane":%q,"next":%d,"free":[],"abandoned":false}`, lane, uint64(math.MaxUint64))
	writeLane(t, dir, lane, body)
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if s.Lane() != lane || reason != AcquireReused {
		t.Fatalf("lane %s reason %s", s.Lane(), reason)
	}
	if _, err := s.Reserve(); err == nil || !strings.Contains(err.Error(), "client_seq exhausted") {
		t.Fatalf("Reserve at MaxUint64 = %v, want exhausted", err)
	}
	if err := s.ReserveSupplied(math.MaxUint64); err == nil || !strings.Contains(err.Error(), "client_seq exhausted") {
		t.Fatalf("ReserveSupplied(MaxUint64) = %v, want exhausted", err)
	}
	if got := string(readLane(t, dir, lane)); got != body {
		t.Fatalf("exhausted lane rewritten: %s", got)
	}
}

func failingDirSync(injected error) func(string) (seqDir, error) {
	return func(path string) (seqDir, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return &failingSeqDir{file: f, syncErr: injected}, nil
	}
}

// Rename precedes the directory fsync, so a failed persist may still be
// visible on disk. The store must not count the seq as issued in memory, and
// a restart must advance from whatever is visible: no seq ever handed out is
// issued twice.
func TestLanePersistFailureNeverIssuesASeq(t *testing.T) {
	injected := errors.New("injected directory fsync failure")
	dir := t.TempDir()
	p := openPool(t, dir, LanePoolOptions{})
	s, _ := acquire(t, p)
	if got := reserve(t, s); got != 1 {
		t.Fatalf("reserve = %d", got)
	}
	p.openDir = failingDirSync(injected)
	if seq, err := s.Reserve(); seq != 0 || !errors.Is(err, injected) {
		t.Fatalf("Reserve = %d, %v; want 0 and the injected error", seq, err)
	}
	if err := s.Release(1); !errors.Is(err, injected) {
		t.Fatalf("Release = %v, want the injected error", err)
	}
	if err := s.ReserveSupplied(7); !errors.Is(err, injected) {
		t.Fatalf("ReserveSupplied = %v, want the injected error", err)
	}
	if err := s.Abandon(); !errors.Is(err, injected) {
		t.Fatalf("Abandon = %v, want the injected error", err)
	}
	p.openDir = openSeqDir
	// In memory nothing above changed: 1 is still issued and not free, the
	// lane is not abandoned, and the next seq is 2.
	if got := reserve(t, s); got != 2 {
		t.Fatalf("reserve after failures = %d, want 2", got)
	}

	// Crash right after a failed but visible persist: the restart reads the
	// renamed file and never re-issues 3.
	p.openDir = failingDirSync(injected)
	if _, err := s.Reserve(); !errors.Is(err, injected) {
		t.Fatalf("Reserve = %v", err)
	}
	lane := s.Lane()
	_ = s.Close()
	again, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if again.Lane() != lane || reason != AcquireReused {
		t.Fatalf("lane %s reason %s", again.Lane(), reason)
	}
	if got := reserve(t, again); got != 4 {
		t.Fatalf("reserve after restart = %d, want 4 (3 visible, never handed out, burned)", got)
	}
}

func TestMintPersistFailureReleasesTheLock(t *testing.T) {
	injected := errors.New("injected directory fsync failure")
	dir := t.TempDir()
	p := openPool(t, dir, LanePoolOptions{})
	p.openDir = failingDirSync(injected)
	if _, _, err := p.Acquire(); !errors.Is(err, injected) {
		t.Fatalf("Acquire = %v, want the injected error", err)
	}
	// The renamed file is visible and its lock was released, so the lane is
	// reused rather than leaked; it never issued a seq, so it starts at 1.
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if reason != AcquireReused || reserve(t, s) != 1 {
		t.Fatalf("after a failed mint: lane %s reason %s", s.Lane(), reason)
	}
}

func TestLanedStoreConcurrentReservationsAreUnique(t *testing.T) {
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{}))
	const workers, each = 8, 25
	var mu sync.Mutex
	seen := map[uint64]int{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				seq, err := s.Reserve()
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				seen[seq]++
				mu.Unlock()
				if i%3 == 0 {
					mu.Lock()
					seen[seq]--
					mu.Unlock()
					if err := s.Release(seq); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	for seq, n := range seen {
		if n > 1 {
			t.Fatalf("seq %d is held %d times", seq, n)
		}
	}
}

func TestConcurrentAcquiresGetDistinctLanes(t *testing.T) {
	dir := t.TempDir()
	const n = 8
	lanes := make([]string, n)
	stores := make([]*LanedStore, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := OpenLanePool(dir, LanePoolOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			s, _, err := p.Acquire()
			if err != nil {
				t.Error(err)
				return
			}
			stores[i], lanes[i] = s, s.Lane()
		}()
	}
	wg.Wait()
	seen := map[string]bool{}
	for i, lane := range lanes {
		if lane == "" || seen[lane] {
			t.Fatalf("acquire %d got lane %q; lanes %v", i, lane, lanes)
		}
		seen[lane] = true
		_ = stores[i].Close()
	}
}

// TestLanePoolCrashHelperProcess is re-executed by
// TestLaneStateSurvivesProcessKill. It reserves 1..3, releases 2, prints its
// lane and blocks until it is killed without closing anything.
func TestLanePoolCrashHelperProcess(t *testing.T) {
	dir := os.Getenv("SISTATEMENT_LANE_CRASH_DIR")
	if dir == "" {
		t.Skip("helper process only")
	}
	fail := func(err error) {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	p, err := OpenLanePool(dir, LanePoolOptions{})
	if err != nil {
		fail(err)
	}
	s, _, err := p.Acquire()
	if err != nil {
		fail(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Reserve(); err != nil {
			fail(err)
		}
	}
	if err := s.Release(2); err != nil {
		fail(err)
	}
	fmt.Println("LANE", s.Lane())
	select {}
}

func TestLaneStateSurvivesProcessKill(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLanePoolCrashHelperProcess$")
	cmd.Env = append(os.Environ(), "SISTATEMENT_LANE_CRASH_DIR="+dir)
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Any t.Fatal below must not leave the helper running; a second Kill
	// after the deliberate one is harmless.
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "LANE ") {
		t.Fatalf("helper: %q %v", line, err)
	}
	lane := strings.TrimSpace(strings.TrimPrefix(line, "LANE "))

	// While the helper lives, its lane is locked.
	held, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if held.Lane() == lane {
		t.Fatal("acquired a lane held by a live process")
	}
	_ = held.Abandon() // keep the next acquire off this lane

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	// A crash between the temp write and the rename leaves only <lane>.json.tmp.
	tmp := filepath.Join(dir, "lanes", lane+".json.tmp")
	if err := os.WriteFile(tmp, []byte(`{"version":1,"lane":"`+lane+`","next":1`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if s.Lane() != lane || reason != AcquireReused {
		t.Fatalf("after a kill: lane %s reason %s, want %s reused (the kernel drops the flock)", s.Lane(), reason, lane)
	}
	var got []string
	for i := 0; i < 2; i++ {
		got = append(got, strconv.FormatUint(reserve(t, s), 10))
	}
	if strings.Join(got, ",") != "2,4" {
		t.Fatalf("reserve after kill = %v, want 2,4 (released 2 reused, 1 and 3 never re-issued)", got)
	}
}
