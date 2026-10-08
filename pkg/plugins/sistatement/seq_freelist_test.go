package sistatement

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const freeAccount = "0x00000000000000000000000000000000000000e1"

func openFree(t *testing.T, dir string) *SeqCounter {
	t.Helper()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatalf("OpenSeqCounter: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func reserveN(t *testing.T, c *SeqCounter, n int) []uint64 {
	t.Helper()
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		seq, err := c.Reserve()
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		out = append(out, seq)
	}
	return out
}

func TestSeqCounter_CreatesItsDirectoryDurably(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "si", "devnet2", freeAccount)
	c := openFree(t, dir)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("state dir = %v, %v; want a 0700 directory", info, err)
	}
	if seq, err := c.Reserve(); err != nil || seq != 1 {
		t.Fatalf("first Reserve = %d, %v", seq, err)
	}
}

// Every newly created ancestor must be fsynced into its own parent.
func TestMkdirAllDurableSyncsEveryNewAncestorIntoItsParent(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b", "c")
	var synced []string
	open := func(path string) (seqDir, error) {
		synced = append(synced, path)
		return openSeqDir(path)
	}
	if err := mkdirAllDurable(dir, 0o700, open); err != nil {
		t.Fatal(err)
	}
	want := []string{root, filepath.Join(root, "a"), filepath.Join(root, "a", "b")}
	if strings.Join(synced, "|") != strings.Join(want, "|") {
		t.Fatalf("fsynced parents = %v, want %v", synced, want)
	}
	// An existing directory needs no fsync.
	synced = nil
	if err := mkdirAllDurable(dir, 0o700, open); err != nil || len(synced) != 0 {
		t.Fatalf("existing dir: err=%v synced=%v", err, synced)
	}
}

func TestSeqCounter_ReleaseReusesTheSmallestFirst(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, 5) // 1..5
	for _, seq := range []uint64{4, 2} {
		if overflow, err := c.Release(seq); err != nil || overflow {
			t.Fatalf("Release(%d) = %v, %v", seq, overflow, err)
		}
	}
	if got := reserveN(t, c, 3); got[0] != 2 || got[1] != 4 || got[2] != 6 {
		t.Fatalf("reuse order = %v, want [2 4 6]", got)
	}
	if c.Last() != 6 {
		t.Fatalf("Last = %d, want 6", c.Last())
	}
}

func TestSeqCounter_ReleaseRefusesUnissuedAndDuplicateSeqs(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, 2)
	if _, err := c.Release(3); err == nil {
		t.Fatal("releasing a never-issued seq must fail")
	}
	if _, err := c.Release(0); err == nil {
		t.Fatal("releasing seq 0 must fail")
	}
	if _, err := c.Release(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Release(1); err == nil {
		t.Fatal("releasing a seq twice must fail")
	}
}

func TestSeqCounter_FreeListSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatal(err)
	}
	reserveN(t, c, 3)
	if _, err := c.Release(2); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openFree(t, dir)
	if got := reserveN(t, reopened, 2); got[0] != 2 || got[1] != 4 {
		t.Fatalf("after restart = %v, want [2 4]", got)
	}
	// The legacy .seq keeps its single-integer format for older binaries.
	b, err := os.ReadFile(filepath.Join(dir, freeAccount+".seq"))
	if err != nil || string(b) != "4\n" {
		t.Fatalf(".seq = %q, %v", b, err)
	}
}

func TestSeqCounter_FreeListCapDropsTheLargest(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, MaxFreeSeqs+1)
	for seq := uint64(1); seq <= MaxFreeSeqs; seq++ {
		if overflow, err := c.Release(seq); err != nil || overflow {
			t.Fatalf("Release(%d) = %v, %v", seq, overflow, err)
		}
	}
	overflow, err := c.Release(MaxFreeSeqs + 1)
	if err != nil || !overflow {
		t.Fatalf("Release past the cap = %v, %v; want overflow", overflow, err)
	}
	got := reserveN(t, c, MaxFreeSeqs+1)
	if got[0] != 1 || got[MaxFreeSeqs-1] != MaxFreeSeqs || got[MaxFreeSeqs] != MaxFreeSeqs+2 {
		t.Fatalf("after overflow the largest (%d) must stay burned: %v", MaxFreeSeqs+1, got)
	}
}

func TestSeqCounter_LockRefusesASecondOpener(t *testing.T) {
	dir := t.TempDir()
	first := openFree(t, dir)
	// flock locks belong to an open file description, so a second open in the
	// same process behaves like a second process.
	if _, err := OpenSeqCounter(dir, freeAccount); !errors.Is(err, ErrSeqLocked) {
		t.Fatalf("second opener err = %v, want ErrSeqLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openFree(t, dir)
	if _, err := second.Reserve(); err != nil {
		t.Fatalf("after Close a new opener must work: %v", err)
	}
}

func TestSeqLockUnsupportedStubRefuses(t *testing.T) {
	_, err := lockUnsupported("/tmp/x.lock")
	if !errors.Is(err, ErrSeqLockUnsupported) || !strings.Contains(err.Error(), "/tmp/x.lock") {
		t.Fatalf("stub err = %v", err)
	}
}

// A crash after the free entry is durably removed but before the statement
// is sent only burns it: the reopened counter neither reissues it nor fails.
func TestSeqCounter_FreeRemovalIsDurableBeforeUse(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatal(err)
	}
	reserveN(t, c, 2)
	if _, err := c.Release(1); err != nil {
		t.Fatal(err)
	}
	if seq, _ := c.Reserve(); seq != 1 {
		t.Fatalf("Reserve = %d, want the freed 1", seq)
	}
	_ = c.Close() // "crash" before using seq 1
	reopened := openFree(t, dir)
	if seq, _ := reopened.Reserve(); seq != 3 {
		t.Fatalf("after crash Reserve = %d, want 3 (1 burned, never reissued)", seq)
	}
}

// A freed seq is only reusable for a NEW statement id: ReserveSupplied keeps
// refusing anything at or below the high watermark.
func TestSeqCounter_ReserveSuppliedStillRefusesFreedSeqs(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, 3)
	if _, err := c.Release(2); err != nil {
		t.Fatal(err)
	}
	if err := c.ReserveSupplied(2); !errors.Is(err, ErrClientSeqReused) {
		t.Fatalf("ReserveSupplied(2) = %v, want ErrClientSeqReused", err)
	}
}

func TestSeqCounter_CorruptFreeListFailsClosedWithDistinctMessages(t *testing.T) {
	cases := []struct {
		name, body, want, notWant string
	}{
		{"undecodable", "{not json", "decode", "version"},
		{"wrong version", `{"version":2,"free":[1]}`, "version 2", "decode"},
		{"entry above last", `{"version":1,"free":[9]}`, "ascending issued", ""},
		{"not ascending", `{"version":1,"free":[2,2]}`, "ascending issued", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, freeAccount+".seq"), []byte("3\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, freeAccount+".seq.free"), []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := OpenSeqCounter(dir, freeAccount)
			if err == nil {
				t.Fatal("corrupt free list must fail closed")
			}
			if strings.Contains(err.Error(), "<nil>") || !strings.Contains(err.Error(), tc.want) ||
				(tc.notWant != "" && strings.Contains(err.Error(), tc.notWant)) {
				t.Fatalf("err = %q, want %q and not %q or <nil>", err, tc.want, tc.notWant)
			}
			// The failed open must have released the lock.
			if err := os.WriteFile(filepath.Join(dir, freeAccount+".seq.free"), []byte(`{"version":1,"free":[]}`), 0o600); err != nil {
				t.Fatal(err)
			}
			c, err := OpenSeqCounter(dir, freeAccount)
			if err != nil {
				t.Fatalf("lock leaked by a failed open: %v", err)
			}
			_ = c.Close()
		})
	}
}
