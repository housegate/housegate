package sistatement

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type failingSeqDir struct {
	file     *os.File
	syncErr  error
	closeErr error
}

func (d *failingSeqDir) Sync() error {
	if d.syncErr != nil {
		return d.syncErr
	}
	return d.file.Sync()
}

func (d *failingSeqDir) Close() error {
	err := d.file.Close()
	if d.closeErr != nil {
		return d.closeErr
	}
	return err
}

func TestSeqCounter_StartsAtOneAndPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xAbC0000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("OpenSeqCounter: %v", err)
	}
	for want := uint64(1); want <= 3; want++ {
		got, err := c.Next()
		if err != nil || got != want {
			t.Fatalf("Next = %d err=%v, want %d", got, err, want)
		}
	}
	if c.Last() != 3 {
		t.Fatalf("Last = %d, want 3", c.Last())
	}
	// File name is the lowercase account; content is the last issued seq.
	b, err := os.ReadFile(filepath.Join(dir, "0xabc0000000000000000000000000000000000001.seq"))
	if err != nil || string(b) != "3\n" {
		t.Fatalf("seq file = %q err=%v, want \"3\\n\"", b, err)
	}
	reopened, err := OpenSeqCounter(dir, "0xabc0000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, _ := reopened.Next(); got != 4 {
		t.Fatalf("after restart Next = %d, want 4 (last+1)", got)
	}
}

func TestSeqCounter_AdvanceToReservesSuppliedSequenceDurably(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Next(); err != nil || got != 1 {
		t.Fatalf("Next = %d err=%v, want 1", got, err)
	}
	if err := c.AdvanceTo(41); err != nil {
		t.Fatalf("AdvanceTo: %v", err)
	}
	if err := c.AdvanceTo(7); err != nil || c.Last() != 41 {
		t.Fatalf("AdvanceTo must not move backwards: last=%d err=%v", c.Last(), err)
	}
	reopened, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Next(); err != nil || got != 42 {
		t.Fatalf("Next after supplied seq = %d err=%v, want 42", got, err)
	}
}

func TestSeqCounter_MaxUint64NeverWrapsOrAdvances(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AdvanceTo(^uint64(0)); !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("AdvanceTo(MaxUint64) = %v, want ErrClientSeqExhausted", err)
	}
	if c.Last() != 0 {
		t.Fatalf("rejected terminal reservation changed last to %d", c.Last())
	}
	if got, err := c.Next(); err != nil || got != 1 {
		t.Fatalf("Next after rejected terminal reservation = %d, %v; want 1", got, err)
	}
	exhaustedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(exhaustedDir, "0xabc.seq"), []byte("18446744073709551615\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	exhausted, err := OpenSeqCounter(exhaustedDir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := exhausted.Next(); got != 0 || !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("Next at exhaustion = %d, %v", got, err)
	}
	if exhausted.Last() != ^uint64(0) {
		t.Fatalf("exhaustion changed durable high watermark to %d", exhausted.Last())
	}
}

func TestSeqCounter_RejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0xabc.seq"), []byte("not-a-number"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSeqCounter(dir, "0xabc"); err == nil {
		t.Fatal("corrupt seq file must fail closed")
	}
}

func TestSeqCounter_ConcurrentNextIsUnique(t *testing.T) {
	c, err := OpenSeqCounter(t.TempDir(), "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu   sync.Mutex
		seen = map[uint64]bool{}
		wg   sync.WaitGroup
	)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Next()
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			mu.Lock()
			if seen[v] {
				t.Errorf("duplicate seq %d", v)
			}
			seen[v] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 32 || c.Last() != 32 {
		t.Fatalf("seen=%d last=%d", len(seen), c.Last())
	}
}

func TestSeqCounter_RequiresAccountAndDir(t *testing.T) {
	if _, err := OpenSeqCounter("", "0xabc"); err == nil {
		t.Fatal("empty dir must be rejected")
	}
	if _, err := OpenSeqCounter(t.TempDir(), ""); err == nil {
		t.Fatal("empty account must be rejected")
	}
	missing := filepath.Join(t.TempDir(), "not-created")
	if _, err := OpenSeqCounter(missing, "0xabc"); err == nil || !strings.Contains(err.Error(), "must already exist") {
		t.Fatalf("missing state dir = %v, want pre-existing-directory error", err)
	}
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSeqCounter(notDir, "0xabc"); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("file state dir = %v, want not-a-directory error", err)
	}
}

func TestSeqCounter_DirectoryDurabilityFailuresNeverIssueSequence(t *testing.T) {
	injected := errors.New("injected directory durability failure")
	tests := []struct {
		name    string
		openDir func(string) (seqDir, error)
	}{
		{
			name: "open",
			openDir: func(string) (seqDir, error) {
				return nil, injected
			},
		},
		{
			name: "sync",
			openDir: func(path string) (seqDir, error) {
				f, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				return &failingSeqDir{file: f, syncErr: injected}, nil
			},
		},
		{
			name: "close",
			openDir: func(path string) (seqDir, error) {
				f, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				return &failingSeqDir{file: f, closeErr: injected}, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			c, err := OpenSeqCounter(dir, "0xabc")
			if err != nil {
				t.Fatal(err)
			}
			c.openDir = tc.openDir
			if got, err := c.Next(); got != 0 || !errors.Is(err, injected) {
				t.Fatalf("Next = %d, %v; want 0 and injected error", got, err)
			}
			if got := c.Last(); got != 0 {
				t.Fatalf("failed reservation changed issued high watermark to %d", got)
			}

			// Rename precedes directory durability. In a live process the new file is
			// visible even when the directory operation reports failure; a restart
			// must therefore advance from it, never move backwards or reissue it as
			// though the failed call had succeeded.
			reopened, err := OpenSeqCounter(dir, "0xabc")
			if err != nil {
				t.Fatal(err)
			}
			if got := reopened.Last(); got != 1 {
				t.Fatalf("reopened high watermark = %d, want visible renamed value 1", got)
			}
			if got, err := reopened.Next(); err != nil || got != 2 {
				t.Fatalf("Next after restart = %d, %v; want 2", got, err)
			}
		})
	}
}

func TestSeqCounter_DirectoryDurabilityFailuresRejectAdvanceTo(t *testing.T) {
	injected := errors.New("injected directory sync failure")
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	c.openDir = func(path string) (seqDir, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return &failingSeqDir{file: f, syncErr: injected}, nil
	}
	if err := c.AdvanceTo(41); !errors.Is(err, injected) {
		t.Fatalf("AdvanceTo = %v, want injected error", err)
	}
	if got := c.Last(); got != 0 {
		t.Fatalf("failed AdvanceTo changed issued high watermark to %d", got)
	}
	reopened, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Next(); err != nil || got != 42 {
		t.Fatalf("Next after restart = %d, %v; want 42", got, err)
	}
}
