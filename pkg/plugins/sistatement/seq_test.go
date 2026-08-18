package sistatement

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

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
}
