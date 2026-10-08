package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type countingStatuses struct {
	calls int
	err   error
}

func (c *countingStatuses) StorageIntegrityTableStatus(context.Context, string, string) (TableStatus, error) {
	c.calls++
	return TableStatus{Status: TableStatusActive}, c.err
}

func TestCachedTableStatuses(t *testing.T) {
	now := time.Unix(100, 0)
	src := &countingStatuses{}
	cached := NewCachedTableStatuses(src, 5*time.Second, time.Second, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err != nil {
			t.Fatal(err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("a burst cost %d lookups, want 1", src.calls)
	}
	_, _ = cached.StorageIntegrityTableStatus(context.Background(), "db", "other")
	now = now.Add(5 * time.Second)
	_, _ = cached.StorageIntegrityTableStatus(context.Background(), "db", "t")
	if src.calls != 3 {
		t.Fatalf("calls = %d, want a miss per table and a refresh after the TTL", src.calls)
	}
}

// A failed lookup is remembered for the failure TTL, so an indexer that does
// not answer the status method costs one lookup per TTL instead of one per
// INSERT; a caller cancellation is never remembered (final review M4).
func TestCachedTableStatuses_NegativeCache(t *testing.T) {
	now := time.Unix(100, 0)
	src := &countingStatuses{err: errors.New("method not found")}
	cached := NewCachedTableStatuses(src, 5*time.Second, time.Second, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err == nil || err.Error() != "method not found" {
			t.Fatalf("err = %v, want the remembered failure", err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("a burst of failed lookups cost %d calls, want 1", src.calls)
	}
	now = now.Add(time.Second)
	src.err = nil
	if st, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err != nil || st.Status != TableStatusActive || src.calls != 2 {
		t.Fatalf("after the failure TTL: status=%+v err=%v calls=%d; want a fresh successful lookup", st, err, src.calls)
	}

	for name, tc := range map[string]struct {
		ctx func() context.Context
		err error
	}{
		"source canceled": {context.Background, context.Canceled},
		"caller canceled": {func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, errors.New("rpc aborted")},
	} {
		t.Run(name, func(t *testing.T) {
			src := &countingStatuses{err: tc.err}
			cached := NewCachedTableStatuses(src, 5*time.Second, time.Minute, func() time.Time { return now })
			for i := 0; i < 2; i++ {
				if _, err := cached.StorageIntegrityTableStatus(tc.ctx(), "db", "t"); err == nil {
					t.Fatal("error not propagated")
				}
			}
			if src.calls != 2 {
				t.Fatalf("calls = %d, want 2: a cancellation must not be cached", src.calls)
			}
		})
	}
}

// The cache sweeps expired entries on insert once it holds
// statusCacheSweepSize tables, so an agent writing to many distinct tables
// does not grow it without bound (final review M1).
func TestCachedTableStatuses_SweepsExpiredEntries(t *testing.T) {
	now := time.Unix(100, 0)
	cached := NewCachedTableStatuses(&countingStatuses{}, 5*time.Second, time.Second, func() time.Time { return now }).(*cachedTableStatuses)
	for i := 0; i < statusCacheSweepSize; i++ {
		if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", fmt.Sprintf("t%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(cached.entries); got != statusCacheSweepSize {
		t.Fatalf("entries = %d, want %d", got, statusCacheSweepSize)
	}
	now = now.Add(5 * time.Second) // every entry has expired
	if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "fresh"); err != nil {
		t.Fatal(err)
	}
	if got := len(cached.entries); got != 1 {
		t.Fatalf("entries after an insert above the sweep size = %d, want 1 (only the fresh table)", got)
	}
}
