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
	cached := NewCachedTableStatuses(src, 5*time.Second, func() time.Time { return now })
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
	src.err = errors.New("down")
	now = now.Add(time.Minute)
	if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err == nil {
		t.Fatal("error not propagated")
	}
	if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err == nil || src.calls != 5 {
		t.Fatalf("errors must not be cached (calls=%d)", src.calls)
	}
}

// The cache sweeps expired entries on insert once it holds
// statusCacheSweepSize tables, so an agent writing to many distinct tables
// does not grow it without bound (final review M1).
func TestCachedTableStatuses_SweepsExpiredEntries(t *testing.T) {
	now := time.Unix(100, 0)
	cached := NewCachedTableStatuses(&countingStatuses{}, 5*time.Second, func() time.Time { return now }).(*cachedTableStatuses)
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
