package registry

import (
	"context"
	"sync"
	"time"
)

// NewCachedTableStatuses caches successful status answers per table for ttl
// (spec 2026-10-09 §6.4: a burst of INSERTs costs one lookup). Errors are
// never cached, so a failed lookup is retried on the next INSERT.
func NewCachedTableStatuses(src TableStatuses, ttl time.Duration, now func() time.Time) TableStatuses {
	if now == nil {
		now = time.Now
	}
	return &cachedTableStatuses{src: src, ttl: ttl, now: now, entries: map[[2]string]cachedStatus{}}
}

type cachedStatus struct {
	status  TableStatus
	expires time.Time
}

type cachedTableStatuses struct {
	src     TableStatuses
	ttl     time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[[2]string]cachedStatus
}

func (c *cachedTableStatuses) StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error) {
	key := [2]string{database, table}
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && c.now().Before(entry.expires) {
		c.mu.Unlock()
		return entry.status, nil
	}
	c.mu.Unlock()
	status, err := c.src.StorageIntegrityTableStatus(ctx, database, table)
	if err != nil {
		return TableStatus{}, err
	}
	c.mu.Lock()
	c.entries[key] = cachedStatus{status: status, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return status, nil
}
