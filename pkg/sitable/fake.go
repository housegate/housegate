package sitable

import "sync"

// Fake is a TableState whose version tests switch explicitly. Every Set
// publishes a new snapshot with the next version and wakes every Changed
// waiter. It is safe for concurrent use.
type Fake struct {
	mu       sync.Mutex
	fallback Status
	version  uint64
	snap     Snapshot
	changed  chan struct{}
}

// NewFake starts at version 1 with the given tables; every unlisted table
// answers fallback.
func NewFake(fallback Status, tables ...Table) *Fake {
	return &Fake{
		fallback: fallback,
		version:  1,
		snap:     NewSnapshot(1, fallback, tables),
		changed:  make(chan struct{}),
	}
}

// Set replaces the table set, bumps the version and returns it.
func (f *Fake) Set(tables ...Table) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version++
	f.snap = NewSnapshot(f.version, f.fallback, tables)
	close(f.changed)
	f.changed = make(chan struct{})
	return f.version
}

func (f *Fake) Current() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *Fake) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

var _ TableState = (*Fake)(nil)
