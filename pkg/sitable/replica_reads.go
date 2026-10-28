package sitable

import (
	"slices"
	"sync"
	"time"
)

// ReplicaReads counts, per database, the queries in flight that read a
// replica-served database, and fences their admission at the host's lease
// deadline (spec 2026-10-14 §11). HouseGate takes one table-state snapshot
// per query, so a query that started while a database was answered Replica
// keeps reading its copy after the host stops answering it. sitablestate
// admits every allowed query that touches a Replica table through Admit and
// ends it when the query completes or its session closes. The host releases a
// database's serving lease only once its published snapshot no longer serves
// the database and InFlight is 0, so its member never installs under a
// running read; and it keeps each database's fence (ServeUntil) at its local
// lease deadline minus serve.HostDeadlineMargin, closing it when it stops
// serving the database, so no read of a copy starts after its member may
// install on the lease's expiry, however long the host's table state takes to
// publish that the database is no longer served. It is safe for concurrent
// use, and a nil *ReplicaReads counts nothing and admits nothing.
type ReplicaReads struct {
	mu       sync.Mutex
	now      func() time.Time
	inFlight map[string]int
	until    map[string]time.Time
}

// NewReplicaReads returns an empty counter whose fences are read on the wall
// clock. Every database is closed until ServeUntil opens it.
func NewReplicaReads() *ReplicaReads {
	return NewReplicaReadsWithClock(time.Now)
}

// NewReplicaReadsWithClock returns an empty counter whose fences are read on
// now: the clock the host computes its lease deadlines on. A nil now is the
// wall clock.
func NewReplicaReadsWithClock(now func() time.Time) *ReplicaReads {
	if now == nil {
		now = time.Now
	}
	return &ReplicaReads{now: now, inFlight: map[string]int{}, until: map[string]time.Time{}}
}

// ServeUntil sets database's admission fence: Admit admits a read of it only
// while the counter's clock is before until. The host calls it with its local
// lease deadline minus serve.HostDeadlineMargin whenever it acquires or renews
// the database's lease, and with the zero time, which closes the fence at
// once, when it stops serving the database. Reads already admitted stay
// counted until they end. It does nothing on a nil receiver or for an empty
// database.
func (r *ReplicaReads) ServeUntil(database string, until time.Time) {
	if r == nil || database == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if until.IsZero() {
		delete(r.until, database)
		return
	}
	if r.until == nil {
		r.until = map[string]time.Time{}
	}
	r.until[database] = until
}

// Begin counts one in-flight read for each distinct, non-empty database
// listed and returns the function that ends it, whatever the fences say.
// end is idempotent: only its first call uncounts. On a nil receiver, or with
// no database, Begin counts nothing and end is a no-op.
func (r *ReplicaReads) Begin(databases []string) (end func()) {
	counted := distinctDatabases(databases)
	if r == nil || len(counted) == 0 {
		return func() {}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.countLocked(counted)
}

// Admit is Begin behind the fences: when the counter's clock is before the
// fence of every distinct, non-empty database listed, it counts one in-flight
// read of each and returns the function that ends it, and closed is empty.
// Otherwise it counts nothing, end is nil and closed is the first listed
// database whose fence is closed. The check and the count are one critical
// section with ServeUntil and InFlight, so a host that closes a fence and then
// reads InFlight either sees an admitted read counted or makes Admit refuse
// it. With no database Admit admits with a no-op end; a nil receiver admits
// nothing.
func (r *ReplicaReads) Admit(databases []string) (end func(), closed string) {
	counted := distinctDatabases(databases)
	if len(counted) == 0 {
		return func() {}, ""
	}
	if r == nil {
		return nil, counted[0]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	clock := r.now
	if clock == nil {
		clock = time.Now
	}
	now := clock()
	for _, database := range counted {
		if until, open := r.until[database]; !open || !now.Before(until) {
			return nil, database
		}
	}
	return r.countLocked(counted), ""
}

// distinctDatabases returns the distinct, non-empty databases listed, in
// their first order.
func distinctDatabases(databases []string) []string {
	counted := make([]string, 0, len(databases))
	for _, database := range databases {
		if database != "" && !slices.Contains(counted, database) {
			counted = append(counted, database)
		}
	}
	return counted
}

// countLocked counts one in-flight read of each database and returns its
// idempotent end. r.mu is held.
func (r *ReplicaReads) countLocked(counted []string) func() {
	if r.inFlight == nil {
		r.inFlight = map[string]int{}
	}
	for _, database := range counted {
		r.inFlight[database]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			for _, database := range counted {
				if r.inFlight[database] <= 1 {
					delete(r.inFlight, database)
				} else {
					r.inFlight[database]--
				}
			}
		})
	}
}

// InFlight returns the number of in-flight reads of database (0 on a nil
// receiver).
func (r *ReplicaReads) InFlight(database string) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inFlight[database]
}

// Counts returns a copy of the in-flight count of every database that has a
// read in flight, for metrics; a database whose reads all ended is absent. A
// nil receiver gives an empty map.
func (r *ReplicaReads) Counts() map[string]int {
	out := map[string]int{}
	if r == nil {
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for database, n := range r.inFlight {
		out[database] = n
	}
	return out
}
