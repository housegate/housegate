package sitable

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestReplicaReadsCountsInFlightQueriesPerDatabase is clarification K44 of
// Plan S3-C4: each query counts once per distinct database it reads from a
// copy, until its end runs, so a host can tell when a database's reads have
// drained.
func TestReplicaReadsCountsInFlightQueriesPerDatabase(t *testing.T) {
	r := NewReplicaReads()
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("a new counter = %v, want empty", got)
	}
	endJoin := r.Begin([]string{"db2", "db3", "db2"}) // a join reading two copies of db2 counts db2 once
	endRead := r.Begin([]string{"db2"})
	if r.InFlight("db2") != 2 || r.InFlight("db3") != 1 || r.InFlight("db1") != 0 {
		t.Fatalf("in flight db2=%d db3=%d db1=%d, want 2, 1, 0", r.InFlight("db2"), r.InFlight("db3"), r.InFlight("db1"))
	}
	counts := r.Counts()
	if !reflect.DeepEqual(counts, map[string]int{"db2": 2, "db3": 1}) {
		t.Fatalf("Counts() = %v, want map[db2:2 db3:1]", counts)
	}
	counts["db2"] = 99
	if r.InFlight("db2") != 2 {
		t.Fatal("Counts must return a copy")
	}
	endJoin()
	if r.InFlight("db2") != 1 || r.InFlight("db3") != 0 {
		t.Fatalf("after the join ended db2=%d db3=%d, want 1, 0", r.InFlight("db2"), r.InFlight("db3"))
	}
	endRead()
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("after every read ended Counts() = %v, want empty", got)
	}

	// Concurrent reads drain to zero.
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			end := r.Begin([]string{"db2", "db4"})
			_ = r.InFlight("db2")
			end()
		}()
	}
	wg.Wait()
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("after concurrent reads Counts() = %v, want empty", got)
	}
	// A read of no database, or of an empty name, counts nothing.
	endNone := r.Begin(nil)
	endEmpty := r.Begin([]string{""})
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("Counts() = %v, want empty: nothing was read from a copy", got)
	}
	endNone()
	endEmpty()
}

// TestReplicaReadsEndIsIdempotent: QueryComplete and Close may both end the
// same read, so only the first call uncounts and a second never takes another
// query's count.
func TestReplicaReadsEndIsIdempotent(t *testing.T) {
	r := NewReplicaReads()
	end := r.Begin([]string{"db2"})
	other := r.Begin([]string{"db2"})
	end()
	end()
	if r.InFlight("db2") != 1 {
		t.Fatalf("after one read ended twice in flight = %d, want the other read's 1", r.InFlight("db2"))
	}
	other()
	if r.InFlight("db2") != 0 {
		t.Fatalf("in flight = %d, want 0", r.InFlight("db2"))
	}
}

// TestReplicaReadsNilIsANoOp: a host that serves nothing from copies passes
// no counter, and every method of a nil counter is safe; it admits nothing.
func TestReplicaReadsNilIsANoOp(t *testing.T) {
	var r *ReplicaReads
	end := r.Begin([]string{"db2"})
	end()
	end()
	if r.InFlight("db2") != 0 {
		t.Fatalf("nil InFlight = %d, want 0", r.InFlight("db2"))
	}
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("nil Counts() = %v, want empty", got)
	}
	r.ServeUntil("db2", time.Now().Add(time.Hour))
	if end, closed := r.Admit([]string{"db2"}); end != nil || closed != "db2" {
		t.Fatalf("nil Admit = (end %v, closed %q), want db2 refused", end != nil, closed)
	}
}

// TestReplicaReadsAdmitsOnlyBeforeTheServingFence is clarification K44 of
// Plan S3-C4 (review round 2): every database is closed until the host opens
// its fence with ServeUntil (its local lease deadline minus
// serve.HostDeadlineMargin); Admit then counts a read only while the
// counter's clock is before the fence, and from the fence on, or once the
// host closed it with the zero time, it refuses and counts nothing. Nothing
// but the clock and ServeUntil moves a fence. A join is admitted or refused
// as a whole, a renewal moves the fence, and a closing fence leaves the reads
// it already admitted counted until they end.
func TestReplicaReadsAdmitsOnlyBeforeTheServingFence(t *testing.T) {
	granted := time.Unix(1_760_000_000, 0)
	var mu sync.Mutex
	now := granted
	at := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = granted.Add(d)
	}
	r := NewReplicaReadsWithClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	if end, closed := r.Admit([]string{"db2"}); end != nil || closed != "db2" {
		t.Fatalf("Admit before any fence = (end %v, closed %q), want db2 refused", end != nil, closed)
	}
	r.ServeUntil("db2", granted.Add(27*time.Second))
	r.ServeUntil("db3", granted.Add(time.Hour))

	end, closed := r.Admit([]string{"db3", "db2", "db3"})
	if end == nil || closed != "" || !reflect.DeepEqual(r.Counts(), map[string]int{"db2": 1, "db3": 1}) {
		t.Fatalf("Admit of a join = (end %v, closed %q), in flight %v; want one read of db2 and one of db3", end != nil, closed, r.Counts())
	}
	end()
	at(27*time.Second - time.Nanosecond)
	if end, closed := r.Admit([]string{"db2"}); end == nil || closed != "" {
		t.Fatalf("Admit just before the fence = (end %v, closed %q), want admitted", end != nil, closed)
	} else {
		end()
	}
	at(27 * time.Second)
	for _, databases := range [][]string{{"db2"}, {"db3", "db2"}} {
		if end, closed := r.Admit(databases); end != nil || closed != "db2" {
			t.Fatalf("Admit(%v) at the fence = (end %v, closed %q), want db2 refused", databases, end != nil, closed)
		}
		if got := r.Counts(); len(got) != 0 {
			t.Fatalf("Admit(%v) at the fence counted %v, want nothing", databases, got)
		}
	}

	// A renewal moves the fence; closing it refuses at once, before its time.
	r.ServeUntil("db2", granted.Add(37*time.Second))
	end, closed = r.Admit([]string{"db2"})
	if end == nil || closed != "" {
		t.Fatalf("Admit after the renewal = (end %v, closed %q), want admitted", end != nil, closed)
	}
	r.ServeUntil("db2", time.Time{})
	if again, closed := r.Admit([]string{"db2"}); again != nil || closed != "db2" {
		t.Fatalf("Admit after the fence was closed = (end %v, closed %q), want db2 refused", again != nil, closed)
	}
	if r.InFlight("db2") != 1 {
		t.Fatalf("db2 in flight = %d after its fence closed, want the read admitted before it", r.InFlight("db2"))
	}
	end()
	if got := r.Counts(); len(got) != 0 {
		t.Fatalf("Counts() = %v, want empty", got)
	}

	// A read of no database is admitted and counts nothing.
	if end, closed := r.Admit(nil); end == nil || closed != "" || len(r.Counts()) != 0 {
		t.Fatalf("Admit(nil) = (end %v, closed %q), want a no-op admission", end != nil, closed)
	}

	// NewReplicaReads reads the wall clock.
	w := NewReplicaReads()
	w.ServeUntil("db2", time.Now().Add(time.Hour))
	if end, closed := w.Admit([]string{"db2"}); end == nil || closed != "" {
		t.Fatalf("wall-clock Admit before the fence = (end %v, closed %q), want admitted", end != nil, closed)
	} else {
		end()
	}
	w.ServeUntil("db2", time.Now().Add(-time.Millisecond))
	if end, closed := w.Admit([]string{"db2"}); end != nil || closed != "db2" {
		t.Fatalf("wall-clock Admit after the fence = (end %v, closed %q), want db2 refused", end != nil, closed)
	}
}
