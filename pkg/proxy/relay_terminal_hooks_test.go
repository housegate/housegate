package proxy

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sitablestate"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// terminalHooks blocks the first OnQueryComplete until release is closed,
// and reports every OnQuery that runs after that complete began.
type terminalHooks struct {
	plugin.NoopHooks
	mu         sync.Mutex
	queries    int
	completing chan struct{} // closed when the first OnQueryComplete begins
	release    chan struct{} // the first OnQueryComplete returns once closed
	overlapped chan string   // an OnQuery that ran while the first complete was running
	once       sync.Once
	blocking   bool
}

func (h *terminalHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	h.queries++
	blocking := h.blocking
	h.mu.Unlock()
	if blocking {
		h.overlapped <- qctx.Query.ID
	}
	return nil
}

func (h *terminalHooks) OnQueryComplete(context.Context, chsession.Session) {
	first := false
	h.once.Do(func() { first = true })
	if !first {
		return
	}
	h.mu.Lock()
	h.blocking = true
	h.mu.Unlock()
	close(h.completing)
	<-h.release
	h.mu.Lock()
	h.blocking = false
	h.mu.Unlock()
}

// TestRelayAdmitsNoQueryWhileThePreviousTerminalHooksRun: a query's terminal
// hooks run before its terminal reaches the client, so a Query that arrives
// while they run was sent before that terminal and is rejected like any
// Query sent before the upstream completed the previous one. It never enters
// OnQuery while the previous query's OnQuerySuccess or OnQueryComplete still
// runs, so a per-session plugin slot (sitablestate's replica read) can never
// be overwritten by the next query and then ended by the previous query's
// complete.
func TestRelayAdmitsNoQueryWhileThePreviousTerminalHooksRun(t *testing.T) {
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		for {
			if _, ok := c.query(); !ok || !c.data() {
				return
			}
			c.endOfStream()
		}
	})
	hooks := &terminalHooks{completing: make(chan struct{}), release: make(chan struct{}), overlapped: make(chan string, 1)}
	c := startFailoverRelay(t, owner.addr, nil, hooks, nil)
	c.mustHello("owner")
	c.sendQuery("q1", "SELECT 1")
	c.sendEmptyData()
	select {
	case <-hooks.completing:
	case <-time.After(10 * time.Second):
		t.Fatal("q1's OnQueryComplete did not run")
	}
	answer := make(chan *chproto.Exception, 1)
	go func() {
		c.sendQuery("q2", "SELECT 2")
		_, exc, _ := c.answer()
		answer <- exc
	}()
	select {
	case id := <-hooks.overlapped:
		close(hooks.release)
		t.Fatalf("query %s entered OnQuery while q1's OnQueryComplete was still running", id)
	case exc := <-answer:
		close(hooks.release)
		if exc == nil || !strings.Contains(exc.Message, `client sent query "q2" before upstream completed query "q1"`) {
			t.Fatalf("q2 answered %v, want the rejection of a query sent before q1's terminal", exc)
		}
	case <-time.After(10 * time.Second):
		close(hooks.release)
		t.Fatal("q2 was neither admitted nor rejected")
	}
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end after the rejected query")
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.queries != 1 {
		t.Fatalf("OnQuery ran %d times, want only q1's", hooks.queries)
	}
}

// replicaPlanner stands in for rewrite: it plans every query as a read of
// the copy db2.a under snapshot.
type replicaPlanner struct{ snapshot sitable.Snapshot }

func (p replicaPlanner) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	qctx.StatementType = sqlmeta.StatementTypeSelect
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "db2", OriginalTable: "a"}}
	qctx.TableSnapshot = p.snapshot
	return nil
}

// completeBlocker holds the first OnQueryComplete, which the chain runs
// before the gate's, until release is closed.
type completeBlocker struct {
	once       sync.Once
	completing chan struct{}
	release    chan struct{}
}

func (b *completeBlocker) OnQueryComplete(context.Context, chsession.Session) {
	b.once.Do(func() {
		close(b.completing)
		<-b.release
	})
}

// completeSignal reports each OnQueryComplete after the gate's has run.
type completeSignal chan struct{}

func (s completeSignal) OnQueryComplete(context.Context, chsession.Session) { s <- struct{}{} }

// TestReplicaReadStaysCountedUntilItsOwnTerminal is the reviewer's
// interleaving on a real relay with the real sitablestate gate: q1's
// terminal is consumed and its complete chain is still running when q2
// arrives. q2 must never reach the upstream uncounted, which it did when it
// entered OnQuery there: q1's late complete then ended q2's registration and
// left InFlight("db2") at 0 while q2 awaited its terminal.
func TestReplicaReadStaysCountedUntilItsOwnTerminal(t *testing.T) {
	table := sitable.Table{ID: "db2.a", Status: sitable.Replica, CopyTable: sitable.CopyTable("db2.a", 1), ReplicaOf: 2}
	reads := sitable.NewReplicaReads()
	reads.ServeUntil("db2", time.Now().Add(time.Hour))
	gate := &sitablestate.Plugin{ReplicaReads: reads, TableState: sitable.NewFake(sitable.Ordinary, table)}
	blocker := &completeBlocker{completing: make(chan struct{}), release: make(chan struct{})}
	completed := make(completeSignal, 4)
	chain := &plugin.PluginChain{
		QueryPlugins:         []plugin.QueryPlugin{replicaPlanner{sitable.NewSnapshot(1, sitable.Ordinary, []sitable.Table{table})}, gate},
		QueryCompletePlugins: []plugin.QueryCompletePlugin{blocker, gate, completed},
		ClosePlugins:         []plugin.ClosePlugin{gate},
	}
	inFlightAtSecondRead := make(chan int, 1)
	owner := startFakeUpstream(t, func(c *fakeConn) {
		if !c.hello("owner") {
			return
		}
		for n := 1; ; n++ {
			if _, ok := c.query(); !ok || !c.data() {
				return
			}
			if n == 2 {
				// q1's complete chain has finished before q2's terminal.
				select {
				case <-completed:
				case <-time.After(10 * time.Second):
				}
				inFlightAtSecondRead <- reads.InFlight("db2")
			}
			c.endOfStream()
		}
	})
	c := startFailoverRelay(t, owner.addr, nil, chain, nil)
	c.mustHello("owner")
	c.sendQuery("q1", "SELECT * FROM db2.a")
	c.sendEmptyData()
	select {
	case <-blocker.completing:
	case <-time.After(10 * time.Second):
		t.Fatal("q1's OnQueryComplete did not run")
	}
	answer := make(chan *chproto.Exception, 1)
	c.sendQuery("q2", "SELECT * FROM db2.a")
	// A rejected q2 ends the session, so its terminator may never be read.
	go func() { _ = c.codec.WriteEmptyDataBlock() }()
	go func() {
		for { // past q1's EndOfStream, to an Exception or the end
			if _, exc, err := c.answer(); exc != nil || err != nil {
				answer <- exc
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond) // let q2 race q1's complete chain
	close(blocker.release)
	select {
	case n := <-inFlightAtSecondRead:
		t.Fatalf("q2 reached the upstream with db2 in flight = %d after q1 completed, want its own read counted", n)
	case exc := <-answer:
		if exc == nil || !strings.Contains(exc.Message, `before upstream completed query "q1"`) {
			t.Fatalf("q2 answered %v, want the rejection of a query sent before q1's terminal", exc)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("q2 was neither forwarded nor rejected")
	}
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not end")
	}
	if got := reads.Counts(); len(got) != 0 {
		t.Fatalf("in flight after the session closed = %v, want empty", got)
	}
}

// TestActiveQueryHandoffToTerminalHooksIsAtomic: consuming the active query's
// terminal and marking its terminal hooks as running is one step. An
// observer racing the handoff never finds the relay idle while the handoff is
// under way: idle there would admit the next query into OnQuery before the
// previous query's completion.
func TestActiveQueryHandoffToTerminalHooksIsAtomic(t *testing.T) {
	r := &Relay{}
	const rounds = 200000
	var (
		round   atomic.Int64 // the handoff round under way, 0 when none
		stop    atomic.Bool
		idleMid atomic.Int64
		wg      sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			before := round.Load()
			_, active := r.currentActiveQuery()
			if !active && before != 0 && round.Load() == before {
				idleMid.Add(1)
			}
		}
	}()
	for i := int64(1); i <= rounds && idleMid.Load() == 0; i++ {
		if !r.beginActiveQuery("q1") {
			t.Fatal("beginActiveQuery refused on an idle relay")
		}
		round.Store(i)
		if _, _, active, _ := r.takeActiveQueryTerminal(); !active {
			t.Fatal("the active query was not taken")
		}
		round.Store(0)
		r.endTerminalHooks()
	}
	stop.Store(true)
	wg.Wait()
	if n := idleMid.Load(); n != 0 {
		t.Fatalf("the relay was observed idle %d time(s) during the handoff to the terminal hooks", n)
	}
}

// TestBeginActiveQueryRefusesWhileTerminalHooksRun: no query begins while the
// previous query's terminal hooks still own the session.
func TestBeginActiveQueryRefusesWhileTerminalHooksRun(t *testing.T) {
	r := &Relay{}
	if !r.beginActiveQuery("q1") {
		t.Fatal("beginActiveQuery refused on an idle relay")
	}
	if _, _, active, _ := r.takeActiveQueryTerminal(); !active {
		t.Fatal("the active query was not taken")
	}
	if r.beginActiveQuery("q2") {
		t.Fatal("q2 began while q1's terminal hooks were running")
	}
	if id, active := r.currentActiveQuery(); !active || id != "q1" {
		t.Fatalf("currentActiveQuery = %q, %v; want q1 while its terminal hooks run", id, active)
	}
	r.endTerminalHooks()
	if !r.beginActiveQuery("q2") {
		t.Fatal("q2 must begin once q1's terminal hooks returned")
	}
}
