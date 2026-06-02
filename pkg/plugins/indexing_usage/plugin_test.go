package indexingusage

import (
	"context"
	"net"
	"sync"
	"testing"

	"housegate/housegate/pkg/billing"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/registry"
	"housegate/housegate/pkg/sqlmeta"
)

// fakeSink implements billing.IndexingUsageReporter, recording every
// entry the plugin reports so tests can assert per-INSERT behaviour.
type fakeSink struct {
	mu      sync.Mutex
	entries []billing.IndexingUsageEntry
}

func (f *fakeSink) Report(_ context.Context, entries []billing.IndexingUsageEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, entries...)
}

func (f *fakeSink) all() []billing.IndexingUsageEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]billing.IndexingUsageEntry, len(f.entries))
	copy(out, f.entries)
	return out
}

var _ billing.IndexingUsageReporter = (*fakeSink)(nil)

// fakeDBs implements registry.Databases for tests. Only Get is read
// by the plugin; All exists to satisfy the interface.
type fakeDBs struct {
	m map[string]registry.Database
}

func (f *fakeDBs) Get(id string) (registry.Database, bool) {
	db, ok := f.m[id]
	return db, ok
}

func (f *fakeDBs) All() map[string]registry.Database {
	out := make(map[string]registry.Database, len(f.m))
	for k, v := range f.m {
		out[k] = v
	}
	return out
}

// newTestSession returns a Session backed by one half of a net.Pipe.
// Same pattern as pkg/plugins/usage/usage_test.go::newTestSession.
func newTestSession(t *testing.T) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(1, client)
}

// processorDB returns a fake processor-typed DatabaseInfo for x2y2_0
// matching the real devnet sample (counter_token=counter,
// gauge_reward_per_block=gauge).
func processorDB() *fakeDBs {
	return &fakeDBs{m: map[string]registry.Database{
		"x2y2_0": {
			DbType:      1, // PROCESSOR
			ProcessorId: "x2y2",
			Tables: []registry.Table{
				{Id: "counter_token", Type: "counter"},
				{Id: "gauge_reward_per_block", Type: "gauge"},
				{Id: "swap_event", Type: "event"},
				{Id: "pool", Type: "entity"},
			},
		},
		"userdb": {
			DbType: 0, // USER
			Tables: []registry.Table{{Id: "things", Type: "user"}},
		},
	}}
}

// driverINSERT builds a QueryContext shaped like the rewriter would
// produce for an INSERT into db.table on a driver-flagged session,
// optionally carrying a log_comment setting.
func driverINSERT(sess chsession.Session, db, table, logComment string) *plugin.QueryContext {
	sess.State().SetIsDriver(true)
	q := &chproto.Query{}
	if logComment != "" {
		q.Settings = []chproto.Setting{{Key: LogCommentSettingKey, Value: logComment}}
	}
	return &plugin.QueryContext{
		Session:       sess,
		Query:         q,
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: db, OriginalTable: table, LogicalDatabase: db},
		},
	}
}

// wantOneEntry asserts the sink recorded exactly one entry and returns it.
func wantOneEntry(t *testing.T, sink *fakeSink) billing.IndexingUsageEntry {
	t.Helper()
	got := sink.all()
	if len(got) != 1 {
		t.Fatalf("got %d reported entries, want exactly 1: %+v", len(got), got)
	}
	return got[0]
}

func TestPlugin_OnQuery_ReportsOneUnitForProcessorInsert(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)

	qctx := driverINSERT(sess, "x2y2_0", "counter_token",
		`{"processor_id":"x2y2","watching":true}`)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	want := billing.IndexingUsageEntry{ProcessorID: "x2y2", SKU: "metric", IsBackfilling: false, Units: 1}
	if got != want {
		t.Errorf("reported %+v, want %+v (one unit per INSERT)", got, want)
	}
}

func TestPlugin_OnQuery_ReportsEachINSERTDirectly(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)

	// Two INSERTs into the metric table + one into the event table.
	// Housegate no longer batches locally: each INSERT is its own
	// 1-unit report. sentio-node's usage accumulator folds them back
	// into per-key totals (2 metric, 1 event), so on-chain settlement
	// is identical to the old pre-aggregated path.
	for i := 0; i < 2; i++ {
		qctx := driverINSERT(sess, "x2y2_0", "counter_token",
			`{"processor_id":"x2y2","watching":true}`)
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("OnQuery #%d: %v", i, err)
		}
	}
	qctx := driverINSERT(sess, "x2y2_0", "swap_event",
		`{"processor_id":"x2y2","watching":true}`)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery event: %v", err)
	}

	got := sink.all()
	if len(got) != 3 {
		t.Fatalf("got %d reported entries, want 3 (one per INSERT, no local batching)", len(got))
	}
	folded := map[string]uint64{}
	for _, e := range got {
		if e.Units != 1 {
			t.Errorf("entry %+v: Units = %d, want 1 per INSERT", e, e.Units)
		}
		folded[e.SKU] += e.Units
	}
	if folded["metric"] != 2 {
		t.Errorf("metric total = %d, want 2", folded["metric"])
	}
	if folded["event"] != 1 {
		t.Errorf("event total = %d, want 1", folded["event"])
	}
}

func TestPlugin_OnQuery_BackfillFromLogComment(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)

	qctx := driverINSERT(sess, "x2y2_0", "counter_token",
		`{"processor_id":"x2y2","watching":false}`)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	want := billing.IndexingUsageEntry{ProcessorID: "x2y2", SKU: "metric", IsBackfilling: true, Units: 1}
	if got != want {
		t.Errorf("reported %+v, want %+v (watching=false → backfill)", got, want)
	}
}

func TestPlugin_OnQuery_NoLogCommentDefaultsToWatching(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	if got.IsBackfilling {
		t.Errorf("missing log_comment should default to watching=true (IsBackfilling=false): got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsNonDriverSession(t *testing.T) {
	sess := newTestSession(t)
	// driverINSERT flips IsDriver; build manually instead.
	sink := &fakeSink{}
	p := New(processorDB(), sink)
	qctx := &plugin.QueryContext{
		Session:       sess,
		Query:         &chproto.Query{},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: "x2y2_0", OriginalTable: "counter_token", LogicalDatabase: "x2y2_0"},
		},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("non-driver session should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsSelect(t *testing.T) {
	sess := newTestSession(t)
	sess.State().SetIsDriver(true)
	sink := &fakeSink{}
	p := New(processorDB(), sink)
	qctx := &plugin.QueryContext{
		Session:       sess,
		Query:         &chproto.Query{},
		StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: "x2y2_0", OriginalTable: "counter_token", LogicalDatabase: "x2y2_0"},
		},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("SELECT should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsUserDatabase(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)
	qctx := driverINSERT(sess, "userdb", "things", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("user DB INSERT should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsUnknownTable(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(processorDB(), sink)
	qctx := driverINSERT(sess, "x2y2_0", "newly_created_unseen_table", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("unseen table should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_NilSinkIsNoOp(t *testing.T) {
	sess := newTestSession(t)
	p := New(processorDB(), nil)
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery with nil sink should be a no-op, got err: %v", err)
	}
}

func TestPlugin_PeerTrustAndForward_OptOut(t *testing.T) {
	p := New(processorDB(), &fakeSink{})
	if p.RunOnPeerTrust() {
		t.Error("RunOnPeerTrust must be false to avoid double-counting peer-forwarded inserts")
	}
	if p.RunOnForward() {
		t.Error("RunOnForward must be false to keep the host proxy out of the metering loop")
	}
}
