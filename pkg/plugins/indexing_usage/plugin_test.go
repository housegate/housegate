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

// driverINSERT builds a QueryContext shaped like the rewriter would
// produce for an INSERT into db.table on a driver-flagged session,
// optionally carrying a log_comment setting. The plugin reads nothing
// from a registry, so no DB fixtures are needed.
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

func TestPlugin_OnQuery_ReportsGenericCoordinates(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(sink)

	const lc = `{"processor_id":"x2y2","watching":true}`
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", lc)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	// Generic ClickHouse coordinates only: the destination logical db +
	// table and the verbatim log_comment. No processor, table type, or
	// SKU — the host resolves those from its registry.
	want := billing.IndexingUsageEntry{LogicalDatabase: "x2y2_0", Table: "counter_token", LogComment: lc, Units: 1}
	if got != want {
		t.Errorf("reported %+v, want %+v (generic coordinates, one unit per INSERT)", got, want)
	}
}

func TestPlugin_OnQuery_PassesLogCommentThroughVerbatim(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(sink)

	// A backfill-marked log_comment must be forwarded byte-for-byte; the
	// plugin does NOT interpret watching (that is the host's job).
	const lc = `{"processor_id":"x2y2","watching":false}`
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", lc)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	if got.LogComment != lc {
		t.Errorf("LogComment = %q, want verbatim %q", got.LogComment, lc)
	}
}

func TestPlugin_OnQuery_EmptyLogCommentWhenAbsent(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(sink)
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	if got.LogComment != "" {
		t.Errorf("absent log_comment should yield empty LogComment, got %q", got.LogComment)
	}
}

func TestPlugin_OnQuery_ReportsEachINSERTDirectly(t *testing.T) {
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(sink)

	// Two INSERTs into one table + one into another. Housegate no longer
	// batches locally: each INSERT is its own 1-unit report. The host's
	// accumulator folds them back into per-key totals.
	for i := 0; i < 2; i++ {
		qctx := driverINSERT(sess, "x2y2_0", "counter_token", "")
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("OnQuery #%d: %v", i, err)
		}
	}
	qctx := driverINSERT(sess, "x2y2_0", "swap_event", "")
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
		folded[e.Table] += e.Units
	}
	if folded["counter_token"] != 2 {
		t.Errorf("counter_token total = %d, want 2", folded["counter_token"])
	}
	if folded["swap_event"] != 1 {
		t.Errorf("swap_event total = %d, want 1", folded["swap_event"])
	}
}

func TestPlugin_OnQuery_EmitsRegardlessOfDatabaseKind(t *testing.T) {
	// housegate does NOT read the registry, so it cannot (and must not)
	// decide whether a database is a processor DB — it emits for every
	// driver INSERT and lets the host drop non-processor / non-billable
	// destinations. A user-looking DB name still produces an entry.
	sess := newTestSession(t)
	sink := &fakeSink{}
	p := New(sink)
	qctx := driverINSERT(sess, "some_user_db", "things", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	got := wantOneEntry(t, sink)
	if got.LogicalDatabase != "some_user_db" || got.Table != "things" {
		t.Errorf("expected generic passthrough of db/table, got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsNonDriverSession(t *testing.T) {
	sess := newTestSession(t)
	// driverINSERT flips IsDriver; build manually instead.
	sink := &fakeSink{}
	p := New(sink)
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
	p := New(sink)
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

func TestPlugin_OnQuery_SkipsEmptyAccessedTables(t *testing.T) {
	sess := newTestSession(t)
	sess.State().SetIsDriver(true)
	sink := &fakeSink{}
	p := New(sink)
	qctx := &plugin.QueryContext{
		Session:        sess,
		Query:          &chproto.Query{},
		StatementType:  sqlmeta.StatementTypeInsert,
		AccessedTables: nil,
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("INSERT with no accessed tables should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_SkipsEmptyLogicalDatabase(t *testing.T) {
	sess := newTestSession(t)
	sess.State().SetIsDriver(true)
	sink := &fakeSink{}
	p := New(sink)
	qctx := &plugin.QueryContext{
		Session:       sess,
		Query:         &chproto.Query{},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: "x", OriginalTable: "t", LogicalDatabase: ""},
		},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("INSERT with empty LogicalDatabase should not report, got %+v", got)
	}
}

func TestPlugin_OnQuery_NilSinkIsNoOp(t *testing.T) {
	sess := newTestSession(t)
	p := New(nil)
	qctx := driverINSERT(sess, "x2y2_0", "counter_token", "")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery with nil sink should be a no-op, got err: %v", err)
	}
}

func TestPlugin_PeerTrustAndForward_OptOut(t *testing.T) {
	p := New(&fakeSink{})
	if p.RunOnPeerTrust() {
		t.Error("RunOnPeerTrust must be false to avoid double-counting peer-forwarded inserts")
	}
	if p.RunOnForward() {
		t.Error("RunOnForward must be false to keep the host proxy out of the metering loop")
	}
}
