package rewrite

import (
	"context"
	"net"
	"testing"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/rewriter"
)

func TestPlugin_RunOnForward_False(t *testing.T) {
	var p plugin.ForwardAware = (*Plugin)(nil)
	if p.RunOnForward() {
		t.Errorf("rewrite must opt out of forwarded sessions")
	}
}

// fakeRewriter is a test-only rewriter.Rewriter that records the number
// of Rewrite calls and returns a fixed canned SQL. The canned SQL is
// distinct from any input the tests pass in so a regression that lets
// the rewriter run when it should be skipped will be visible as a
// changed RewrittenSQL.
type fakeRewriter struct {
	out                  string
	rewriteCalls         int
	errMsgCalls          int
	lastEffectiveAccount string
}

func (f *fakeRewriter) Rewrite(_ context.Context, _, effectiveAccount string) (rewriter.RewriteResult, error) {
	f.rewriteCalls++
	f.lastEffectiveAccount = effectiveAccount
	return rewriter.RewriteResult{SQL: f.out}, nil
}

func (f *fakeRewriter) RewriteErrorMessage(_ context.Context, message string) (string, error) {
	f.errMsgCalls++
	return message, nil
}

func (f *fakeRewriter) Close() error { return nil }

// fakeFactory hands the same fakeRewriter to every NewRewriter call so a
// single test can assert against its call counters regardless of how
// many times the plugin lazily constructs (or re-constructs) the
// Rewriter.
type fakeFactory struct {
	rw *fakeRewriter
}

func (f *fakeFactory) NewRewriter(_ rewriter.Session) rewriter.Rewriter { return f.rw }
func (f *fakeFactory) Close() error                                     { return nil }

func newSessionForTest(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

// TestOnQuery_MaintenanceSkipsRewrite verifies that when the session's
// SessionState.Maintenance flag is true, OnQuery forwards the original
// SQL verbatim and never invokes the underlying rewriter. Indexer-
// signed maintenance traffic carries fully-qualified physical CH names
// already (sentio-node DatabaseGC drops, etc.) and must bypass the AST
// rewriter.
func TestOnQuery_MaintenanceSkipsRewrite(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}

	sess := newSessionForTest(t, 1)
	sess.State().SetMaintenance(true)

	const original = "DROP TABLE testnet.`db.table`"
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: original,
		Query:       &chproto.Query{Body: original},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if qctx.RewrittenSQL != original {
		t.Errorf("RewrittenSQL=%q, want %q (maintenance must forward verbatim)",
			qctx.RewrittenSQL, original)
	}
	if qctx.Query.Body != original {
		t.Errorf("Query.Body=%q, want %q (maintenance must not mutate query body)",
			qctx.Query.Body, original)
	}
	if rw.rewriteCalls != 0 {
		t.Errorf("rewriter.Rewrite calls=%d, want 0 (maintenance must skip rewriter)",
			rw.rewriteCalls)
	}
}

// TestOnQuery_IsDriverStillRunsRewriter verifies that an indexer-driver
// session (SessionState.IsDriver == true) does NOT bypass rewrite. This
// is the defining behavioural difference between the IsDriver bypass
// and the Maintenance bypass: driver traffic still needs the rewriter
// because the driver writes logical names (CREATE DATABASE x2y2_0 /
// INSERT INTO x2y2_0.events) and depends on logical→physical
// translation to land on the right table. A regression that adds
// IsDriver to this plugin's bypass condition would break driver
// writes; this test locks the behaviour down.
func TestOnQuery_IsDriverStillRunsRewriter(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}

	sess := newSessionForTest(t, 3)
	sess.State().SetIsDriver(true)

	const original = "INSERT INTO x2y2_0.events FORMAT Native"
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: original,
		Query:       &chproto.Query{Body: original},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if rw.rewriteCalls != 1 {
		t.Errorf("rewriter.Rewrite calls=%d, want 1 (IsDriver MUST still invoke rewriter)",
			rw.rewriteCalls)
	}
	if qctx.RewrittenSQL != "REWRITTEN-SQL" {
		t.Errorf("RewrittenSQL=%q, want %q (IsDriver must NOT forward verbatim)",
			qctx.RewrittenSQL, "REWRITTEN-SQL")
	}
}

// TestOnQuery_NonMaintenanceRunsRewriter is the negative control: with
// Maintenance=false the rewriter must still run. Without this, a bug
// that always-skips rewriting would also satisfy the maintenance test.
func TestOnQuery_NonMaintenanceRunsRewriter(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}

	sess := newSessionForTest(t, 2)
	// Maintenance left at default false.

	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "SELECT 1",
		Query:       &chproto.Query{Body: "SELECT 1"},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	if rw.rewriteCalls != 1 {
		t.Errorf("rewriter.Rewrite calls=%d, want 1 (non-maintenance must invoke rewriter)",
			rw.rewriteCalls)
	}
	if qctx.RewrittenSQL != "REWRITTEN-SQL" {
		t.Errorf("RewrittenSQL=%q, want %q", qctx.RewrittenSQL, "REWRITTEN-SQL")
	}
}
