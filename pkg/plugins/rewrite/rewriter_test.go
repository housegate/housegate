package rewrite

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/rewriter"
)

func TestPlugin_RunOnForward_False(t *testing.T) {
	var p plugin.ForwardAware = (*Plugin)(nil)
	if p.RunOnForward() {
		t.Errorf("rewrite must opt out of forwarded sessions")
	}
}

func TestPlugin_RejectUndecodableQueryFollowsConfiguredSIPolicy(t *testing.T) {
	p := &Plugin{}
	var strict plugin.StrictQueryDecodePlugin = p
	if strict.RejectUndecodableQuery() {
		t.Fatal("empty-SI rewrite plugin must retain the legacy decode fallback")
	}
	p.FailClosedOnError = true
	if !strict.RejectUndecodableQuery() {
		t.Fatal("configured-SI rewrite plugin must reject undecodable Query packets")
	}
}

// fakeRewriter is a test-only rewriter.Rewriter that records the number
// of Rewrite calls and returns a fixed canned SQL. The canned SQL is
// distinct from any input the tests pass in so a regression that lets
// the rewriter run when it should be skipped will be visible as a
// changed RewrittenSQL.
type fakeRewriter struct {
	out                             string
	err                             error
	rewriteCalls                    int
	errMsgCalls                     int
	lastCtx                         context.Context
	lastEffectiveAccount            string
	storageIntegrityContractVersion pb.StorageIntegrityContractVersion
}

func (f *fakeRewriter) Rewrite(ctx context.Context, _, effectiveAccount string) (rewriter.RewriteResult, error) {
	f.rewriteCalls++
	f.lastCtx = ctx
	f.lastEffectiveAccount = effectiveAccount
	if f.err != nil {
		return rewriter.RewriteResult{}, f.err
	}
	return rewriter.RewriteResult{SQL: f.out, StorageIntegrityContractVersion: f.storageIntegrityContractVersion}, nil
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

func TestOnQuery_ReadModeSettingIsCarriedInContext(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 40)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1",
		Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: rewriter.ReadModeSettingKey, Value: "'unsafe_latest'", Custom: true}}}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatal(err)
	}
	if m, ok := rewriter.ReadModeFromContext(rw.lastCtx); !ok || m != rewriter.ReadModeUnsafeLatest {
		t.Fatalf("ctx read mode = %q %v", m, ok)
	}
	// Setting is left in place (D-3), like every other SQL_x_* key.
	if len(qctx.Query.Settings) != 1 {
		t.Fatalf("settings must not be stripped: %v", qctx.Query.Settings)
	}
}

func TestOnQuery_InvalidReadModeIsAnException(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN"}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 41)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1",
		Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: rewriter.ReadModeSettingKey, Value: "latest"}}}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "SQL_x_read_mode") {
		t.Fatalf("err = %v, want invalid-mode rejection", err)
	}
	if rw.rewriteCalls != 0 {
		t.Fatal("rewriter must not run on an invalid mode")
	}
}

func TestOnQuery_RejectedErrorFailsClosed(t *testing.T) {
	rw := &fakeRewriter{err: &rewriter.RejectedError{Code: pb.RewriteCode_RewriteError, Message: "reserved column _hg_row_id is not addressable"}}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 42)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT _hg_row_id FROM db1.t", Query: &chproto.Query{Body: "SELECT _hg_row_id FROM db1.t"}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "reserved column _hg_row_id") {
		t.Fatalf("err = %v, want the rewriter's rejection surfaced", err)
	}
}

func TestOnQuery_OrdinaryErrorStaysFailOpenWhenNoSISurfaceIsConfigured(t *testing.T) {
	rw := &fakeRewriter{err: errors.New("transport down")}
	p := &Plugin{Factory: &fakeFactory{rw: rw}}
	sess := newSessionForTest(t, 43)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("ordinary rewriter errors must stay fail-open: %v", err)
	}
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body = %q", qctx.Query.Body)
	}
}

func TestOnQuery_OrdinaryErrorFailsClosedWhenSISurfaceIsConfigured(t *testing.T) {
	rw := &fakeRewriter{err: errors.New("transport down")}
	p := &Plugin{Factory: &fakeFactory{rw: rw}, FailClosedOnError: true}
	sess := newSessionForTest(t, 44)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "INSERT INTO db1.t FORMAT Native", Query: &chproto.Query{Body: "INSERT INTO db1.t FORMAT Native"}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "classification unavailable") {
		t.Fatalf("err = %v, want configured-SI fail-closed error", err)
	}
}

func TestOnQuery_MissingContractAcknowledgementFromCustomRewriterFailsClosed(t *testing.T) {
	rw := &fakeRewriter{out: "SELECT 1"} // nil error, zero acknowledgement
	p := &Plugin{Factory: &fakeFactory{rw: rw},
		RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV1}
	sess := newSessionForTest(t, 45)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "contract acknowledgement") {
		t.Fatalf("err = %v, want missing-ack rejection", err)
	}
}

func TestOnQuery_WrongContractAcknowledgementFromCustomRewriterFailsClosed(t *testing.T) {
	rw := &fakeRewriter{out: "SELECT 1", storageIntegrityContractVersion: pb.StorageIntegrityContractVersion(99)}
	p := &Plugin{Factory: &fakeFactory{rw: rw},
		RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV1}
	sess := newSessionForTest(t, 46)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "contract acknowledgement") {
		t.Fatalf("err = %v, want wrong-ack rejection", err)
	}
}

func TestOnQuery_AcknowledgedCustomRewriterAllowsNormalQuery(t *testing.T) {
	rw := &fakeRewriter{out: "SELECT 1", storageIntegrityContractVersion: rewriter.StorageIntegrityContractV1}
	p := &Plugin{Factory: &fakeFactory{rw: rw},
		RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV1}
	sess := newSessionForTest(t, 47)
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("acknowledged normal query: %v", err)
	}
}

func TestOnException_ScrubsStorageIntegrityNames(t *testing.T) {
	rw := &fakeRewriter{out: "REWRITTEN-SQL"}
	p := &Plugin{
		Factory: &fakeFactory{rw: rw},
		StorageIntegrityScrubber: rewriter.NewStorageIntegrityScrubber(rewriter.StorageIntegrityOptions{
			Tables: []rewriter.StorageIntegrityTable{
				{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			}}),
	}
	sess := newSessionForTest(t, 48)

	exc := &chproto.Exception{Message: "Unknown identifier _hg_row_id in table hg_safe.db1__t"}
	if err := p.OnException(context.Background(), sess, exc); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	if strings.Contains(exc.Message, "hg_safe") || strings.Contains(exc.Message, "_hg_row_id") {
		t.Fatalf("protocol-owned names leaked to the client: %q", exc.Message)
	}
	if !strings.Contains(exc.Message, "db1.t") {
		t.Fatalf("the logical name must survive scrubbing: %q", exc.Message)
	}
}
