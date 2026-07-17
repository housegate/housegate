package materialize

import (
	"context"
	"errors"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/rewriter"
)

type fakeMat struct {
	out rewriter.MaterializeOutcome
	err error
}

func (f *fakeMat) Materialize(_ context.Context, _ string) (rewriter.MaterializeOutcome, error) {
	return f.out, f.err
}

type fakeObs struct {
	applied, noop, callErr int
	nonSuccess             []string
}

func (o *fakeObs) MaterializeApplied()               { o.applied++ }
func (o *fakeObs) MaterializeNoop()                  { o.noop++ }
func (o *fakeObs) MaterializeNonSuccess(code string) { o.nonSuccess = append(o.nonSuccess, code) }
func (o *fakeObs) MaterializeCallError()             { o.callErr++ }

func runOnQuery(t *testing.T, p *Plugin, body string) *plugin.QueryContext {
	t.Helper()
	qctx := &plugin.QueryContext{Query: &chproto.Query{Body: body}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery must never return an error (fail-open), got %v", err)
	}
	return qctx
}

func TestOnQuery_AppliedSwapsBody(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "INSERT INTO t VALUES ('2026-07-01 00:00:00')", Changed: true,
		Code: pb.MaterializeCode_MaterializeSuccess,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "INSERT INTO t VALUES (now())")
	if qctx.Query.Body != "INSERT INTO t VALUES ('2026-07-01 00:00:00')" {
		t.Fatalf("body not swapped: %q", qctx.Query.Body)
	}
	if obs.applied != 1 {
		t.Fatalf("applied metric = %d, want 1", obs.applied)
	}
}

func TestOnQuery_NoopLeavesBody(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "SELECT 1", Changed: false, Code: pb.MaterializeCode_MaterializeSuccess,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body changed on noop: %q", qctx.Query.Body)
	}
	if obs.noop != 1 {
		t.Fatalf("noop metric = %d, want 1", obs.noop)
	}
}

func TestOnQuery_NonSuccessFailsOpen(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{out: rewriter.MaterializeOutcome{
		SQL: "NOT SQL", Changed: false, Code: pb.MaterializeCode_MaterializeSyntaxError,
	}}, Observer: obs}
	qctx := runOnQuery(t, p, "NOT SQL")
	if qctx.Query.Body != "NOT SQL" {
		t.Fatalf("body changed on non-success: %q", qctx.Query.Body)
	}
	if len(obs.nonSuccess) != 1 || obs.nonSuccess[0] != "MaterializeSyntaxError" {
		t.Fatalf("nonSuccess metric = %v, want [MaterializeSyntaxError]", obs.nonSuccess)
	}
}

func TestOnQuery_CallErrorFailsOpen(t *testing.T) {
	obs := &fakeObs{}
	p := &Plugin{Materializer: &fakeMat{err: errors.New("grpc down")}, Observer: obs}
	qctx := runOnQuery(t, p, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("body changed on call error: %q", qctx.Query.Body)
	}
	if obs.callErr != 1 {
		t.Fatalf("callErr metric = %d, want 1", obs.callErr)
	}
}

func TestOnQuery_NilMaterializerNoop(t *testing.T) {
	qctx := runOnQuery(t, &Plugin{}, "SELECT 1")
	if qctx.Query.Body != "SELECT 1" {
		t.Fatalf("nil materializer must be a clean no-op")
	}
}
