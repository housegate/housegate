package materialize

import (
	"context"
	"errors"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/rewriter"
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

func TestOnQuery_RecordsOutcomeForTheInlineLane(t *testing.T) {
	cases := []struct {
		name string
		mat  *fakeMat
		want string
	}{
		{"applied", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (toDateTime(1))", Changed: true, Code: pb.MaterializeCode_MaterializeSuccess}}, "applied"},
		{"noop", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (1)", Code: pb.MaterializeCode_MaterializeSuccess}}, "noop"},
		{"call error", &fakeMat{err: errors.New("dial tcp: refused")}, "error:dial tcp: refused"},
		{"non success", &fakeMat{out: rewriter.MaterializeOutcome{SQL: "INSERT INTO t VALUES (1)", Code: pb.MaterializeCode_MaterializeSyntaxError, Message: "boom"}}, "error:MaterializeSyntaxError: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qctx := runOnQuery(t, &Plugin{Materializer: tc.mat}, "INSERT INTO t VALUES (1)")
			if got, ok := qctx.Values[plugin.ValuesKeyMaterialized].(string); !ok || got != tc.want {
				t.Fatalf("Values[%q] = %v, want %q", plugin.ValuesKeyMaterialized, qctx.Values[plugin.ValuesKeyMaterialized], tc.want)
			}
		})
	}
	// A disabled materializer must leave the key absent so the inline lane
	// refuses with "materialization did not run".
	if qctx := runOnQuery(t, &Plugin{}, "INSERT INTO t VALUES (1)"); qctx.Values[plugin.ValuesKeyMaterialized] != nil {
		t.Fatal("a nil Materializer must record nothing")
	}
}

type recordingMat struct{ calls []string }

func (m *recordingMat) Materialize(_ context.Context, sql string) (rewriter.MaterializeOutcome, error) {
	m.calls = append(m.calls, sql)
	return rewriter.MaterializeOutcome{SQL: sql + " /* materialized */", Changed: true, Code: pb.MaterializeCode_MaterializeSuccess}, nil
}

// A Scope that refuses a statement leaves it exactly as the client sent it:
// the materializer is not called and no outcome is recorded.
func TestOnQuery_ScopeLimitsTheRewrite(t *testing.T) {
	mat := &recordingMat{}
	p := &Plugin{Materializer: mat, Scope: func(sql string) bool { return sql == "INSERT INTO t VALUES (now())" }}
	out := runOnQuery(t, p, "SELECT rand() FROM numbers(3)")
	if out.Query.Body != "SELECT rand() FROM numbers(3)" || len(mat.calls) != 0 {
		t.Fatalf("out of scope: body=%q calls=%v; want untouched", out.Query.Body, mat.calls)
	}
	if _, ok := out.Values[plugin.ValuesKeyMaterialized]; ok {
		t.Fatal("an out-of-scope statement must record no materialize outcome")
	}
	in := runOnQuery(t, p, "INSERT INTO t VALUES (now())")
	if in.Query.Body != "INSERT INTO t VALUES (now()) /* materialized */" || len(mat.calls) != 1 {
		t.Fatalf("in scope: body=%q calls=%v; want materialized", in.Query.Body, mat.calls)
	}

	// Without a Scope every query is rewritten.
	all := &Plugin{Materializer: &recordingMat{}}
	if got := runOnQuery(t, all, "SELECT rand() FROM numbers(3)").Query.Body; got != "SELECT rand() FROM numbers(3) /* materialized */" {
		t.Fatalf("nil scope: body=%q; want materialized", got)
	}
}
