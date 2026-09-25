package sitablestate

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

type versionRecordingRewriter struct{ seen []uint64 }

func (r *versionRecordingRewriter) Rewrite(ctx context.Context, sql, _ string) (rewriter.RewriteResult, error) {
	if snap, ok := rewriter.TableSnapshotFromContext(ctx); ok {
		r.seen = append(r.seen, snap.Version())
	}
	return rewriter.RewriteResult{
		SQL:                             sql,
		StatementType:                   sqlmeta.StatementTypeSelect,
		AccessedTables:                  accessed("db1.t"),
		StorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2,
	}, nil
}
func (r *versionRecordingRewriter) RewriteErrorMessage(_ context.Context, m string) (string, error) {
	return m, nil
}
func (r *versionRecordingRewriter) Close() error { return nil }

type oneRewriterFactory struct{ rw rewriter.Rewriter }

func (f oneRewriterFactory) NewRewriter(rewriter.Session) rewriter.Rewriter { return f.rw }
func (f oneRewriterFactory) Close() error                                   { return nil }

// hookFunc adapts a function into a QueryPlugin.
type hookFunc func(*plugin.QueryContext)

func (h hookFunc) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h(qctx)
	return nil
}

// TestOneSnapshotPerQuery is spec 2026-09-24 §11.2: the fake changes version
// between two hooks of one query, and every stage still observes the version
// the rewrite plugin took. The table is Active in version 1 and Gone in
// version 2, so a stage that re-read Current() would refuse the query.
func TestOneSnapshotPerQuery(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	rw := &versionRecordingRewriter{}
	var later []uint64
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{
		&rewrite.Plugin{Factory: oneRewriterFactory{rw: rw}, TableState: fake, RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2},
		hookFunc(func(*plugin.QueryContext) { fake.Set(sitable.Table{ID: "db1.t", Status: sitable.Gone}) }),
		&Plugin{},
		hookFunc(func(qctx *plugin.QueryContext) { later = append(later, qctx.TableSnapshot.Version()) }),
	}}
	qctx := &plugin.QueryContext{Session: newSession(t, 20), OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	if err := chain.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("a query that started under version 1 must finish under it: %v", err)
	}
	if len(rw.seen) != 1 || rw.seen[0] != 1 || len(later) != 1 || later[0] != 1 {
		t.Fatalf("rewriter saw %v, later stage saw %v, want [1] and [1]", rw.seen, later)
	}
	if fake.Current().Version() != 2 {
		t.Fatalf("the fake must have moved to version 2, got %d", fake.Current().Version())
	}
	next := &plugin.QueryContext{Session: newSession(t, 21), OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
	chain.QueryPlugins = []plugin.QueryPlugin{chain.QueryPlugins[0], &Plugin{}}
	check(t, "the next query sees version 2", chain.OnQuery(context.Background(), next), unknownErr("db1.t"))
}

// TestEverySessionTheGateRunsForHasASnapshot pins the controller ruling that a
// nil snapshot fails closed: rewrite and sitablestate share the chain filters
// (neither is RouteAware, both opt out of peer trust and origin-side
// forwarding), so every session kind either skips both or reaches sitablestate
// with the snapshot rewrite took. db1.t is Gone, so a session the gate decides
// answers code 60 and a skipped or bypassed one passes; none may see the
// "table state is unavailable" refusal.
func TestEverySessionTheGateRunsForHasASnapshot(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Gone})
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{
		&rewrite.Plugin{Factory: oneRewriterFactory{rw: &versionRecordingRewriter{}}, TableState: fake, RequiredStorageIntegrityContractVersion: rewriter.StorageIntegrityContractV2},
		&Plugin{},
	}}
	for i, tc := range []struct {
		name  string
		setup func(chsession.Session)
		want  outcome
	}{
		{"ordinary", func(chsession.Session) {}, unknownErr("db1.t")},
		{"maintenance", func(s chsession.Session) { s.State().SetMaintenance(true) }, ok},
		{"platform operator", func(s chsession.Session) { s.State().SetPlatformOperator(true) }, ok},
		{"peer-trusted", func(s chsession.Session) { s.State().SetPeerTrust("10.0.0.7:9001") }, ok},
		{"origin-side forwarding", func(s chsession.Session) { s.State().SetForwarding(true) }, ok},
		{"forwarded from peer", func(s chsession.Session) { s.State().SetPeerTrustForwarded("10.0.0.7:9001", true) }, unknownErr("db1.t")},
		{"routed", func(s chsession.Session) { s.State().SetRouteTarget("10.0.0.8:9000") }, ok},
		{"routed and peer-trusted", func(s chsession.Session) {
			s.State().SetRouteTarget("10.0.0.8:9000")
			s.State().SetPeerTrust("10.0.0.7:9001")
		}, ok},
	} {
		sess := newSession(t, int64(30+i))
		tc.setup(sess)
		qctx := &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT a FROM db1.t", Query: &chproto.Query{Body: "SELECT a FROM db1.t"}}
		check(t, tc.name, chain.OnQuery(context.Background(), qctx), tc.want)
	}
}
