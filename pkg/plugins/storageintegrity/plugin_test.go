package storageintegrity

import (
	"context"
	"net"
	"strings"
	"testing"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/sqlmeta"
	core "housegate/housegate/pkg/storageintegrity"
)

func TestPluginRewritesInsertToUnsafeAndSubmitsPayload(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	sink := &recordingIngressSink{}
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, payloads, sink)
	sess := newFakeSession(41)
	qctx := &plugin.QueryContext{
		Session:      sess,
		OriginalSQL:  "INSERT INTO dual_hg_auth.t FORMAT Native",
		RewrittenSQL: "INSERT INTO dual_hg_phys.`dual_hg_auth.t` FORMAT Native",
		Query: &chproto.Query{
			ID:   "insert-qid-1",
			Body: "INSERT INTO dual_hg_phys.`dual_hg_auth.t` FORMAT Native",
		},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalTable:    "t",
			LogicalDatabase:  "dual_hg_auth",
			PhysicalDatabase: "dual_hg_phys",
		}},
		TableRewrites: map[string]string{
			"dual_hg_auth.t": "dual_hg_phys.`dual_hg_auth.t`",
		},
	}

	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` FORMAT Native"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}

	if err := p.OnClientData(ctx, qctx, []byte("native-block-1")); err != nil {
		t.Fatalf("OnClientData #1: %v", err)
	}
	if err := p.OnClientData(ctx, qctx, []byte("native-block-2")); err != nil {
		t.Fatalf("OnClientData #2: %v", err)
	}
	p.OnQueryComplete(ctx, sess)

	if len(sink.records) != 1 {
		t.Fatalf("ingress records = %d, want 1", len(sink.records))
	}
	rec := sink.records[0]
	if rec.TableID != "dual_hg_auth.t" {
		t.Fatalf("TableID = %q, want dual_hg_auth.t", rec.TableID)
	}
	if rec.StatementID != "insert-qid-1" {
		t.Fatalf("StatementID = %q, want insert-qid-1", rec.StatementID)
	}
	if rec.UnsafeTable != "`hg_unsafe`.`dual_hg_auth.t_a`" {
		t.Fatalf("UnsafeTable = %q", rec.UnsafeTable)
	}
	if rec.SafeTable != "`hg_safe`.`dual_hg_auth.t`" {
		t.Fatalf("SafeTable = %q", rec.SafeTable)
	}
	if rec.Payload.Hash == "" || rec.Payload.Ref == "" || rec.Payload.Length == 0 {
		t.Fatalf("payload commitment is incomplete: %+v", rec.Payload)
	}
	body, err := payloads.GetPayload(ctx, rec.Payload.Ref)
	if err != nil {
		t.Fatalf("GetPayload: %v", err)
	}
	if !strings.Contains(string(body), "native-block-1") || !strings.Contains(string(body), "native-block-2") {
		t.Fatalf("stored payload did not include data blocks: %s", body)
	}
}

func TestPluginRewritesSelectToSafe(t *testing.T) {
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, nil, nil)
	qctx := &plugin.QueryContext{
		Session:      newFakeSession(42),
		OriginalSQL:  "SELECT count() FROM dual_hg_auth.t",
		RewrittenSQL: "SELECT count() FROM dual_hg_phys.`dual_hg_auth.t`",
		Query: &chproto.Query{
			Body: "SELECT count() FROM dual_hg_phys.`dual_hg_auth.t` WHERE value > 0",
		},
		StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalTable:    "t",
			LogicalDatabase:  "dual_hg_auth",
			PhysicalDatabase: "dual_hg_phys",
		}},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "SELECT count() FROM `hg_safe`.`dual_hg_auth.t` WHERE value > 0"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
}

func TestPluginDropsCaptureOnException(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	sink := &recordingIngressSink{}
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, payloads, sink)
	sess := newFakeSession(43)
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		Query: &chproto.Query{
			ID:   "insert-qid-failed",
			Body: "INSERT INTO dual_hg_phys.`dual_hg_auth.t` VALUES (1)",
		},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalTable:   "t",
			LogicalDatabase: "dual_hg_auth",
		}},
	}
	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnException(ctx, sess, &chproto.Exception{Message: "unknown table"}); err != nil {
		t.Fatalf("OnException: %v", err)
	}
	p.OnQueryComplete(ctx, sess)
	if len(sink.records) != 0 {
		t.Fatalf("ingress records = %d, want 0 after exception", len(sink.records))
	}
}

type recordingIngressSink struct {
	records []core.InsertRecord
}

func (s *recordingIngressSink) SubmitInsert(_ context.Context, rec core.InsertRecord) error {
	s.records = append(s.records, rec)
	return nil
}

type fakeSession struct {
	id    int64
	state *chsession.SessionState
}

func newFakeSession(id int64) *fakeSession {
	return &fakeSession{id: id, state: chsession.NewSessionState()}
}

func (s *fakeSession) ID() int64 { return s.id }

func (s *fakeSession) State() *chsession.SessionState { return s.state }

func (s *fakeSession) Client() *chproto.Codec { return nil }

func (s *fakeSession) Upstream() *chproto.Codec { return nil }

func (s *fakeSession) RemoteAddr() net.Addr { return nil }

func (s *fakeSession) Close() error { return nil }

func (s *fakeSession) BindUpstream(context.Context, *chproto.Codec) error { return nil }

func (s *fakeSession) RebindUpstream(context.Context, *chproto.Codec, bool) error { return nil }

func (s *fakeSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

func (s *fakeSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

var _ chsession.Session = (*fakeSession)(nil)
