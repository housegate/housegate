package storageintegrity

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	hlog "housegate/housegate/pkg/log"
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
	p := New(testStorageConfig(), payloads, sink)
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
	if got, want := strings.Join(rec.PartitionIDs, ","), "202606"; got != want {
		t.Fatalf("PartitionIDs = %q, want %q", got, want)
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

func TestPluginUsesStaticRewriterForInsertUnsafeRewrite(t *testing.T) {
	rw := &recordingTableRewriter{
		outputSQL: "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` FORMAT Native",
	}
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		TableRewriter:     rw,
	}, nil, nil)
	sess := newFakeSession(410)
	qctx := &plugin.QueryContext{
		Session:      sess,
		OriginalSQL:  "INSERT INTO dual_hg_auth.t FORMAT Native",
		RewrittenSQL: "INSERT INTO dual_hg_phys.`dual_hg_auth.t` FORMAT Native",
		Query: &chproto.Query{
			ID:   "insert-qid-rewriter",
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

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := rw.inputSQL; got != "INSERT INTO dual_hg_phys.`dual_hg_auth.t` FORMAT Native" {
		t.Fatalf("rewriter input SQL = %q", got)
	}
	if got, want := rw.tableMap["dual_hg_phys.dual_hg_auth.t"], "hg_unsafe.dual_hg_auth.t_a"; got != want {
		t.Fatalf("rewriter table map = %v, want physical target -> %s", rw.tableMap, want)
	}
	if got, want := qctx.Query.Body, rw.outputSQL; got != want {
		t.Fatalf("query body = %q, want rewriter output %q", got, want)
	}
}

func TestPluginUsesStaticRewriterForSelectSafeRewrite(t *testing.T) {
	rw := &recordingTableRewriter{
		outputSQL: "SELECT * FROM `hg_safe`.`dual_hg_auth.t` WHERE id = 1",
	}
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		TableRewriter:     rw,
	}, nil, nil)
	qctx := &plugin.QueryContext{
		Session:      newFakeSession(411),
		OriginalSQL:  "SELECT * FROM dual_hg_auth.t WHERE id = 1",
		RewrittenSQL: "SELECT * FROM dual_hg_phys.`dual_hg_auth.t` WHERE id = 1",
		Query: &chproto.Query{
			Body: "SELECT * FROM dual_hg_phys.`dual_hg_auth.t` WHERE id = 1",
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
	if got, want := rw.tableMap["dual_hg_phys.dual_hg_auth.t"], "hg_safe.dual_hg_auth.t"; got != want {
		t.Fatalf("rewriter table map = %v, want physical target -> %s", rw.tableMap, want)
	}
	if got, want := qctx.Query.Body, rw.outputSQL; got != want {
		t.Fatalf("query body = %q, want rewriter output %q", got, want)
	}
}

func TestPluginRequiresTableRewriterForStorageIntegrityRewrite(t *testing.T) {
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(412),
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("OnQuery succeeded without table rewriter")
	}
	if !strings.Contains(err.Error(), "sql rewriter") {
		t.Fatalf("error = %v, want sql rewriter requirement", err)
	}
}

func TestPluginRequiresTableRewriterForSelectRewrite(t *testing.T) {
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(414),
		OriginalSQL: "SELECT * FROM realbin.t",
		Query: &chproto.Query{
			Body: "SELECT * FROM realbin.t",
		},
		StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "realbin",
			OriginalTable:    "t",
		}},
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("OnQuery succeeded without table rewriter")
	}
	if !strings.Contains(err.Error(), "rewrite SELECT to safe") {
		t.Fatalf("error = %v, want SELECT rewrite failure", err)
	}
}

func TestPluginRewritesSelectFromRawSQLWhenMetadataMissing(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(416),
		OriginalSQL: "SELECT concat(toString(count()), ':', groupArray(v)[1]) FROM realbin.t",
		Query: &chproto.Query{
			Body: "SELECT concat(toString(count()), ':', groupArray(v)[1]) FROM realbin.t",
		},
		StatementType: sqlmeta.StatementTypeSelect,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "SELECT concat(toString(count()), ':', groupArray(v)[1]) FROM `hg_safe`.`realbin.t`"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
}

func TestPluginRewritesUnclassifiedRawSelectWhenMetadataMissing(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(417),
		OriginalSQL: "SELECT count() FROM realbin.t",
		Query: &chproto.Query{
			Body: "SELECT count() FROM realbin.t",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "SELECT count() FROM `hg_safe`.`realbin.t`"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
	if qctx.StatementType != sqlmeta.StatementTypeSelect {
		t.Fatalf("StatementType = %s, want SELECT", qctx.StatementType)
	}
}

func TestPluginRejectsUnmaterializedNondeterministicInsert(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(415),
		OriginalSQL: "INSERT INTO realbin.t VALUES (rand())",
		Query: &chproto.Query{
			Body: "INSERT INTO realbin.t VALUES (rand())",
		},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("OnQuery succeeded with unmaterialized rand()")
	}
	if !strings.Contains(err.Error(), "non-deterministic") {
		t.Fatalf("error = %v, want non-deterministic rejection", err)
	}
}

func TestPluginRewritesInsertFromRawSQLWhenMetadataMissing(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(44),
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "INSERT INTO `hg_unsafe`.`realbin.t_a` VALUES (1, 'a')"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
}

func TestPluginRewritesUnclassifiedRawInsertWithTableRewriter(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(45),
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "INSERT INTO `hg_unsafe`.`realbin.t_a` VALUES (1, 'a')"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
	if qctx.StatementType != sqlmeta.StatementTypeInsert {
		t.Fatalf("StatementType = %s, want INSERT", qctx.StatementType)
	}
	if len(qctx.AccessedTables) != 1 {
		t.Fatalf("AccessedTables len = %d, want 1", len(qctx.AccessedTables))
	}
	if got := qctx.AccessedTables[0]; got.LogicalDatabase != "realbin" || got.OriginalTable != "t" {
		t.Fatalf("AccessedTables[0] = %+v, want realbin.t", got)
	}
}

func TestPluginFallsBackWhenSqlRewriterDoesNotRewriteInsertTarget(t *testing.T) {
	cfg := testStorageConfig()
	cfg.TableRewriter = fixedTableRewriter{sql: "INSERT INTO realbin.t FORMAT Values"}
	p := New(cfg, nil, nil)
	qctx := &plugin.QueryContext{
		Session:     newFakeSession(45),
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got, want := qctx.Query.Body, "INSERT INTO `hg_unsafe`.`realbin.t_a` VALUES (1, 'a')"; got != want {
		t.Fatalf("rewritten SQL = %q, want %q", got, want)
	}
}

func TestPluginSubmitsInsertAfterQueryCompleteWhenClientDataTerminatorSeen(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	sink := &recordingIngressSink{}
	p := New(testStorageConfig(), payloads, sink)
	sess := newFakeSession(46)
	sess.state.ClientRevision = 54453
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			ID:   "insert-qid-terminator",
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientData(ctx, qctx, emptyClientDataPacket()); err != nil {
		t.Fatalf("OnClientData terminator: %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("ingress records after terminator = %d, want 0 before query complete", len(sink.records))
	}
	p.OnQueryComplete(ctx, sess)
	if len(sink.records) != 1 {
		t.Fatalf("ingress records after OnQueryComplete = %d, want 1", len(sink.records))
	}
}

func TestPluginSubmitsInsertAfterClientDataTerminatorDelay(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	sink := &channelIngressSink{records: make(chan core.InsertRecord, 1)}
	p := New(testStorageConfig(), payloads, sink)
	p.terminatorDelay = time.Millisecond
	sess := newFakeSession(47)
	sess.state.ClientRevision = 54453
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			ID:   "insert-qid-terminator-delay",
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientData(ctx, qctx, emptyClientDataPacket()); err != nil {
		t.Fatalf("OnClientData terminator: %v", err)
	}

	select {
	case rec := <-sink.records:
		if rec.StatementID != "insert-qid-terminator-delay" {
			t.Fatalf("StatementID = %q", rec.StatementID)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for delayed terminator submission")
	}
}

func TestPluginSubmitsInsertOnCloseAfterClientDataTerminator(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}
	sink := &channelIngressSink{records: make(chan core.InsertRecord, 1)}
	p := New(testStorageConfig(), payloads, sink)
	p.terminatorDelay = time.Hour
	sess := newFakeSession(48)
	sess.state.ClientRevision = 54453
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "INSERT INTO realbin.t VALUES (1, 'a')",
		Query: &chproto.Query{
			ID:   "insert-qid-terminator-close",
			Body: "INSERT INTO realbin.t VALUES (1, 'a')",
		},
		StatementType: sqlmeta.StatementTypeUnspecified,
	}

	if err := p.OnQuery(ctx, qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientData(ctx, qctx, emptyClientDataPacket()); err != nil {
		t.Fatalf("OnClientData terminator: %v", err)
	}
	p.OnClose(sess)

	select {
	case rec := <-sink.records:
		if rec.StatementID != "insert-qid-terminator-close" {
			t.Fatalf("StatementID = %q", rec.StatementID)
		}
	default:
		t.Fatalf("OnClose after terminator did not submit insert")
	}
}

func TestPluginRewritesSelectToSafe(t *testing.T) {
	p := New(testStorageConfig(), nil, nil)
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
	p := New(testStorageConfig(), payloads, sink)
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

func TestPluginDoesNotLogSubmittedWhenSinkRejects(t *testing.T) {
	ctx := context.Background()
	payloads, err := core.NewMockPayloadStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMockPayloadStore: %v", err)
	}

	var buf bytes.Buffer
	prev := hlog.Default()
	hlog.SetDefault(hlog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer hlog.SetDefault(prev)

	p := New(testStorageConfig(), payloads, failingIngressSink{err: errors.New("keeper rejected")})

	p.finalizeCapture(ctx, &insertCapture{
		tableID:     "dual_hg_auth.t",
		statementID: "insert-qid-rejected",
		originalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		unsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		unsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		safeTable:   "`hg_safe`.`dual_hg_auth.t`",
		dataPackets: [][]byte{[]byte("native-block")},
	})

	out := buf.String()
	if !strings.Contains(out, "storage_integrity: submit insert failed") {
		t.Fatalf("log output missing submit failure: %s", out)
	}
	if strings.Contains(out, "storage_integrity: insert submitted") {
		t.Fatalf("log output contains success after submit failure: %s", out)
	}
}

type recordingIngressSink struct {
	records []core.InsertRecord
}

func testStorageConfig() Config {
	return Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		TableRewriter:     simpleTableRewriter{},
		PartitionIDs:      []string{"202606"},
	}
}

type recordingTableRewriter struct {
	inputSQL  string
	tableMap  map[string]string
	outputSQL string
}

func (r *recordingTableRewriter) RewriteTables(_ context.Context, sql string, tableMap map[string]string) (string, error) {
	r.inputSQL = sql
	r.tableMap = tableMap
	return r.outputSQL, nil
}

type simpleTableRewriter struct{}

type fixedTableRewriter struct {
	sql string
}

func (r fixedTableRewriter) RewriteTables(context.Context, string, map[string]string) (string, error) {
	return r.sql, nil
}

func (simpleTableRewriter) RewriteTables(_ context.Context, sql string, tableMap map[string]string) (string, error) {
	out := sql
	for from, to := range tableMap {
		out = replaceTargetVariant(out, from, to)
	}
	return out, nil
}

func replaceTargetVariant(sql, from, to string) string {
	toSQL := quoteRawTarget(to)
	for _, candidate := range rawTargetVariants(from) {
		if strings.Contains(sql, candidate) {
			return strings.Replace(sql, candidate, toSQL, 1)
		}
	}
	return sql
}

func rawTargetVariants(raw string) []string {
	db, table := splitRawTarget(raw)
	if db == "" {
		return []string{table, core.QuoteIdentifier(table)}
	}
	return []string{
		db + "." + table,
		db + "." + core.QuoteIdentifier(table),
		core.QuoteIdentifier(db) + "." + table,
		core.QuoteIdentifier(db) + "." + core.QuoteIdentifier(table),
	}
}

func quoteRawTarget(raw string) string {
	db, table := splitRawTarget(raw)
	if db == "" {
		return core.QuoteIdentifier(table)
	}
	return core.QuoteTable(db, table)
}

func splitRawTarget(raw string) (string, string) {
	if idx := strings.IndexByte(raw, '.'); idx >= 0 {
		return raw[:idx], raw[idx+1:]
	}
	return "", raw
}

func (s *recordingIngressSink) SubmitInsert(_ context.Context, rec core.InsertRecord) error {
	s.records = append(s.records, rec)
	return nil
}

type failingIngressSink struct {
	err error
}

func (s failingIngressSink) SubmitInsert(context.Context, core.InsertRecord) error {
	return s.err
}

type channelIngressSink struct {
	records chan core.InsertRecord
}

func (s *channelIngressSink) SubmitInsert(_ context.Context, rec core.InsertRecord) error {
	s.records <- rec
	return nil
}

func emptyClientDataPacket() []byte {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	(&proto.BlockInfo{BucketNum: -1}).Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	return append([]byte(nil), buf.Buf...)
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
