package sistatement

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakeEvaluator struct {
	blocks [][]proto.InputColumn
	err    error
	seen   ValuesEvaluation
	calls  int
}

func (f *fakeEvaluator) Evaluate(_ context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error) {
	f.calls++
	f.seen = req
	return f.blocks, f.err
}

// evaluatedBlock builds one row for testSchema (id UInt64, region String, amount Float64).
func evaluatedBlock(id uint64, region string, amount float64) []proto.InputColumn {
	i, r, a := &proto.ColUInt64{}, &proto.ColStr{}, &proto.ColFloat64{}
	i.Append(id)
	r.Append(region)
	a.Append(amount)
	return []proto.InputColumn{{Name: "id", Data: i}, {Name: "region", Data: r}, {Name: "amount", Data: a}}
}

func inlineOptions(t *testing.T, ev ValuesEvaluator, declare bool) (Options, *SeqCounter) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	if declare {
		declareSchema(t, ns, testSchema())
	}
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	return Options{
		Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Evaluator: ev,
		InlineValues: InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000},
	}, seq
}

func newInlinePlugin(t *testing.T, ev ValuesEvaluator) (*Plugin, *SeqCounter) {
	t.Helper()
	opts, seq := inlineOptions(t, ev, true)
	p, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, seq
}

func inlineQctx(sess *fakeSession, sql string) *plugin.QueryContext {
	qctx := insertQctx(sess, sql)
	client, server := net.Pipe()
	_ = server.Close()
	sess.upstream = chproto.NewCodec(client, chproto.DirToUpstream)
	sess.upstream.SetRevision(testRevision)
	sess.state.SetUpstreamHello(&chproto.ClientHello{ProtocolVersion: testRevision, User: "writer", Database: "shop"})
	qctx.Values[plugin.ValuesKeyMaterialized] = "noop"
	return qctx
}

func TestNew_InlineValuesOptionsAreValidated(t *testing.T) {
	cases := []struct {
		name, wantErr string
		mutate        func(*Options)
	}{
		{"no evaluator", "values evaluator is required", func(o *Options) { o.Evaluator = nil }},
		{"timeout too small", "evaluation timeout", func(o *Options) { o.InlineValues.EvaluationTimeout = time.Millisecond }},
		{"zero max rows", "max rows", func(o *Options) { o.InlineValues.MaxRows = 0 }},
		{"disabled needs no evaluator", "", func(o *Options) { o.Evaluator, o.InlineValues = nil, InlineValuesOptions{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := inlineOptions(t, &fakeEvaluator{}, false)
			tc.mutate(&opts)
			_, err := New(opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("New err = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestPlugin_InlineValuesInstallsSynthesizedPlan(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5), evaluatedBlock(2, "us", 2.5)}}
	p, seq := newInlinePlugin(t, ev)
	sql := "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5), (2, 'us', 2.5)"
	qctx := inlineQctx(newSession(7, ""), sql)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.DeferredInsert != nil {
		t.Fatal("the inline lane must not install a deferred plan")
	}
	plan := qctx.SynthesizedInsert
	if plan == nil {
		t.Fatal("SynthesizedInsert is nil")
	}
	if want := "INSERT INTO shop.orders (`id`, `region`, `amount`) FORMAT Native"; qctx.Query.Body != want {
		t.Fatalf("rewritten body = %q, want %q", qctx.Query.Body, want)
	}
	if len(plan.Blocks) != 2 || plan.Rows != 2 {
		t.Fatalf("plan blocks=%d rows=%d, want 2/2", len(plan.Blocks), plan.Rows)
	}
	if len(plan.SampleColumns) != 3 || plan.SampleColumns[1].Name != "region" || plan.SampleColumns[1].Type != "String" {
		t.Fatalf("sample columns = %+v", plan.SampleColumns)
	}
	if ev.calls != 1 || ev.seen.Rows != "(1, 'eu', 1.5), (2, 'us', 2.5)" {
		t.Fatalf("calls=%d rows=%q", ev.calls, ev.seen.Rows)
	}
	if got := ev.seen.Columns; len(got) != 3 || got[0] != "id" || got[2] != "amount" {
		t.Fatalf("columns = %v", got)
	}
	if ev.seen.MaxRows != 1000 || ev.seen.MaxBytes != 1<<20 || ev.seen.Timeout != 5*time.Second {
		t.Fatalf("evaluation limits = %+v", ev.seen)
	}
	if _, s, _, err := sicore.ParseFlatStatementID(qctx.Query.ID); err != nil || s != 1 || seq.Last() != 1 {
		t.Fatalf("statement id %q: seq=%d last=%d err=%v", qctx.Query.ID, s, seq.Last(), err)
	}
}

// TestPlugin_InlineValuesClaimsOnlyTheInlineShape is the feature-on mirror
// of TestPlugin_NonSILaneStatementsPassThrough: with the flag on the inline VALUES statement of that list is claimed and the others still fall through (D1).
func TestPlugin_InlineValuesClaimsOnlyTheInlineShape(t *testing.T) {
	for _, sql := range []string{"SELECT 1", "INSERT INTO shop.orders SELECT * FROM src", "CREATE TABLE x (a UInt8) ENGINE=Memory"} {
		p, seq := newInlinePlugin(t, &fakeEvaluator{})
		qctx := inlineQctx(newSession(21, ""), sql)
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("%q: OnQuery: %v", sql, err)
		}
		if qctx.SynthesizedInsert != nil || qctx.DeferredInsert != nil || qctx.Query.ID != "client-uuid-1" || seq.Last() != 0 {
			t.Fatalf("%q was claimed by the inline lane", sql)
		}
	}
	p, _ := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
	qctx := inlineQctx(newSession(22, ""), "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.SynthesizedInsert == nil {
		t.Fatal("an inline VALUES INSERT without a column list must be claimed")
	}
	if want := "INSERT INTO shop.orders (`id`, `region`, `amount`) FORMAT Native"; qctx.Query.Body != want {
		t.Fatalf("rewritten body = %q, want the declared column order %q", qctx.Query.Body, want)
	}
}

func TestPlugin_InlineValuesRejectionsConsumeNoSeq(t *testing.T) {
	sql := "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5)"
	cases := []struct {
		name, wantErr string
		ev            *fakeEvaluator
		mutate        func(*plugin.QueryContext)
	}{
		{"closure refused", "currentDatabase", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			q.Query.Body = "INSERT INTO shop.orders (id, region, amount) VALUES (1, currentDatabase(), 1.5)"
		}},
		{"evaluation failed", "code=62", &fakeEvaluator{err: errors.New("code=62 syntax error")}, nil},
		{"materialization missing", "materialization did not run", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			delete(q.Values, plugin.ValuesKeyMaterialized)
		}},
		{"materialization failed", "materialization failed: engine unavailable", &fakeEvaluator{}, func(q *plugin.QueryContext) {
			q.Values[plugin.ValuesKeyMaterialized] = "error:engine unavailable"
		}},
		{"column type mismatch", "wire type", &fakeEvaluator{blocks: [][]proto.InputColumn{{
			{Name: "id", Data: &proto.ColStr{}}, {Name: "region", Data: &proto.ColStr{}}, {Name: "amount", Data: &proto.ColFloat64{}},
		}}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, seq := newInlinePlugin(t, tc.ev)
			qctx := inlineQctx(newSession(9, ""), sql)
			if tc.mutate != nil {
				tc.mutate(qctx)
			}
			body := qctx.Query.Body
			err := p.OnQuery(context.Background(), qctx)
			if err == nil {
				t.Fatal("OnQuery accepted a statement it must refuse")
			}
			if !strings.HasPrefix(err.Error(), sicore.InlineValuesErrorPrefix) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %q, want prefix %q containing %q", err, sicore.InlineValuesErrorPrefix, tc.wantErr)
			}
			if qctx.SynthesizedInsert != nil || qctx.DeferredInsert != nil {
				t.Fatal("a refused statement installed a plan")
			}
			if qctx.Query.ID != "client-uuid-1" || qctx.Query.Body != body {
				t.Fatalf("a refused statement mutated the query")
			}
			if seq.Last() != 0 {
				t.Fatalf("a refused statement consumed client_seq (last=%d)", seq.Last())
			}
		})
	}
}

func TestPlugin_InlineValuesStrictHookHashesPlanPayload(t *testing.T) {
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
	qctx := inlineQctx(newSession(13, ""), "INSERT INTO shop.orders (id, region, amount) VALUES (1, 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	plan := qctx.SynthesizedInsert // the relay lane fills Packets before firing the strict hook
	plan.Packets, plan.PayloadBytes = [][]byte{{0x02, 0x00, 0xaa}, {0x02, 0x00, 0xbb}}, 6
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("strict hook: %v", err)
	}
	var token string
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.StatementTokenSettingKey {
			token = strings.Trim(s.Value, "'")
		}
	}
	if token == "" {
		t.Fatal("no statement token was appended")
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID: testNetworkID, StatementID: qctx.Query.ID, SQLHash: replay.DigestString(qctx.Query.Body),
		SettingsHash: sicore.EmptySettingsHash, SchemaHash: payloadexec.TableSchemaHash(testNetworkID, testSchema()),
		PayloadHash: replay.DigestBytes(plan.Payload()), PayloadLength: uint64(len(plan.Payload())),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: testRevision,
		TargetTableID: "shop.orders", RowIDProfileID: payloadexec.RowIDProfileID, StatementKind: sicore.StatementKindCodeInsert,
	}
	if got, err := validator.ValidateStatementV2(token, want); err != nil || got != signer.Address() {
		t.Fatalf("token does not bind the plan payload: signer=%s err=%v", got, err)
	}
}

type inlineMetrics struct{ synthesized, evaluation, closure int }

func (m *inlineMetrics) InlineValuesSynthesized()      { m.synthesized++ }
func (m *inlineMetrics) InlineValuesEvaluationFailed() { m.evaluation++ }
func (m *inlineMetrics) InlineValuesClosureRefused()   { m.closure++ }

func TestPlugin_InlineValuesAdmissionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name               string
		change             func(*Plugin, *plugin.QueryContext, *fakeEvaluator)
		evaluated, closure bool
	}{
		{"unknown materialization", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Values[plugin.ValuesKeyMaterialized] = "unknown"
		}, false, false},
		{"empty materialization", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) { q.Values[plugin.ValuesKeyMaterialized] = "" }, false, false},
		{"nil values", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) { q.Values = nil }, false, false},
		{"compression", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Compression = proto.CompressionEnabled
		}, false, false},
		{"settings", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Settings = []chproto.Setting{{Key: "max_threads", Value: "1"}}
		}, false, false},
		{"parameters", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Parameters = []proto.Parameter{{Key: "x", Value: "1"}}
		}, false, false},
		{"schema missing", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Body = "INSERT INTO shop.missing VALUES (1)"
		}, false, false},
		{"parse", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Body = "INSERT INTO shop.orders VALUES (1); SELECT 1"
		}, false, false},
		{"closure", func(_ *Plugin, q *plugin.QueryContext, _ *fakeEvaluator) {
			q.Query.Body = "INSERT INTO shop.orders VALUES (rand(), 'eu', 1.5)"
		}, false, true},
		{"empty result", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) { e.blocks = nil }, true, false},
		{"nil column", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) { e.blocks[0][0].Data = nil }, true, false},
		{"typed nil column", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) {
			e.blocks[0][0].Data = (*proto.ColUInt64)(nil)
		}, true, false},
		{"wrong name", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) { e.blocks[0][0].Name = "other" }, true, false},
		{"ragged", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) {
			e.blocks[0][0].Data = &proto.ColUInt64{1, 2}
		}, true, false},
		{"wrong count", func(_ *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) { e.blocks[0] = e.blocks[0][:2] }, true, false},
		{"aggregate rows", func(p *Plugin, _ *plugin.QueryContext, e *fakeEvaluator) {
			p.inline.MaxRows = 1
			e.blocks = append(e.blocks, evaluatedBlock(2, "us", 2.5))
		}, true, false},
		{"aggregate bytes", func(p *Plugin, q *plugin.QueryContext, e *fakeEvaluator) {
			data, _ := q.Session.Upstream().EncodeClientDataPacket(e.blocks[0])
			p.maxPayload = uint64(len(data))
			e.blocks = append(e.blocks, evaluatedBlock(2, "us", 2.5))
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
			p, seq := newInlinePlugin(t, ev)
			metrics := &inlineMetrics{}
			p.observer = metrics
			q := inlineQctx(newSession(99, ""), "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
			tc.change(p, q, ev)
			before := q.Query.Body
			err := p.OnQuery(context.Background(), q)
			if err == nil || strings.Count(err.Error(), sicore.InlineValuesErrorPrefix) != 1 || !strings.HasPrefix(err.Error(), sicore.InlineValuesErrorPrefix) {
				t.Fatalf("err=%v", err)
			}
			if seq.Last() != 0 || q.Query.ID != "client-uuid-1" || q.Query.Body != before || q.SynthesizedInsert != nil {
				t.Fatal("refusal changed statement or sequence")
			}
			want := 0
			if tc.evaluated {
				want = 1
			}
			if ev.calls != want || metrics.evaluation != want {
				t.Fatalf("calls=%d metrics=%+v", ev.calls, metrics)
			}
			closure := 0
			if tc.closure {
				closure = 1
			}
			if metrics.closure != closure || metrics.synthesized != 0 {
				t.Fatalf("metrics=%+v", metrics)
			}
		})
	}
}

type namedEvaluationConn struct {
	net.Conn
	address string
}

func (c namedEvaluationConn) UpstreamAddress() string { return c.address }

func TestPlugin_InlineValuesUsesCurrentEndpointAndRevision(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
	p, _ := newInlinePlugin(t, ev)
	sess := newSession(100, "shop")
	q := inlineQctx(sess, "INSERT INTO orders VALUES (1, 'eu', 1.5)")
	sess.upstream = chproto.NewCodec(namedEvaluationConn{Conn: sess.upstream.Conn().(net.Conn), address: "selected-peer:9000"}, chproto.DirToUpstream)
	sess.upstream.SetRevision(54470)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if ev.seen.UpstreamAddress != "selected-peer:9000" || ev.seen.Hello.Database != "shop" || ev.seen.Hello.User != "writer" {
		t.Fatalf("evaluation=%+v", ev.seen)
	}
	if p.pending[sess.ID()].clientRevision != 54470 {
		t.Fatal("signed revision must be current upstream, not client revision")
	}
	sess.upstream.SetRevision(54460)
	if err := p.OnQueryInputCompleteStrict(context.Background(), q); err == nil || !strings.Contains(err.Error(), "revision changed") {
		t.Fatalf("err=%v", err)
	}
}

func TestPlugin_InlineValuesColumnIdentityAndPendingSequence(t *testing.T) {
	got := inlineInsertBody(sicore.InsertTarget{Database: "shop", Table: "orders"}, []chproto.SampleColumn{{Name: "a.b"}, {Name: "a`b"}, {Name: "select"}})
	if got != "INSERT INTO shop.orders (`a.b`, `a``b`, `select`) FORMAT Native" {
		t.Fatal(got)
	}
	p, seq := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
	sess := newSession(101, "")
	q := inlineQctx(sess, "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	second := inlineQctx(sess, "INSERT INTO shop.orders VALUES (2, 'us', 2.5)")
	if err := p.OnQuery(context.Background(), second); err == nil {
		t.Fatal("overlapping statement accepted")
	}
	if seq.Last() != 1 {
		t.Fatalf("rejection consumed seq %d", seq.Last())
	}
	p.OnQueryAbort(context.Background(), q)
	if seq.Last() != 1 {
		t.Fatal("durable sequence rolled back")
	}
}

func TestPlugin_InlineValuesColumnOrderAndAppliedMaterialization(t *testing.T) {
	block := evaluatedBlock(1, "eu", 1.5)
	block[0], block[1] = block[1], block[0]
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{block}}
	p, _ := newInlinePlugin(t, ev)
	q := inlineQctx(newSession(103, "shop"), "INSERT INTO orders (region, id, amount) VALUES ('eu', 1, 1.5)")
	q.Values[plugin.ValuesKeyMaterialized] = "applied"
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.Query.Body != "INSERT INTO shop.orders (`region`, `id`, `amount`) FORMAT Native" || strings.Join(ev.seen.Columns, ",") != "region,id,amount" {
		t.Fatalf("query=%s columns=%v", q.Query.Body, ev.seen.Columns)
	}
}

func TestPlugin_InlineValuesStrictHookRejectsInvalidPayloadAccounting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    uint64
		packets [][]byte
	}{
		{"empty", 0, nil}, {"mismatch", 100, [][]byte{{2, 0, 1}}}, {"over limit", 1 << 21, [][]byte{make([]byte, 1<<21)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, seq := newInlinePlugin(t, &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}})
			q := inlineQctx(newSession(104, ""), "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
			if err := p.OnQuery(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			q.SynthesizedInsert.Packets = tc.packets
			q.SynthesizedInsert.PayloadBytes = tc.size
			if err := p.OnQueryInputCompleteStrict(context.Background(), q); err == nil || strings.Count(err.Error(), sicore.InlineValuesErrorPrefix) != 1 {
				t.Fatalf("err=%v", err)
			}
			for _, setting := range q.Query.Settings {
				if setting.Key == auth.StatementTokenSettingKey {
					t.Fatal("signed invalid payload")
				}
			}
			if seq.Last() != 1 {
				t.Fatal("rolled back allocated sequence")
			}
		})
	}
}
