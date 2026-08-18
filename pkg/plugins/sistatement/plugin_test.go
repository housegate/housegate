package sistatement

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

const (
	testKey       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testNetworkID = "testnet-v2"
	testRevision  = 54460
)

type fakeSession struct {
	id    int64
	state *chsession.SessionState
}

func (s *fakeSession) ID() int64                                          { return s.id }
func (s *fakeSession) State() *chsession.SessionState                     { return s.state }
func (s *fakeSession) Client() *chproto.Codec                             { return nil }
func (s *fakeSession) Upstream() *chproto.Codec                           { return nil }
func (s *fakeSession) RemoteAddr() net.Addr                               { return nil }
func (s *fakeSession) Close() error                                       { return nil }
func (s *fakeSession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *fakeSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *fakeSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *fakeSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

func newSession(id int64, logicalDB string) *fakeSession {
	st := chsession.NewSessionState()
	st.ClientRevision = testRevision
	if logicalDB != "" {
		st.SetLogicalDatabase(logicalDB)
	}
	return &fakeSession{id: id, state: st}
}

func declareSchema(t *testing.T, ns *network.InMemoryNetworkState, schema payloadexec.TableSchema) string {
	t.Helper()
	db, table, ok := strings.Cut(schema.TableID, ".")
	if !ok {
		t.Fatalf("schema table id %q", schema.TableID)
	}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hash := payloadexec.TableSchemaHash(testNetworkID, schema)
	ns.TableSchemas[db+"/"+table+"@1"] = network.TableSchemaInfo{DatabaseId: db, TableId: table, Version: 1, SchemaHash: hash, SchemaJson: string(js)}
	return hash
}

func newTestPlugin(t *testing.T, ns *network.InMemoryNetworkState, stateDir string) (*Plugin, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(stateDir, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, KeeperShardID: 0, Seq: seq, MaxPayloadBytes: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, signer
}

func insertQctx(sess *fakeSession, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{ID: "client-uuid-1", Body: sql, Compression: proto.CompressionDisabled},
		Values:      map[string]any{},
	}
}

func encodeRows(t *testing.T) []byte {
	t.Helper()
	id := proto.ColUInt64{1, 2}
	region := proto.ColStr{}
	region.Append("eu")
	region.Append("us")
	amount := proto.ColFloat64{1.5, 2.5}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 2, Columns: 3}).EncodeBlock(&buf, testRevision, proto.Input{{Name: "id", Data: &id}, {Name: "region", Data: &region}, {Name: "amount", Data: &amount}}); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestPlugin_HappyPathSignsStatementTokenAfterPayload(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	schemaHash := declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	sess := newSession(7, "")
	sql := "INSERT INTO shop.orders FORMAT Native"
	qctx := insertQctx(sess, sql)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.DeferredInsert == nil || len(qctx.DeferredInsert.SampleColumns) != 3 || qctx.DeferredInsert.SampleColumns[1] != (chproto.SampleColumn{Name: "region", Type: "String"}) {
		t.Fatalf("DeferredInsert = %#v", qctx.DeferredInsert)
	}
	if qctx.DeferredInsert.MaxPayloadBytes != 1<<20 {
		t.Fatalf("MaxPayloadBytes = %d", qctx.DeferredInsert.MaxPayloadBytes)
	}
	account, seq, nonce, err := sicore.ParseFlatStatementID(qctx.Query.ID)
	if err != nil || account != signer.Address() || seq != 1 || len(nonce) != 32 {
		t.Fatalf("statement id %q: account=%s seq=%d nonce=%q err=%v", qctx.Query.ID, account, seq, nonce, err)
	}
	payload := encodeRows(t)
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if limit, enforce := p.ClientDataReadLimit(qctx); !enforce || limit != (1<<20)-uint64(len(payload)) {
		t.Fatalf("ClientDataReadLimit = %d/%v", limit, enforce)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	var token string
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.StatementTokenSettingKey {
			token = s.Value
			if !s.Custom || !strings.HasPrefix(s.Value, "'") {
				t.Fatalf("statement token setting must be Custom and quoted: %#v", s)
			}
		}
	}
	if token == "" {
		t.Fatal("SQL_x_statement_token missing")
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID:      testNetworkID,
		KeeperShardID:  0,
		StatementID:    qctx.Query.ID,
		SQLHash:        replay.DigestString(sql),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: testRevision,
		TargetTableID:  "shop.orders",
		RowIDProfileID: payloadexec.RowIDProfileID,
	}
	if got, err := validator.ValidateStatementV2(token, want); err != nil || got != signer.Address() {
		t.Fatalf("token does not bind the envelope: got=%s err=%v", got, err)
	}
	// After signing the per-session state is released; the next INSERT gets seq 2.
	next := insertQctx(sess, sql)
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatalf("second OnQuery: %v", err)
	}
	if _, seq, _, _ := sicore.ParseFlatStatementID(next.Query.ID); seq != 2 {
		t.Fatalf("second seq = %d, want 2", seq)
	}
}

func TestPlugin_SeqSurvivesRestart(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	dir := t.TempDir()
	p, _ := newTestPlugin(t, ns, dir)
	q := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	p2, _ := newTestPlugin(t, ns, dir)
	q2 := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p2.OnQuery(context.Background(), q2); err != nil {
		t.Fatal(err)
	}
	if _, seq, _, _ := sicore.ParseFlatStatementID(q2.Query.ID); seq != 2 {
		t.Fatalf("seq after restart = %d, want 2", seq)
	}
}

func TestPlugin_ClientSuppliedStatementIDIsKeptOnlyForOwnAccount(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	own := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	own.Query.ID = "0x" + strings.ToUpper(strings.TrimPrefix(signer.Address(), "0x")) + ":41:sdk-nonce"
	if err := p.OnQuery(context.Background(), own); err != nil || own.Query.ID != signer.Address()+":41:sdk-nonce" {
		t.Fatalf("own-account id must be kept: id=%q err=%v", own.Query.ID, err)
	}
	generated := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), generated); err != nil {
		t.Fatal(err)
	}
	if account, seq, _, err := sicore.ParseFlatStatementID(generated.Query.ID); err != nil || account != signer.Address() || seq != 42 {
		t.Fatalf("generated id after SDK reservation = %q account=%s seq=%d err=%v, want seq 42", generated.Query.ID, account, seq, err)
	}
	foreign := insertQctx(newSession(3, ""), "INSERT INTO shop.orders FORMAT Native")
	foreign.Query.ID = "0x0000000000000000000000000000000000000001:5:n"
	if err := p.OnQuery(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	if account, _, _, _ := sicore.ParseFlatStatementID(foreign.Query.ID); account != signer.Address() {
		t.Fatalf("foreign-account id must be replaced, got %q", foreign.Query.ID)
	}
}

func TestPlugin_ClientSuppliedMaxSequenceIsRejectedWithoutAdvancing(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	terminal := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	terminal.Query.ID = signer.Address() + ":18446744073709551615:sdk-nonce"
	if err := p.OnQuery(context.Background(), terminal); !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("terminal supplied sequence = %v, want ErrClientSeqExhausted", err)
	}
	generated := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), generated); err != nil {
		t.Fatal(err)
	}
	if _, seq, _, err := sicore.ParseFlatStatementID(generated.Query.ID); err != nil || seq != 1 {
		t.Fatalf("generated id after rejected terminal reservation = %q seq=%d err=%v, want seq 1", generated.Query.ID, seq, err)
	}
}

func TestPlugin_Rejections(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, _ := newTestPlugin(t, ns, t.TempDir())
	cases := []struct {
		name    string
		mutate  func(*plugin.QueryContext)
		wantErr string
	}{
		{"schema missing", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO shop.unknown FORMAT Native" }, "not declared"},
		{"compressed", func(q *plugin.QueryContext) { q.Query.Compression = proto.CompressionEnabled }, "compressed"},
		{"user setting", func(q *plugin.QueryContext) {
			q.Query.Settings = []chproto.Setting{{Key: "SQL_x_payer", Value: "'0xabc'", Custom: true}, {Key: "async_insert", Value: "1"}}
		}, "async_insert"},
		{"unqualified without session db", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO orders FORMAT Native" }, "database-qualified"},
		{"column subset", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO shop.orders (id, region) FORMAT Native" }, "amount"},
		{"unknown revision", func(q *plugin.QueryContext) { q.Session.State().ClientRevision = 0 }, "revision"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := insertQctx(newSession(int64(10+i), ""), "INSERT INTO shop.orders FORMAT Native")
			tc.mutate(q)
			err := p.OnQuery(context.Background(), q)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
			if q.DeferredInsert != nil {
				t.Fatal("rejected query must not carry a deferred plan")
			}
		})
	}
}

func TestPlugin_NonSILaneStatementsPassThrough(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	p, _ := newTestPlugin(t, ns, t.TempDir())
	for _, sql := range []string{"SELECT 1", "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)", "INSERT INTO shop.orders SELECT * FROM src", "CREATE TABLE x (a UInt8) ENGINE=Memory"} {
		q := insertQctx(newSession(1, ""), sql)
		if err := p.OnQuery(context.Background(), q); err != nil || q.DeferredInsert != nil || q.Query.ID != "client-uuid-1" {
			t.Fatalf("%q: err=%v deferred=%v id=%q", sql, err, q.DeferredInsert, q.Query.ID)
		}
	}
}

func TestPlugin_UseTrackingResolvesUnqualifiedTarget(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, _ := newTestPlugin(t, ns, t.TempDir())
	sess := newSession(3, "other")
	if err := p.OnQuery(context.Background(), insertQctx(sess, "USE shop")); err != nil {
		t.Fatal(err)
	}
	q := insertQctx(sess, "INSERT INTO orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("USE-resolved target: %v", err)
	}
	if q.DeferredInsert == nil {
		t.Fatal("expected deferred plan for shop.orders")
	}
}

func TestPlugin_OversizedPayloadAndAbortAndClose(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, _ := auth.NewRelaySigner(testKey)
	seq, _ := OpenSeqCounter(t.TempDir(), signer.Address())
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	sess := newSession(4, "")
	q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err == nil || !strings.Contains(err.Error(), "max_payload_bytes") {
		t.Fatalf("oversized payload must be rejected: %v", err)
	}
	// Rejected capture leaves no state: a new INSERT on the session is accepted.
	q2 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q2); err != nil {
		t.Fatalf("after oversized rejection: %v", err)
	}
	// A pending INSERT blocks a second one on the same session until abort/close.
	q3 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q3); err == nil || !strings.Contains(err.Error(), "has not completed") {
		t.Fatalf("second in-flight INSERT: %v", err)
	}
	p.OnQueryAbort(context.Background(), q2)
	if err := p.OnQuery(context.Background(), q3); err != nil {
		t.Fatalf("after abort: %v", err)
	}
	p.OnClose(sess)
	q4 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q4); err != nil {
		t.Fatalf("after close: %v", err)
	}
	// Input-complete without any payload fails closed.
	if err := p.OnQueryInputCompleteStrict(context.Background(), q4); err == nil || !strings.Contains(err.Error(), "no payload") {
		t.Fatalf("empty payload must be rejected: %v", err)
	}
}

func TestPlugin_SequenceDirectoryDurabilityFailureDoesNotAdmitOrSign(t *testing.T) {
	injected := errors.New("injected directory durability failure")
	tests := []struct {
		name    string
		openDir func(string) (seqDir, error)
	}{
		{
			name: "open",
			openDir: func(string) (seqDir, error) {
				return nil, injected
			},
		},
		{
			name: "sync",
			openDir: func(path string) (seqDir, error) {
				f, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				return &failingSeqDir{file: f, syncErr: injected}, nil
			},
		},
		{
			name: "close",
			openDir: func(path string) (seqDir, error) {
				f, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				return &failingSeqDir{file: f, closeErr: injected}, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns := network.NewInMemoryNetworkState()
			declareSchema(t, ns, testSchema())
			signer, err := auth.NewRelaySigner(testKey)
			if err != nil {
				t.Fatal(err)
			}
			seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
			if err != nil {
				t.Fatal(err)
			}
			seq.openDir = tc.openDir
			p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			q := insertQctx(newSession(100, ""), "INSERT INTO shop.orders FORMAT Native")
			if err := p.OnQuery(context.Background(), q); !errors.Is(err, injected) {
				t.Fatalf("OnQuery = %v, want injected durability error", err)
			}
			if q.DeferredInsert != nil || q.Query.ID != "client-uuid-1" {
				t.Fatalf("failed reservation admitted query: deferred=%#v id=%q", q.DeferredInsert, q.Query.ID)
			}
			if err := p.OnQueryInputCompleteStrict(context.Background(), q); err != nil {
				t.Fatalf("failed reservation unexpectedly left signable state: %v", err)
			}
			for _, setting := range q.Query.Settings {
				if setting.Key == auth.StatementTokenSettingKey {
					t.Fatalf("failed reservation emitted statement token: %#v", setting)
				}
			}
		})
	}
}
