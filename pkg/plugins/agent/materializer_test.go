package agent

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay/payloadexec"
)

func TestMaterializerRewritesWhitelistedNondeterministicFunctions(t *testing.T) {
	p := NewMaterializer(MaterializerOptions{
		Now:  func() time.Time { return time.Date(2026, 6, 29, 4, 32, 0, 0, time.UTC) },
		Rand: func() uint64 { return 7 },
		UUID: func() string { return "11111111-2222-4333-8444-555555555555" },
	})
	qctx := newTestQueryContext(newTestSession(t, 101),
		"INSERT INTO t VALUES (now(), rand(), random(), rand64(), generateUUIDv4())")

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	want := "INSERT INTO t VALUES ('2026-06-29 04:32:00', 7, 7, 7, '11111111-2222-4333-8444-555555555555')"
	if qctx.Query.Body != want {
		t.Fatalf("materialized SQL = %q, want %q", qctx.Query.Body, want)
	}
	if qctx.OriginalSQL != "INSERT INTO t VALUES (now(), rand(), random(), rand64(), generateUUIDv4())" {
		t.Fatalf("OriginalSQL changed: %q", qctx.OriginalSQL)
	}
}

func TestMaterializerSkipsStringsQuotedIdentifiersAndComments(t *testing.T) {
	p := NewMaterializer(MaterializerOptions{
		Now:  func() time.Time { return time.Date(2026, 6, 29, 4, 32, 0, 0, time.UTC) },
		Rand: func() uint64 { return 9 },
		UUID: func() string { return "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" },
	})
	sql := "SELECT 'rand()' AS s, `now()` AS c, rand() AS r -- random()\nFROM t /* generateUUIDv4() */"
	qctx := newTestQueryContext(newTestSession(t, 102), sql)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	want := "SELECT 'rand()' AS s, `now()` AS c, 9 AS r -- random()\nFROM t /* generateUUIDv4() */"
	if qctx.Query.Body != want {
		t.Fatalf("materialized SQL = %q, want %q", qctx.Query.Body, want)
	}
}

func TestMaterializerRunsBeforeAgentSigner(t *testing.T) {
	signer, err := auth.NewRelaySigner("9999999999999999999999999999999999999999999999999999999999999999")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}
	mat := NewMaterializer(MaterializerOptions{
		Now:  func() time.Time { return time.Date(2026, 6, 29, 4, 32, 0, 0, time.UTC) },
		Rand: func() uint64 { return 11 },
		UUID: func() string { return "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff" },
	})
	sign := &Plugin{Signer: signer}
	qctx := &plugin.QueryContext{
		Session:     newTestSession(t, 103),
		OriginalSQL: "SELECT rand()",
		Query:       &chproto.Query{Body: "SELECT rand()"},
	}

	if err := mat.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("materializer OnQuery: %v", err)
	}
	if err := sign.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("signer OnQuery: %v", err)
	}

	qhash, err := decodeJWSPayloadQHash(findAuthToken(qctx.Query.Settings))
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	if want := keccak256Hex([]byte("SELECT 11")); qhash != want {
		t.Fatalf("qhash = %s, want materialized SQL hash %s", qhash, want)
	}
}

func TestMaterializerInjectsValuesRowIDBeforeAgentSigner(t *testing.T) {
	signer, err := auth.NewRelaySigner("8888888888888888888888888888888888888888888888888888888888888888")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}
	mat := NewMaterializer(MaterializerOptions{
		Rand: func() uint64 { return 42 },
		RowID: RowIDOptions{
			Enabled:   true,
			NetworkID: "sentio-testnet",
		},
	})
	sign := &Plugin{Signer: signer}
	qctx := &plugin.QueryContext{
		Session:     newTestSession(t, 106),
		OriginalSQL: "INSERT INTO balances VALUES (rand(), 'a'), (rand(), 'b')",
		Query: &chproto.Query{
			ID:   "stmt-values-rowid",
			Body: "INSERT INTO balances VALUES (rand(), 'a'), (rand(), 'b')",
		},
	}

	if err := mat.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("materializer OnQuery: %v", err)
	}
	if err := sign.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("signer OnQuery: %v", err)
	}

	for i := 0; i < 2; i++ {
		want := hex.EncodeToString(payloadexec.RowID("sentio-testnet", "balances", "stmt-values-rowid", uint64(i)))
		if !strings.Contains(qctx.Query.Body, "unhex('"+want+"')") {
			t.Fatalf("materialized SQL %q missing row id %d %s", qctx.Query.Body, i, want)
		}
	}
	if strings.Contains(qctx.Query.Body, "rand()") {
		t.Fatalf("materialized SQL still contains rand(): %q", qctx.Query.Body)
	}
	qhash, err := decodeJWSPayloadQHash(findAuthToken(qctx.Query.Settings))
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	if want := keccak256Hex([]byte(qctx.Query.Body)); qhash != want {
		t.Fatalf("qhash = %s, want final materialized SQL hash %s", qhash, want)
	}
}

func TestMaterializerInjectsRowIDIntoNativeDataBeforeForward(t *testing.T) {
	p := NewMaterializer(MaterializerOptions{
		RowID: RowIDOptions{Enabled: true, NetworkID: "sentio-testnet"},
	})
	sess := newTestSession(t, 104)
	sess.State().ClientRevision = testAgentRevision
	qctx := newTestQueryContext(sess, "INSERT INTO app.balances FORMAT Native")
	qctx.Query.ID = "stmt-rowid-1"

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	accounts := new(proto.ColStr)
	accounts.Append("0xaaa")
	accounts.Append("0xbbb")
	balances := proto.ColUInt64{100, 200}
	raw := encodeAgentClientDataPacket(t, 2, proto.Input{
		{Name: "account", Data: accounts},
		{Name: "balance", Data: &balances},
	})

	rewritten, err := p.RewriteClientData(context.Background(), qctx, raw)
	if err != nil {
		t.Fatalf("RewriteClientData: %v", err)
	}

	cols, rowIDs := decodeAgentRowIDs(t, rewritten)
	if len(cols) != 3 {
		t.Fatalf("columns = %v, want _hg_row_id plus 2 user columns", cols)
	}
	if cols[0] != "_hg_row_id" || cols[1] != "account" || cols[2] != "balance" {
		t.Fatalf("columns = %v, want [_hg_row_id account balance]", cols)
	}
	for i, got := range rowIDs {
		want := payloadexec.RowID("sentio-testnet", "app.balances", "stmt-rowid-1", uint64(i))
		if !bytes.Equal(got, want) {
			t.Fatalf("row id %d = %x, want %x", i, got, want)
		}
	}
}

func TestMaterializerRowIDOrdinalContinuesAcrossNativeBlocks(t *testing.T) {
	p := NewMaterializer(MaterializerOptions{
		RowID: RowIDOptions{Enabled: true, NetworkID: "sentio-testnet"},
	})
	sess := newTestSession(t, 105)
	sess.State().ClientRevision = testAgentRevision
	qctx := newTestQueryContext(sess, "INSERT INTO balances FORMAT Native")
	qctx.Query.ID = "stmt-rowid-2"

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	raw1 := encodeAgentClientDataPacket(t, 1, proto.Input{{Name: "amount", Data: &proto.ColUInt64{1}}})
	raw2 := encodeAgentClientDataPacket(t, 1, proto.Input{{Name: "amount", Data: &proto.ColUInt64{2}}})
	rewritten1, err := p.RewriteClientData(context.Background(), qctx, raw1)
	if err != nil {
		t.Fatalf("RewriteClientData block 1: %v", err)
	}
	rewritten2, err := p.RewriteClientData(context.Background(), qctx, raw2)
	if err != nil {
		t.Fatalf("RewriteClientData block 2: %v", err)
	}

	_, ids1 := decodeAgentRowIDs(t, rewritten1)
	_, ids2 := decodeAgentRowIDs(t, rewritten2)
	if len(ids1) != 1 || len(ids2) != 1 {
		t.Fatalf("decoded row ids = %d/%d, want 1/1", len(ids1), len(ids2))
	}
	want0 := payloadexec.RowID("sentio-testnet", "balances", "stmt-rowid-2", 0)
	want1 := payloadexec.RowID("sentio-testnet", "balances", "stmt-rowid-2", 1)
	if !bytes.Equal(ids1[0], want0) {
		t.Fatalf("block1 row id = %x, want ordinal 0 %x", ids1[0], want0)
	}
	if !bytes.Equal(ids2[0], want1) {
		t.Fatalf("block2 row id = %x, want ordinal 1 %x", ids2[0], want1)
	}
}

func TestMaterializerRejectsCompressedNativeRowIDPath(t *testing.T) {
	p := NewMaterializer(MaterializerOptions{
		RowID: RowIDOptions{Enabled: true, NetworkID: "sentio-testnet"},
	})
	qctx := newTestQueryContext(newTestSession(t, 107), "INSERT INTO balances FORMAT Native")
	qctx.Query.ID = "stmt-compressed"
	qctx.Query.Compression = proto.CompressionEnabled

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("OnQuery succeeded, want compressed Native row-id rejection")
	}
	if !strings.Contains(err.Error(), "compressed") {
		t.Fatalf("error = %v, want compressed rejection", err)
	}
}

const testAgentRevision = 54453

func encodeAgentClientDataPacket(t *testing.T, rows int, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Rows: rows, Columns: len(input)}
	if err := block.EncodeBlock(&buf, testAgentRevision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return buf.Buf
}

func decodeAgentRowIDs(t *testing.T, raw []byte) ([]string, [][]byte) {
	t.Helper()
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil {
		t.Fatalf("packet code: %v", err)
	}
	if code != uint64(chproto.ClientDataCode) {
		t.Fatalf("packet code = %d, want ClientData", code)
	}
	if _, err := r.Str(); err != nil {
		t.Fatalf("block name: %v", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, testAgentRevision, results.Auto()); err != nil {
		t.Fatalf("decode block: %v", err)
	}
	cols := make([]string, 0, len(results))
	for _, col := range results {
		cols = append(cols, col.Name)
	}
	if len(results) == 0 {
		return cols, nil
	}
	auto, ok := results[0].Data.(*proto.ColAuto)
	if ok {
		if fixed, ok := auto.Data.(*proto.ColFixedStr); ok {
			return cols, fixedRows(fixed)
		}
		if fixed, ok := auto.Data.(*proto.ColFixedStr32); ok {
			return cols, fixed32Rows(fixed)
		}
		t.Fatalf("first column auto data = %T, want *proto.ColFixedStr", auto.Data)
	}
	if fixed, ok := results[0].Data.(*proto.ColFixedStr); ok {
		return cols, fixedRows(fixed)
	}
	if fixed, ok := results[0].Data.(*proto.ColFixedStr32); ok {
		return cols, fixed32Rows(fixed)
	}
	t.Fatalf("first column data = %T, want *proto.ColFixedStr", results[0].Data)
	return nil, nil
}

func fixedRows(col *proto.ColFixedStr) [][]byte {
	out := make([][]byte, 0, col.Rows())
	for i := 0; i < col.Rows(); i++ {
		out = append(out, append([]byte(nil), col.Row(i)...))
	}
	return out
}

func fixed32Rows(col *proto.ColFixedStr32) [][]byte {
	out := make([][]byte, 0, len(*col))
	for _, row := range *col {
		out = append(out, append([]byte(nil), row[:]...))
	}
	return out
}
