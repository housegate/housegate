package storageintegrity

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/replay"
)

const testNativeReplayRevision = 54453

func TestComputeInsertReplayReplaysNativePayloadBlocks(t *testing.T) {
	rowID1 := fixedRowID(1)
	rowID2 := fixedRowID(2)
	raw := encodeReplayNativeDataPacket(t, proto.Input{
		{Name: "_hg_row_id", Data: fixedRowIDColumn(rowID1, rowID2)},
		{Name: "id", Data: &proto.ColUInt64{101, 102}},
		{Name: "v", Data: replayStringColumn("alpha", "beta")},
	})
	payload := encodeReplayPayloadEnvelope(t, replayPayloadEnvelope{
		statementID: "stmt-native-1",
		tableID:     "app.balances",
		originalSQL: "INSERT INTO app.balances FORMAT Native",
		unsafeSQL:   "INSERT INTO `hg_unsafe`.`app.balances_a` FORMAT Native",
		dataPackets: [][]byte{raw, emptyReplayNativeDataPacket(t)},
	})
	conn := newFakeReplayConn([]string{"_hg_row_id", "id", "v"})
	verifier := &ClickHouseInsertReplayVerifier{conn: conn}

	result, err := verifier.ComputeInsertReplay(context.Background(), InsertReplayRequest{
		TableID:     "app.balances",
		StatementID: "stmt-native-1",
		SQL:         "INSERT INTO `hg_unsafe`.`app.balances_a` FORMAT Native",
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("ComputeInsertReplay: %v", err)
	}
	if result.RowCount != 2 {
		t.Fatalf("RowCount=%d, want 2", result.RowCount)
	}
	if len(conn.batches) != 1 {
		t.Fatalf("batches=%d, want 1", len(conn.batches))
	}
	if got := conn.batches[0].query; !strings.HasPrefix(got, "INSERT INTO `default`.`_hg_replay_insert_") {
		t.Fatalf("batch query=%q, want scratch insert", got)
	}
	wantRows := [][]any{
		{rowID1[:], uint64(101), "alpha"},
		{rowID2[:], uint64(102), "beta"},
	}
	if diff := compareReplayRows(conn.batches[0].rows, wantRows); diff != "" {
		t.Fatal(diff)
	}
	if conn.batches[0].sent != 1 {
		t.Fatalf("batch sent=%d, want 1", conn.batches[0].sent)
	}
}

func TestVerifyLoadsAndValidatesNativePayload(t *testing.T) {
	payload := []byte("bad native payload")
	store := mapPayloadStore{
		"mockda://payload": payload,
	}
	conn := newFakeReplayConn([]string{"_hg_row_id", "id"})
	verifier := &ClickHouseInsertReplayVerifier{
		conn:     conn,
		Signer:   fakeReplaySigner{workerID: "r1"},
		Payloads: store,
	}
	_, err := verifier.Verify(context.Background(), replayJobForNativePayload("mockda://payload", "0xdeadbeef", uint64(len(payload))))
	if err == nil {
		t.Fatal("Verify succeeded, want payload digest mismatch")
	}
	if !strings.Contains(err.Error(), "payload digest mismatch") {
		t.Fatalf("error=%v, want payload digest mismatch", err)
	}
}

type fakeReplayConn struct {
	columns []string
	execs   []string
	batches []*fakeReplayBatch
}

func newFakeReplayConn(columns []string) *fakeReplayConn {
	return &fakeReplayConn{columns: append([]string(nil), columns...)}
}

func (c *fakeReplayConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeReplayConn) Query(_ context.Context, query string, args ...any) (chdriver.Rows, error) {
	if strings.Contains(query, "FROM system.columns") {
		return &fakeReplayRows{rows: columnNameRows(c.columns)}, nil
	}
	if strings.HasPrefix(query, "SELECT lower(hex(_hg_row_id))") {
		if len(c.batches) == 0 {
			return &fakeReplayRows{}, nil
		}
		return &fakeReplayRows{rows: hashReadRows(c.batches[len(c.batches)-1].rows)}, nil
	}
	return nil, fmt.Errorf("unexpected query %q args=%v", query, args)
}

func (c *fakeReplayConn) PrepareBatch(_ context.Context, query string, _ ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	b := &fakeReplayBatch{query: query}
	c.batches = append(c.batches, b)
	return b, nil
}

type fakeReplayBatch struct {
	query string
	rows  [][]any
	sent  int
}

func (b *fakeReplayBatch) Abort() error { return nil }
func (b *fakeReplayBatch) Append(v ...any) error {
	row := make([]any, len(v))
	copy(row, v)
	b.rows = append(b.rows, row)
	return nil
}
func (b *fakeReplayBatch) AppendStruct(any) error { return fmt.Errorf("not implemented") }
func (b *fakeReplayBatch) Column(int) chdriver.BatchColumn {
	return nil
}
func (b *fakeReplayBatch) Flush() error { return nil }
func (b *fakeReplayBatch) Send() error {
	b.sent++
	return nil
}
func (b *fakeReplayBatch) IsSent() bool { return b.sent > 0 }
func (b *fakeReplayBatch) Rows() int    { return len(b.rows) }
func (b *fakeReplayBatch) Columns() []column.Interface {
	return nil
}
func (b *fakeReplayBatch) Close() error { return nil }

type fakeReplayRows struct {
	rows [][]any
	idx  int
}

func (r *fakeReplayRows) Next() bool {
	return r.idx < len(r.rows)
}

func (r *fakeReplayRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return fmt.Errorf("scan past end")
	}
	row := r.rows[r.idx]
	r.idx++
	if len(dest) != len(row) {
		return fmt.Errorf("scan dest count=%d row count=%d", len(dest), len(row))
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = row[i].(string)
		default:
			return fmt.Errorf("unsupported scan dest %T", dest[i])
		}
	}
	return nil
}

func (r *fakeReplayRows) ScanStruct(any) error { return fmt.Errorf("not implemented") }
func (r *fakeReplayRows) ColumnTypes() []chdriver.ColumnType {
	return nil
}
func (r *fakeReplayRows) Totals(...any) error { return nil }
func (r *fakeReplayRows) Columns() []string   { return nil }
func (r *fakeReplayRows) Close() error        { return nil }
func (r *fakeReplayRows) Err() error          { return nil }

func columnNameRows(columns []string) [][]any {
	rows := make([][]any, 0, len(columns))
	for _, col := range columns {
		rows = append(rows, []any{col})
	}
	return rows
}

func hashReadRows(rows [][]any) [][]any {
	out := make([][]any, 0, len(rows))
	for _, row := range rows {
		rowIDBytes, ok := row[0].([]byte)
		if !ok {
			panic(fmt.Sprintf("row id has type %T", row[0]))
		}
		values := make([]any, 0, len(row))
		values = append(values, strings.ToLower(hex.EncodeToString(rowIDBytes)))
		for _, v := range row[1:] {
			values = append(values, fmt.Sprint(v))
		}
		out = append(out, values)
	}
	return out
}

func compareReplayRows(got, want [][]any) string {
	if len(got) != len(want) {
		return fmt.Sprintf("rows=%d, want %d", len(got), len(want))
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			return fmt.Sprintf("row %d len=%d, want %d", i, len(got[i]), len(want[i]))
		}
		for j := range got[i] {
			if !valuesEqual(got[i][j], want[i][j]) {
				return fmt.Sprintf("row %d col %d=%v (%T), want %v (%T)", i, j, got[i][j], got[i][j], want[i][j], want[i][j])
			}
		}
	}
	return ""
}

func valuesEqual(a, b any) bool {
	ab, aok := a.([]byte)
	bb, bok := b.([]byte)
	if aok || bok {
		return aok && bok && bytes.Equal(ab, bb)
	}
	return a == b
}

func fixedRowID(seed byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = seed
	}
	return out
}

func fixedRowIDColumn(ids ...[32]byte) *proto.ColFixedStr {
	col := &proto.ColFixedStr{Size: 32}
	for _, id := range ids {
		col.Append(id[:])
	}
	return col
}

func replayStringColumn(values ...string) *proto.ColStr {
	col := new(proto.ColStr)
	for _, value := range values {
		col.Append(value)
	}
	return col
}

func encodeReplayNativeDataPacket(t *testing.T, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(chproto.ClientDataCode))
	buf.PutString("")
	block := proto.Block{Rows: input[0].Data.Rows(), Columns: len(input)}
	if err := block.EncodeBlock(&buf, testNativeReplayRevision, input); err != nil {
		t.Fatalf("encode native block: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func emptyReplayNativeDataPacket(t *testing.T) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(chproto.ClientDataCode))
	buf.PutString("")
	(&proto.BlockInfo{BucketNum: -1}).Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	return append([]byte(nil), buf.Buf...)
}

type replayPayloadEnvelope struct {
	statementID string
	tableID     string
	originalSQL string
	unsafeSQL   string
	dataPackets [][]byte
}

func encodeReplayPayloadEnvelope(t *testing.T, env replayPayloadEnvelope) []byte {
	t.Helper()
	var b bytes.Buffer
	fmt.Fprintln(&b, "# housegate.storage_integrity.insert.v1")
	fmt.Fprintf(&b, "statement_id: %s\n", env.statementID)
	fmt.Fprintf(&b, "table_id: %s\n", env.tableID)
	fmt.Fprintf(&b, "original_sql_length: %d\n", len(env.originalSQL))
	b.WriteString(env.originalSQL)
	b.WriteByte('\n')
	fmt.Fprintf(&b, "unsafe_sql_length: %d\n", len(env.unsafeSQL))
	b.WriteString(env.unsafeSQL)
	b.WriteByte('\n')
	fmt.Fprintf(&b, "data_packet_count: %d\n", len(env.dataPackets))
	for i, packet := range env.dataPackets {
		fmt.Fprintf(&b, "data_packet_%d_length: %d\n", i, len(packet))
		b.Write(packet)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

type mapPayloadStore map[string][]byte

func (m mapPayloadStore) GetPayload(_ context.Context, ref string) ([]byte, error) {
	payload, ok := m[ref]
	if !ok {
		return nil, fmt.Errorf("missing payload %s", ref)
	}
	return append([]byte(nil), payload...), nil
}

type fakeReplaySigner struct {
	workerID string
}

func (s fakeReplaySigner) SignReplayReceipt(_ context.Context, _ string) (string, string, error) {
	return s.workerID, "sig", nil
}

func replayJobForNativePayload(ref, hash string, length uint64) replay.ReplayJob {
	return replay.ReplayJob{
		BlockSeq: 1,
		Statements: []replay.Statement{{
			StatementID:   "stmt-native-1",
			TargetTableID: "app.balances",
			SQL:           "INSERT INTO `hg_unsafe`.`app.balances_a` FORMAT Native",
			PayloadRef:    ref,
			PayloadHash:   hash,
			PayloadLength: length,
		}},
	}
}
