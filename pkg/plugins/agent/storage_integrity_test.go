package agent

import (
	"bytes"
	"context"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	core "housegate/housegate/pkg/storageintegrity"
)

const agentStorageIntegrityRevision = 54460

func TestStorageIntegrityPluginGeneratesQueryIDAndTracksInsert(t *testing.T) {
	sess := newTestSession(t, 41)
	sess.State().ClientRevision = agentStorageIntegrityRevision
	sess.State().Database = "tenant_a"
	qctx := newTestQueryContext(sess, "INSERT INTO events")

	p := NewStorageIntegrityPlugin(StorageIntegrityConfig{Enabled: true, NetworkID: "sentio-testnet"})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Query.ID == "" {
		t.Fatal("Query.ID was not generated")
	}
	p.mu.Lock()
	state := p.active[sess.ID()]
	p.mu.Unlock()
	if state == nil || state.tableID != "tenant_a.events" || state.statementID != qctx.Query.ID {
		t.Fatalf("state = %+v, query_id=%q", state, qctx.Query.ID)
	}
}

func TestStorageIntegrityPluginRewritesClientNativeData(t *testing.T) {
	sess := newTestSession(t, 42)
	sess.State().ClientRevision = agentStorageIntegrityRevision
	sess.State().Database = "tenant_a"
	qctx := newTestQueryContext(sess, "INSERT INTO events")
	qctx.Query.ID = "stmt-agent"

	p := NewStorageIntegrityPlugin(StorageIntegrityConfig{Enabled: true, NetworkID: "sentio-testnet"})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := encodeAgentNativePacket(t, uint64(proto.ClientCodeData), proto.Input{
		{Name: "id", Data: &proto.ColUInt64{7}},
		{Name: "label", Data: agentColStr("row")},
	}, 1)
	rewritten, err := p.RewriteClientData(context.Background(), qctx, raw)
	if err != nil {
		t.Fatalf("RewriteClientData: %v", err)
	}
	if bytes.Equal(rewritten, raw) {
		t.Fatal("client native data was not rewritten")
	}
	claim, err := core.ComputeNativePayloadClaim("tenant_a.events", agentStorageIntegrityRevision, rewritten)
	if err != nil {
		t.Fatalf("ComputeNativePayloadClaim: %v", err)
	}
	if claim.RowCount != 1 || len(claim.Columns) != 3 || claim.Columns[0].Name != "_hg_row_id" {
		t.Fatalf("claim = %+v", claim)
	}
}

func TestStorageIntegrityPluginStripsServerDataSample(t *testing.T) {
	sess := newTestSession(t, 43)
	sess.State().ClientRevision = agentStorageIntegrityRevision
	sess.State().Database = "tenant_a"
	qctx := newTestQueryContext(sess, "INSERT INTO events")
	qctx.Query.ID = "stmt-agent"

	p := NewStorageIntegrityPlugin(StorageIntegrityConfig{Enabled: true, NetworkID: "sentio-testnet"})
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := encodeAgentNativePacket(t, uint64(proto.ServerCodeData), proto.Input{
		{Name: "_hg_row_id", Data: &proto.ColFixedStr{Size: 32}},
		{Name: "id", Data: &proto.ColUInt64{}},
		{Name: "label", Data: agentColStr()},
	}, 0)

	rewritten, err := p.RewriteServerData(context.Background(), sess, raw)
	if err != nil {
		t.Fatalf("RewriteServerData: %v", err)
	}
	if bytes.Equal(rewritten, raw) {
		t.Fatal("server sample was not rewritten")
	}
	reader := proto.NewReader(bytes.NewReader(rewritten))
	code, err := reader.UVarInt()
	if err != nil {
		t.Fatalf("packet code: %v", err)
	}
	if code != uint64(chproto.ServerDataCode) {
		t.Fatalf("packet code = %d, want ServerData", code)
	}
	if _, err := reader.Str(); err != nil {
		t.Fatalf("block name: %v", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(reader, agentStorageIntegrityRevision, results.Auto()); err != nil {
		t.Fatalf("decode block: %v", err)
	}
	if block.Rows != 0 || len(results) != 2 || results[0].Name != "id" || results[1].Name != "label" {
		t.Fatalf("block rows=%d results=%+v, want id,label sample", block.Rows, results)
	}
}

func encodeAgentNativePacket(t *testing.T, code uint64, input proto.Input, rows int) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(code)
	buf.PutString("")
	if err := (proto.Block{Rows: rows, Columns: len(input)}).EncodeBlock(&buf, agentStorageIntegrityRevision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return buf.Buf
}

func agentColStr(values ...string) *proto.ColStr {
	col := new(proto.ColStr)
	for _, value := range values {
		col.Append(value)
	}
	return col
}
