package lthashplugin

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/lthash"
)

// testRevision matches the revision used across housegate codec tests.
const testRevision = 54453

func encodeClientDataPacket(t *testing.T, rows int, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("") // block name (always empty from real clients)
	block := proto.Block{Rows: rows, Columns: len(input)}
	if err := block.EncodeBlock(&buf, testRevision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return buf.Buf
}

func TestDecodeClientDataMatchesDirectRowHash(t *testing.T) {
	users := new(proto.ColStr)
	users.Append("0x123")
	users.Append("0x123") // duplicate row on purpose
	users.Append("0xabc")
	balances := proto.ColUInt64{10, 10, 20}

	raw := encodeClientDataPacket(t, 3, proto.Input{
		{Name: "user_id", Data: users},
		{Name: "balance", Data: &balances},
	})

	blk, err := DecodeClientData(raw, testRevision)
	if err != nil {
		t.Fatalf("DecodeClientData: %v", err)
	}
	if blk.Rows != 3 {
		t.Fatalf("rows = %d, want 3", blk.Rows)
	}

	acc := lthash.New()
	for i := 0; i < blk.Rows; i++ {
		values, err := blk.Row(i)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		h, err := lthash.RowHash("balances", blk.Columns, values)
		if err != nil {
			t.Fatalf("row hash %d: %v", i, err)
		}
		acc.AddHash(h)
	}

	// The same rows hashed directly, without any wire round-trip.
	cols := []lthash.Column{{Name: "user_id", Type: "String"}, {Name: "balance", Type: "UInt64"}}
	want := lthash.New()
	for _, row := range [][]any{
		{"0x123", uint64(10)},
		{"0x123", uint64(10)},
		{"0xabc", uint64(20)},
	} {
		h, err := lthash.RowHash("balances", cols, row)
		if err != nil {
			t.Fatalf("direct row hash: %v", err)
		}
		want.AddHash(h)
	}

	if !acc.Equal(want) {
		t.Fatalf("wire-decoded accumulator != direct accumulator: %s vs %s", acc.Hex(), want.Hex())
	}
}

func TestDecodeClientDataTerminatorBlock(t *testing.T) {
	raw := encodeClientDataPacket(t, 0, proto.Input{})
	blk, err := DecodeClientData(raw, testRevision)
	if err != nil {
		t.Fatalf("DecodeClientData(terminator): %v", err)
	}
	if blk.Rows != 0 {
		t.Fatalf("terminator block rows = %d, want 0", blk.Rows)
	}
}

func TestDecodeClientDataRejectsNonDataPacket(t *testing.T) {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeQuery))
	if _, err := DecodeClientData(buf.Buf, testRevision); err == nil {
		t.Fatal("non-Data packet must be rejected")
	}
}
