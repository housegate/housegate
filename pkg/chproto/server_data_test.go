package chproto

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestNormalizeServerDataBlockInfo(t *testing.T) {
	for _, revision := range []int{51902, 51903, 54454, 54470} {
		for _, field3 := range []bool{false, true} {
			if revision < 51903 && field3 {
				continue
			}
			var packet, body proto.Buffer
			packet.PutUVarInt(uint64(proto.ServerCodeData))
			packet.PutString("")
			id := proto.ColUInt64{42}
			if err := (proto.Block{Rows: 1, Columns: 1, Info: proto.BlockInfo{BucketNum: -1}}).EncodeBlock(&body, revision, proto.Input{{Name: "id", Data: &id}}); err != nil {
				t.Fatal(err)
			}
			want := append(append([]byte(nil), packet.Buf...), body.Buf...)
			if field3 {
				// The ordinary info is 1,false,2,-1,0. Insert the server's
				// out_of_order_buckets vector before the end sentinel.
				packet.Buf = append(packet.Buf, body.Buf[:7]...)
				packet.PutUVarInt(3)
				packet.PutUVarInt(2)
				packet.PutInt32(7)
				packet.PutInt32(9)
				packet.PutUVarInt(0)
				packet.Buf = append(packet.Buf, body.Buf[8:]...)
			} else {
				packet.Buf = append(packet.Buf, body.Buf...)
			}
			got, err := NormalizeServerDataBlockInfo(packet.Buf, revision)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("revision=%d field3=%v err=%v got=%x want=%x", revision, field3, err, got, want)
			}
			pr := proto.NewReader(bytes.NewReader(got))
			pr.UVarInt()
			pr.Str()
			var result proto.Results
			var block proto.Block
			if err := block.DecodeBlock(pr, revision, result.Auto()); err != nil || block.Rows != 1 || len(result) != 1 {
				t.Fatalf("normalized row decode: %v", err)
			}
		}
	}
}

func TestNormalizeServerDataBlockInfoRejectsTruncatedCounts(t *testing.T) {
	for _, count := range []uint64{1, 1 << 40, ^uint64(0)} {
		var b proto.Buffer
		b.PutUVarInt(uint64(proto.ServerCodeData))
		b.PutString("")
		b.PutUVarInt(3)
		b.PutUVarInt(count)
		if _, err := NormalizeServerDataBlockInfo(b.Buf, 54470); err == nil {
			t.Fatalf("accepted truncated count %d", count)
		}
	}
	var hugeName [10]byte
	n := binary.PutUvarint(hugeName[:], ^uint64(0))
	if _, err := NormalizeServerDataBlockInfo(append([]byte{byte(proto.ServerCodeData)}, hugeName[:n]...), 54470); err == nil {
		t.Fatal("accepted impossible name length")
	}
}
