package chproto

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func chgoSampleBlock(t *testing.T, rev int) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	id := proto.ColUInt64{}
	region := proto.ColStr{}
	input := proto.Input{{Name: "id", Data: &id}, {Name: "region", Data: &region}}
	if err := (proto.Block{Rows: 0, Columns: 2}).EncodeBlock(&buf, rev, input); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestWriteSampleBlock_MatchesChGoEncodingAcrossRevisions(t *testing.T) {
	// 54453 predates FeatureCustomSerialization (54454); 54460 / 54470 carry
	// the per-column "has custom serialization" flag.
	for _, rev := range []int{54453, 54460, 54470} {
		var out bytes.Buffer
		c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
		c.SetRevision(rev)
		if err := c.WriteSampleBlock([]SampleColumn{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}); err != nil {
			t.Fatalf("rev %d: WriteSampleBlock: %v", rev, err)
		}
		if want := chgoSampleBlock(t, rev); !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("rev %d: sample block bytes\n got %x\nwant %x", rev, out.Bytes(), want)
		}
	}
}

func TestWriteSampleBlock_DecodesAsZeroRowBlockWithColumns(t *testing.T) {
	var out bytes.Buffer
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
	c.SetRevision(54460)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}); err != nil {
		t.Fatalf("WriteSampleBlock: %v", err)
	}
	r := proto.NewReader(bytes.NewReader(out.Bytes()))
	code, err := r.UVarInt()
	if err != nil || code != uint64(proto.ServerCodeData) {
		t.Fatalf("packet code = %d err=%v, want ServerCodeData", code, err)
	}
	if name, err := r.Str(); err != nil || name != "" {
		t.Fatalf("block name = %q err=%v", name, err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, 54460, results.Auto()); err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if block.Rows != 0 || block.Columns != 2 || len(results) != 2 {
		t.Fatalf("block rows=%d cols=%d results=%d", block.Rows, block.Columns, len(results))
	}
	if results[0].Name != "id" || results[0].Data.Type() != "UInt64" || results[1].Name != "region" || results[1].Data.Type() != "String" {
		t.Fatalf("columns = %s %s / %s %s", results[0].Name, results[0].Data.Type(), results[1].Name, results[1].Data.Type())
	}
}

func TestWriteSampleBlock_ChunkFramesWhenNegotiated(t *testing.T) {
	var out bytes.Buffer
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
	c.SetRevision(54470)
	c.EnableChunked(false, true)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "v", Type: "UInt64"}}); err != nil {
		t.Fatalf("WriteSampleBlock: %v", err)
	}
	chunk, err := NewChunkedReader(bytes.NewReader(out.Bytes()), true).ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	var want proto.Buffer
	want.PutUVarInt(uint64(proto.ServerCodeData))
	want.PutString("")
	v := proto.ColUInt64{}
	if err := (proto.Block{Rows: 0, Columns: 1}).EncodeBlock(&want, 54470, proto.Input{{Name: "v", Data: &v}}); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	if !bytes.Equal(chunk, want.Buf) {
		t.Fatalf("chunk payload mismatch\n got %x\nwant %x", chunk, want.Buf)
	}
}

func TestWriteSampleBlock_RejectsUnnamedOrUntypedColumns(t *testing.T) {
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}, DirFromClient)
	c.SetRevision(54460)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "", Type: "UInt64"}}); err == nil {
		t.Fatal("unnamed column must be rejected")
	}
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "v", Type: ""}}); err == nil {
		t.Fatal("untyped column must be rejected")
	}
	if err := c.WriteSampleBlock(nil); err == nil {
		t.Fatal("a sample block needs at least one column")
	}
}
