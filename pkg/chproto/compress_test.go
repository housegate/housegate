package chproto

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	chcompress "github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// buildEmptyUncompressedDataPacket constructs the wire bytes for an empty
// uncompressed ClientCodeData (or ServerCodeData) packet:
//
//	[VarUInt: ClientCodeData=2]
//	[String: block_name=""]
//	[BlockInfo: field1(overflows=false), field2(bucket_num=-1), field0(end)]
//	[UVarInt: num_columns=0]
//	[UVarInt: num_rows=0]
//
// This mirrors what ClickHouse sends as an empty Data block to signal end-of-stream.
func buildEmptyUncompressedDataPacket(t *testing.T) []byte {
	t.Helper()
	var buf proto.Buffer
	// Leading type byte: ClientCodeData == 2 (also ServerCodeData).
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	// block_name: empty string.
	buf.PutString("")
	// BlockInfo with standard default values. BucketNum -1 is the default.
	bi := proto.BlockInfo{BucketNum: -1}
	bi.Encode(&buf)
	// num_columns = 0, num_rows = 0
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	return append([]byte(nil), buf.Buf...)
}

func buildNativeBlockPayload(t *testing.T, rev int, values proto.ColUInt8) []byte {
	t.Helper()
	var payload proto.Buffer
	block := proto.Block{
		Info:    proto.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    len(values),
	}
	if err := block.EncodeBlock(&payload, rev, []proto.InputColumn{
		{Name: "value", Data: values},
	}); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return append([]byte(nil), payload.Buf...)
}

func buildTupleNativeBlockPayload(rev int) []byte {
	var payload proto.Buffer
	(proto.BlockInfo{BucketNum: -1}).Encode(&payload)
	payload.PutUVarInt(1) // columns
	payload.PutUVarInt(1) // rows
	payload.PutString("value")
	payload.PutString("Tuple(UInt8, UInt8)")
	if proto.FeatureCustomSerialization.In(rev) {
		payload.PutBool(false)
	}
	// Tuple is serialized column-wise: all values for element 1, then element 2.
	payload.PutUInt8(1)
	payload.PutUInt8(2)
	return append([]byte(nil), payload.Buf...)
}

func buildAggregateStateNativeBlockPayload(rev int, typeName string, state []byte) []byte {
	var payload proto.Buffer
	(proto.BlockInfo{BucketNum: -1}).Encode(&payload)
	payload.PutUVarInt(1) // columns
	payload.PutUVarInt(1) // rows
	payload.PutString("state")
	payload.PutString(typeName)
	if proto.FeatureCustomSerialization.In(rev) {
		payload.PutBool(false)
	}
	payload.Buf = append(payload.Buf, state...)
	return append([]byte(nil), payload.Buf...)
}

func buildCompressedFrame(t *testing.T, payload []byte, method chcompress.Method) []byte {
	t.Helper()
	writer := chcompress.NewWriter(0, method)
	if err := writer.Compress(payload); err != nil {
		t.Fatalf("Compress: %v", err)
	}
	return append([]byte(nil), writer.Data...)
}

func buildCompressedDataPrefix() []byte {
	var prefix proto.Buffer
	prefix.PutUVarInt(uint64(proto.ClientCodeData))
	prefix.PutString("")
	return append([]byte(nil), prefix.Buf...)
}

// TestSplice_DataBlock_Empty_Uncompressed proves walkDataBlock consumes an
// empty Data block (columns=0, rows=0) via the fast path, and Splice forwards
// the full packet bytes unchanged.
func TestSplice_DataBlock_Empty_Uncompressed(t *testing.T) {
	rev := 54453
	original := buildEmptyUncompressedDataPacket(t)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), original...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionDisabled)

	pkt, err := c.ReadPacket() // no decodeTypes → goes through skipPacketBody → walkDataBlock
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if pkt.Type != uint64(proto.ClientCodeData) {
		t.Fatalf("Type=%d, want ClientCodeData(%d)", pkt.Type, proto.ClientCodeData)
	}
	if pkt.Decoded != nil {
		t.Fatalf("Decoded should be nil for unrequested type, got %T", pkt.Decoded)
	}

	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), original) {
		t.Fatalf("Splice dst=%x\nwant     %x", dst.Bytes(), original)
	}
}

func TestClientDataPacketIsEmptyUncompressed(t *testing.T) {
	empty, err := ClientDataPacketIsEmpty(buildEmptyUncompressedDataPacket(t), proto.CompressionDisabled)
	if err != nil {
		t.Fatalf("ClientDataPacketIsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("empty ClientData block was not recognized as input completion")
	}
}

func TestClientDataPacketIsEmptyRejectsNonEmptyBlock(t *testing.T) {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	(proto.BlockInfo{BucketNum: -1}).Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(1)

	empty, err := ClientDataPacketIsEmpty(buf.Buf, proto.CompressionDisabled)
	if err != nil {
		t.Fatalf("ClientDataPacketIsEmpty: %v", err)
	}
	if empty {
		t.Fatal("non-empty ClientData block was recognized as input completion")
	}
}

func TestClientDataPacketIsEmptyCompressed(t *testing.T) {
	var wire bytes.Buffer
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &wire}, DirFromClient)
	c.SetCompression(proto.CompressionEnabled)
	if err := c.WriteEmptyDataBlock(); err != nil {
		t.Fatalf("WriteEmptyDataBlock: %v", err)
	}

	empty, err := ClientDataPacketIsEmpty(wire.Bytes(), proto.CompressionEnabled)
	if err != nil {
		t.Fatalf("ClientDataPacketIsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("compressed empty ClientData block was not recognized as input completion")
	}
}

func TestReadPacketWithDataLimitStopsBeforeBufferingFullPacket(t *testing.T) {
	original := buildEmptyUncompressedDataPacket(t)
	source := bytes.NewBuffer(append([]byte(nil), original...))
	c := NewCodec(&readerWriter{r: source, w: &bytes.Buffer{}}, DirFromClient)
	c.SetCompression(proto.CompressionDisabled)

	limit := uint64(len(original) - 1)
	pkt, err := c.ReadPacketWithDataLimit(limit)
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("ReadPacketWithDataLimit err = %v, want ErrPacketTooLarge", err)
	}
	if pkt == nil || uint64(pkt.RawLen) > limit {
		t.Fatalf("partial packet = %#v, want captured bytes <= %d", pkt, limit)
	}
	if uint64(len(c.cap.cap)) > limit || c.br.Buffered() == 0 {
		t.Fatalf("capture len/buffered = %d/%d, want capture <= %d with unread buffered bytes", len(c.cap.cap), c.br.Buffered(), limit)
	}
}

// TestSplice_DataBlock_Empty_Uncompressed_ServerSide exercises the same path
// from the DirToUpstream direction (server→client ServerCodeData packet).
func TestSplice_DataBlock_Empty_Uncompressed_ServerSide(t *testing.T) {
	rev := 54453
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	bi := proto.BlockInfo{BucketNum: -1}
	bi.Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	original := append([]byte(nil), buf.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), original...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionDisabled)

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), original) {
		t.Fatalf("Splice dst=%x\nwant     %x", dst.Bytes(), original)
	}
}

func TestReadPacket_UnknownAggregateStateFailsClosed(t *testing.T) {
	const rev = MaxSupportedRevision
	payload := buildAggregateStateNativeBlockPayload(
		rev,
		"AggregateFunction(uniq, UInt64)",
		[]byte{1, 2, 3, 4},
	)
	var wire proto.Buffer
	wire.PutUVarInt(uint64(proto.ServerCodeData))
	wire.PutString("")
	wire.Buf = append(wire.Buf, payload...)

	rw := &readerWriter{r: bytes.NewBuffer(wire.Buf), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionDisabled)

	pkt, err := c.ReadPacket()
	if !errors.Is(err, ErrUnsupportedResultType) {
		t.Fatalf("ReadPacket err=%v, want ErrUnsupportedResultType", err)
	}
	if pkt == nil || pkt.Type != uint64(proto.ServerCodeData) {
		t.Fatalf("partial packet=%#v, want ServerCodeData metadata", pkt)
	}
}

// TestSplice_DataBlock_Compressed_SingleFrame exercises the compressed branch
// with a valid LZ4-framed Native block and verifies byte-for-byte forwarding.
func TestSplice_DataBlock_Compressed_SingleFrame(t *testing.T) {
	rev := 54453
	values := make(proto.ColUInt8, 256)
	payload := buildNativeBlockPayload(t, rev, values)
	original := append(buildCompressedDataPrefix(), buildCompressedFrame(t, payload, chcompress.LZ4)...)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), original...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if pkt.Type != uint64(proto.ClientCodeData) {
		t.Fatalf("Type=%d, want ClientCodeData(%d)", pkt.Type, proto.ClientCodeData)
	}

	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), original) {
		t.Fatalf("Splice dst=%x\nwant     %x", dst.Bytes(), original)
	}
}

func TestReadPacket_CompressedDataSingleFrameDoesNotWaitForNextPacket(t *testing.T) {
	rev := 54480
	values := make(proto.ColUInt8, 256)
	payload := buildNativeBlockPayload(t, rev, values)
	original := append(buildCompressedDataPrefix(), buildCompressedFrame(t, payload, chcompress.LZ4)...)

	readerSide, writerSide := net.Pipe()
	defer readerSide.Close()
	defer writerSide.Close()

	writeDone := make(chan error, 1)
	go func() {
		_, err := writerSide.Write(original)
		writeDone <- err
	}()

	c := NewCodec(readerSide, DirFromClient)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	readDone := make(chan struct {
		pkt *Packet
		err error
	}, 1)
	go func() {
		pkt, err := c.ReadPacket()
		readDone <- struct {
			pkt *Packet
			err error
		}{pkt: pkt, err: err}
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("ReadPacket: %v", res.err)
		}
		if res.pkt == nil {
			t.Fatal("ReadPacket returned nil packet")
		}
		if !bytes.Equal(res.pkt.Raw, original) {
			t.Fatalf("Raw=%x\nwant %x", res.pkt.Raw, original)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ReadPacket blocked waiting for bytes from the next packet")
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("writer did not finish")
	}
}

func TestReadPacket_CompressedDataStopsBeforeFollowingPacket(t *testing.T) {
	rev := 54480
	values := make(proto.ColUInt8, 256)
	payload := buildNativeBlockPayload(t, rev, values)
	var dataPrefix proto.Buffer
	dataPrefix.PutUVarInt(uint64(proto.ServerCodeData))
	dataPrefix.PutString("")
	dataPacket := append(
		append([]byte(nil), dataPrefix.Buf...),
		buildCompressedFrame(t, payload, chcompress.LZ4)...,
	)

	var eos proto.Buffer
	eos.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	wire := append(append([]byte(nil), dataPacket...), eos.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(wire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	data, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(Data): %v", err)
	}
	if !bytes.Equal(data.Raw, dataPacket) {
		t.Fatalf("Data Raw=%x\nwant %x", data.Raw, dataPacket)
	}

	terminal, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(EndOfStream): %v", err)
	}
	if terminal.Type != uint64(proto.ServerCodeEndOfStream) {
		t.Fatalf("terminal Type=%d, want EndOfStream(%d)", terminal.Type, proto.ServerCodeEndOfStream)
	}
}

func TestReadPacket_CompressedDataRejectsMultipleBlocksInOneFrame(t *testing.T) {
	rev := MaxSupportedRevision
	first := buildNativeBlockPayload(t, rev, proto.ColUInt8{1})
	second := buildNativeBlockPayload(t, rev, proto.ColUInt8{2})
	framePayload := append(append([]byte(nil), first...), second...)
	var prefix proto.Buffer
	prefix.PutUVarInt(uint64(proto.ServerCodeData))
	prefix.PutString("")
	packet := append(
		append([]byte(nil), prefix.Buf...),
		buildCompressedFrame(t, framePayload, chcompress.LZ4)...,
	)

	rw := &readerWriter{r: bytes.NewBuffer(packet), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	pkt, err := c.ReadPacket()
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("ReadPacket err=%v, want ErrMalformed", err)
	}
	if pkt == nil || pkt.Type != uint64(proto.ServerCodeData) {
		t.Fatalf("partial packet=%#v, want ServerCodeData metadata", pkt)
	}
}

func TestReadPacket_CompressedTupleStopsBeforeFollowingEndOfStream(t *testing.T) {
	rev := 54480
	payload := buildTupleNativeBlockPayload(rev)

	var dataPrefix proto.Buffer
	dataPrefix.PutUVarInt(uint64(proto.ServerCodeData))
	dataPrefix.PutString("")
	dataPacket := append(
		append([]byte(nil), dataPrefix.Buf...),
		buildCompressedFrame(t, payload, chcompress.LZ4)...,
	)

	var eos proto.Buffer
	eos.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	wire := append(append([]byte(nil), dataPacket...), eos.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(wire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	data, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(Tuple Data): %v", err)
	}
	if !bytes.Equal(data.Raw, dataPacket) {
		t.Fatalf("Tuple Data Raw=%x\nwant %x", data.Raw, dataPacket)
	}

	terminal, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(EndOfStream): %v", err)
	}
	if terminal.Type != uint64(proto.ServerCodeEndOfStream) {
		t.Fatalf("terminal Type=%d, want EndOfStream(%d)", terminal.Type, proto.ServerCodeEndOfStream)
	}
}

// TestReadPacket_CompressedDataWaitsForDelayedSecondFrame proves that the
// logical Native block, rather than the currently-buffered compression
// frames, is the packet boundary.
func TestReadPacket_CompressedDataWaitsForDelayedSecondFrame(t *testing.T) {
	rev := 54480
	values := make(proto.ColUInt8, 4096)
	payload := buildNativeBlockPayload(t, rev, values)
	split := len(payload) - 1024

	firstWrite := append(buildCompressedDataPrefix(), buildCompressedFrame(t, payload[:split], chcompress.LZ4)...)
	secondWrite := buildCompressedFrame(t, payload[split:], chcompress.ZSTD)
	original := append(append([]byte(nil), firstWrite...), secondWrite...)

	readerSide, writerSide := net.Pipe()
	defer readerSide.Close()
	defer writerSide.Close()

	c := NewCodec(readerSide, DirFromClient)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	readDone := make(chan struct {
		pkt *Packet
		err error
	}, 1)
	go func() {
		pkt, err := c.ReadPacket()
		readDone <- struct {
			pkt *Packet
			err error
		}{pkt: pkt, err: err}
	}()

	firstWriteDone := make(chan error, 1)
	go func() {
		_, err := writerSide.Write(firstWrite)
		firstWriteDone <- err
	}()
	select {
	case err := <-firstWriteDone:
		if err != nil {
			t.Fatalf("first writer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first writer did not finish")
	}

	select {
	case res := <-readDone:
		t.Fatalf("ReadPacket returned before the logical block was complete: pkt=%#v err=%v", res.pkt, res.err)
	case <-time.After(100 * time.Millisecond):
	}

	secondWriteDone := make(chan error, 1)
	go func() {
		_, err := writerSide.Write(secondWrite)
		secondWriteDone <- err
	}()

	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("ReadPacket: %v", res.err)
		}
		if res.pkt == nil {
			t.Fatal("ReadPacket returned nil packet")
		}
		if !bytes.Equal(res.pkt.Raw, original) {
			t.Fatalf("Raw=%x\nwant %x", res.pkt.Raw, original)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadPacket did not finish after the second frame arrived")
	}

	select {
	case err := <-secondWriteDone:
		if err != nil {
			t.Fatalf("second writer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second writer did not finish")
	}
}

// TestWalkDataBlock_CompressedMultiFrame verifies that two compression frames
// already buffered together are consumed as one logical Data block.
func TestWalkDataBlock_CompressedMultiFrame(t *testing.T) {
	rev := 54453
	values := make(proto.ColUInt8, 4096)
	payload := buildNativeBlockPayload(t, rev, values)
	split := len(payload) - 1024
	original := append(buildCompressedDataPrefix(), buildCompressedFrame(t, payload[:split], chcompress.LZ4)...)
	original = append(original, buildCompressedFrame(t, payload[split:], chcompress.ZSTD)...)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), original...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}

	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), original) {
		t.Fatalf("Splice dst=%x\nwant     %x", dst.Bytes(), original)
	}
}

func TestReadPacket_CompressedTableColumnsStopsBeforeFollowingPacket(t *testing.T) {
	rev := revisionMinCompressedLogsProfileEventsColumns

	var body proto.Buffer
	body.PutString("db.table")
	body.PutString("columns format version: 1\n1 columns:\n`value` UInt8")

	var prefix proto.Buffer
	prefix.PutUVarInt(uint64(proto.ServerCodeTableColumns))
	tableColumnsPacket := append(
		append([]byte(nil), prefix.Buf...),
		buildCompressedFrame(t, body.Buf, chcompress.LZ4)...,
	)

	var eos proto.Buffer
	eos.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	wire := append(append([]byte(nil), tableColumnsPacket...), eos.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(wire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	tableColumns, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(TableColumns): %v", err)
	}
	if !bytes.Equal(tableColumns.Raw, tableColumnsPacket) {
		t.Fatalf("TableColumns Raw=%x\nwant %x", tableColumns.Raw, tableColumnsPacket)
	}

	terminal, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(EndOfStream): %v", err)
	}
	if terminal.Type != uint64(proto.ServerCodeEndOfStream) {
		t.Fatalf("terminal Type=%d, want EndOfStream(%d)", terminal.Type, proto.ServerCodeEndOfStream)
	}
}

func TestReadPacket_ProfileEventsRemainUncompressedBeforeRevision54481(t *testing.T) {
	rev := MaxSupportedRevision
	values := proto.ColUInt8{1}
	payload := buildNativeBlockPayload(t, rev, values)

	var profileEvents proto.Buffer
	profileEvents.PutUVarInt(uint64(ServerProfileEventsCode))
	profileEvents.PutString("")
	profileEvents.Buf = append(profileEvents.Buf, payload...)
	packet := append([]byte(nil), profileEvents.Buf...)

	var eos proto.Buffer
	eos.PutUVarInt(uint64(proto.ServerCodeEndOfStream))
	wire := append(append([]byte(nil), packet...), eos.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(wire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)
	c.SetCompression(proto.CompressionEnabled)

	profile, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(ProfileEvents): %v", err)
	}
	if !bytes.Equal(profile.Raw, packet) {
		t.Fatalf("ProfileEvents Raw=%x\nwant %x", profile.Raw, packet)
	}

	terminal, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(EndOfStream): %v", err)
	}
	if terminal.Type != uint64(proto.ServerCodeEndOfStream) {
		t.Fatalf("terminal Type=%d, want EndOfStream(%d)", terminal.Type, proto.ServerCodeEndOfStream)
	}
}

// TestWalkDataBlock_DefaultCompressionIsDisabled verifies that a Codec without
// SetCompression called treats Data blocks as uncompressed (safe default).
func TestWalkDataBlock_DefaultCompressionIsDisabled(t *testing.T) {
	rev := 54453
	original := buildEmptyUncompressedDataPacket(t)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), original...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(rev)
	// Note: SetCompression NOT called — should default to CompressionDisabled.

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), original) {
		t.Fatalf("Splice dst=%x\nwant     %x", dst.Bytes(), original)
	}
}
