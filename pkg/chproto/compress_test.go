package chproto

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

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

// TestSplice_DataBlock_Compressed_SingleFrame exercises the compressed branch
// by constructing a minimal single-frame compressed Data packet with a fake
// LZ4 frame (method=0x82, compressed_size=9, decompressed_size=1, no payload).
//
// The frame structure (all values chosen so compressed_size-9 == 0 payload):
//
//	[16 bytes: zeroed checksum]
//	[1 byte: 0x82 = LZ4]
//	[4 bytes LE: 9]  (compressed_size = 9 = sub-header only, no payload)
//	[4 bytes LE: 1]  (decompressed_size)
func TestSplice_DataBlock_Compressed_SingleFrame(t *testing.T) {
	rev := 54453

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("") // block_name

	// Build one compressed frame:
	// [16-byte checksum = all zeros][method=0x82][compressedSize=9 LE][decompSize=1 LE]
	frame := make([]byte, frameHeaderSize) // 25 bytes
	// checksum: bytes 0-15, all zero
	frame[16] = 0x82                               // method = LZ4
	binary.LittleEndian.PutUint32(frame[17:21], 9) // compressed_size = 9 (no payload)
	binary.LittleEndian.PutUint32(frame[21:25], 1) // decompressed_size = 1
	buf.Buf = append(buf.Buf, frame...)

	original := append([]byte(nil), buf.Buf...)

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

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")

	frame := make([]byte, frameHeaderSize)
	frame[16] = 0x82
	binary.LittleEndian.PutUint32(frame[17:21], 9)
	binary.LittleEndian.PutUint32(frame[21:25], 1)
	buf.Buf = append(buf.Buf, frame...)
	original := append([]byte(nil), buf.Buf...)

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

// TestWalkDataBlock_CompressedMultiFrame verifies that two consecutive
// compressed frames are consumed as a single Data block. The second frame's
// method byte (0x90 = ZSTD) must be detected via Peek so both frames end up
// in Packet.Raw.
func TestWalkDataBlock_CompressedMultiFrame(t *testing.T) {
	rev := 54453

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")

	// Frame 1: LZ4, compressed_size=9, decompressed_size=1, no payload.
	frame1 := make([]byte, frameHeaderSize)
	frame1[16] = 0x82
	binary.LittleEndian.PutUint32(frame1[17:21], 9)
	binary.LittleEndian.PutUint32(frame1[21:25], 1)
	buf.Buf = append(buf.Buf, frame1...)

	// Frame 2: ZSTD, compressed_size=9, decompressed_size=2, no payload.
	frame2 := make([]byte, frameHeaderSize)
	frame2[16] = 0x90
	binary.LittleEndian.PutUint32(frame2[17:21], 9)
	binary.LittleEndian.PutUint32(frame2[21:25], 2)
	buf.Buf = append(buf.Buf, frame2...)

	original := append([]byte(nil), buf.Buf...)

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
