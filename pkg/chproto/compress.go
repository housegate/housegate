package chproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// ClientDataPacketIsEmpty reports whether raw is the protocol-level empty
// ClientData block that terminates one query's client input stream.
func ClientDataPacketIsEmpty(raw []byte, compression proto.Compression) (bool, error) {
	r := bytes.NewReader(raw)
	code, err := binary.ReadUvarint(r)
	if err != nil {
		return false, fmt.Errorf("client data packet code: %w", err)
	}
	if code != uint64(proto.ClientCodeData) {
		return false, fmt.Errorf("packet type %d is not ClientData", code)
	}
	nameLen, err := binary.ReadUvarint(r)
	if err != nil {
		return false, fmt.Errorf("client data block name length: %w", err)
	}
	if nameLen > uint64(r.Len()) {
		return false, fmt.Errorf("client data block name length %d exceeds remaining packet bytes %d", nameLen, r.Len())
	}
	if _, err := r.Seek(int64(nameLen), io.SeekCurrent); err != nil {
		return false, fmt.Errorf("client data block name: %w", err)
	}

	var body io.Reader = r
	if compression == proto.CompressionEnabled {
		body = compress.NewReader(r)
	}
	pr := proto.NewReader(body)
	if _, _, err := decodeBlockInfoCompat(pr); err != nil {
		return false, fmt.Errorf("client data BlockInfo: %w", err)
	}
	columns, err := pr.UVarInt()
	if err != nil {
		return false, fmt.Errorf("client data columns: %w", err)
	}
	rows, err := pr.UVarInt()
	if err != nil {
		return false, fmt.Errorf("client data rows: %w", err)
	}
	return columns == 0 && rows == 0, nil
}

// Compressed frame layout constants (ClickHouse native protocol):
//
//	[16 bytes: CityHash128 checksum]
//	[1 byte:   compression method (0x82=LZ4, 0x90=ZSTD, 0x02=none)]
//	[4 bytes LE: compressed_size (includes the 9-byte sub-header below)]
//	[4 bytes LE: decompressed_size]
//	[compressed_size - 9 bytes: compressed payload]
//
// Total frame = 16 (checksum) + compressed_size bytes.
const (
	frameHeaderSize     = 16 + 1 + 4 + 4 // 25 bytes
	maxCompressedFrame  = 32 * 1024 * 1024
	maxDecompressedSize = 256 * 1024 * 1024
)

// blockInfoCompat is a minimal representation of ClickHouse BlockInfo.
// It matches the on-wire field layout for ClickHouse 26.x which added field 3
// (out_of_order_buckets). We decode-and-discard (bytes are already captured by
// the tee); no re-encode is needed.
type blockInfoCompat struct {
	Overflows            bool
	BucketNum            int
	OutOfOrderBuckets    []int32
	HasOutOfOrderBuckets bool
}

// decodeBlockInfoCompat reads a BlockInfo from r using the compat decoder that
// handles the optional field 3 added in ClickHouse 26.x. Bytes are consumed
// from the proto.Reader (which is tee-backed), so the tee captures them.
// Returns the number of bytes consumed for the caller to Advance the tee cursor.
func decodeBlockInfoCompat(r *proto.Reader) (*blockInfoCompat, int, error) {
	info := &blockInfoCompat{BucketNum: -1}
	consumed := 0
	for {
		f, err := r.UVarInt()
		if err != nil {
			return nil, consumed, fmt.Errorf("BlockInfo field id: %w", err)
		}
		consumed += uvarintEncLen(f)
		switch f {
		case 0: // end sentinel
			return info, consumed, nil
		case 1: // is_overflows (bool = 1 byte)
			v, err := r.Bool()
			if err != nil {
				return nil, consumed, fmt.Errorf("BlockInfo overflows: %w", err)
			}
			info.Overflows = v
			consumed++ // bool = 1 byte
		case 2: // bucket_num (Int32 = 4 bytes)
			v, err := r.Int32()
			if err != nil {
				return nil, consumed, fmt.Errorf("BlockInfo bucket_num: %w", err)
			}
			info.BucketNum = int(v)
			consumed += 4
		case 3: // out_of_order_buckets (vector<Int32>), ClickHouse 26.x+
			info.HasOutOfOrderBuckets = true
			count, err := r.UVarInt()
			if err != nil {
				return nil, consumed, fmt.Errorf("BlockInfo out_of_order_buckets count: %w", err)
			}
			consumed += uvarintEncLen(count)
			buckets := make([]int32, count)
			for i := uint64(0); i < count; i++ {
				v, err := r.Int32()
				if err != nil {
					return nil, consumed, fmt.Errorf("BlockInfo out_of_order_buckets[%d]: %w", i, err)
				}
				buckets[i] = v
				consumed += 4
			}
			info.OutOfOrderBuckets = buckets
		default:
			// Field type is not self-describing; cannot safely skip unknown fields.
			return nil, consumed, fmt.Errorf("BlockInfo unknown field %d (cannot skip safely)", f)
		}
	}
}

// walkDataBlock consumes one Data-block body. The block_name (short string)
// is read through the codec's long-lived proto.Reader — bytes land in the
// capture automatically. Subsequent compressed payload reads bypass that
// one-byte-at-a-time layer via readFullRaw for throughput; uncompressed
// blocks fall back to DecodeRawBlock on c.r.
//
// Uses c.Compression() to pick between the compressed (multi-frame LZ4/ZSTD
// passthrough) and uncompressed (BlockInfo + empty-block fast path +
// DecodeRawBlock) branches — matching legacy proxy.go:handleDataBlock.
func (c *Codec) walkDataBlock() error {
	// block_name is always a short uncompressed string in the plaintext stream.
	if _, err := c.r.Str(); err != nil {
		return fmt.Errorf("data block name: %w", err)
	}

	if c.Compression() == proto.CompressionEnabled {
		return c.walkCompressedFrames()
	}
	return c.walkUncompressedBlock()
}

// walkCompressedFrames consumes one or more consecutive compressed frames
// that make up a single logical Data block. Reads go directly through c.br
// via readFullRaw — the one-byte-at-a-time capture path that proto.Reader
// uses would cost ~5 function calls per byte, unacceptable for MB-sized
// payloads. Multi-frame detection uses c.br.Peek, which is correct because
// no other reader layer prefetches under the new codec design.
//
// Port of pkg/proxy/proxy.go:handleDataBlock compressed branch.
func (c *Codec) walkCompressedFrames() error {
	for {
		headerBuf, err := c.readFullRaw(frameHeaderSize)
		if err != nil {
			return fmt.Errorf("compressed frame header: %w", err)
		}

		// Extract compressed_size from header[17:21] (little-endian uint32).
		compressedSize := binary.LittleEndian.Uint32(headerBuf[17:21])
		if compressedSize < 9 {
			return fmt.Errorf("invalid compressed_size %d (< 9)", compressedSize)
		}
		if compressedSize > maxCompressedFrame {
			return fmt.Errorf("compressed_size %d exceeds limit %d", compressedSize, maxCompressedFrame)
		}

		// The 9 bytes of sub-header are already inside the 25-byte header we
		// just read; the remaining payload is (compressedSize - 9) bytes.
		remainingDataSize := int(compressedSize) - 9
		if remainingDataSize > 0 {
			if _, err := c.readFullRaw(remainingDataSize); err != nil {
				return fmt.Errorf("compressed frame data: %w", err)
			}
		}

		// Peek at the next 25 bytes in c.br (without consuming) to detect a
		// following frame, but only when those bytes are already buffered.
		// bufio.Reader.Peek blocks until the requested size is available; at a
		// packet boundary that would wait for the next client packet and stall
		// the relay after a single-frame compressed Data block.
		if c.br.Buffered() < frameHeaderSize {
			break
		}
		nextBytes, err := c.br.Peek(frameHeaderSize)
		if err != nil || len(nextBytes) < frameHeaderSize {
			break
		}
		methodByte := nextBytes[16]
		if methodByte != 0x82 && methodByte != 0x90 && methodByte != 0x02 {
			break
		}
		peekCompSize := binary.LittleEndian.Uint32(nextBytes[17:21])
		peekDecompSize := binary.LittleEndian.Uint32(nextBytes[21:25])
		if peekCompSize < 9 || peekCompSize > maxCompressedFrame ||
			peekDecompSize == 0 || peekDecompSize > maxDecompressedSize {
			break
		}
		// Continue to next compressed frame.
	}
	return nil
}

// walkUncompressedBlock consumes an uncompressed Data-block body:
//   - BlockInfo (compat decoder, handles ClickHouse 26.x field 3)
//   - Empty-block fast path: if num_columns == 0 and num_rows == 0, consume 2 bytes
//   - Non-empty: full DecodeRawBlock via proto.Block to consume all column data
//
// Port of pkg/proxy/proxy.go:handleDataBlock uncompressed branch.
func (c *Codec) walkUncompressedBlock() error {
	if _, _, err := decodeBlockInfoCompat(c.r); err != nil {
		return fmt.Errorf("BlockInfo decode: %w", err)
	}

	// Empty-block fast path: if the next 2 bytes are zero-uvarints (i.e.
	// num_columns=0, num_rows=0), just consume them and we're done. Use
	// c.br.Peek — no other layer prefetches under the new codec design.
	peekBytes, err := c.br.Peek(2)
	if err == nil && len(peekBytes) >= 2 && peekBytes[0] == 0 && peekBytes[1] == 0 {
		if _, err := c.readFullRaw(2); err != nil {
			return fmt.Errorf("empty block discard: %w", err)
		}
		return nil
	}

	// Non-empty block: DecodeRawBlock reads via the one-byte-at-a-time path
	// (c.r). Slower than compressed payload handling, but uncompressed blocks
	// tend to be small (system-table scans, handshake-time probes) and
	// exercising this path with the capture reader keeps Raw bytes intact.
	var block proto.Block
	var results proto.Results
	if err := block.DecodeRawBlock(c.r, c.Revision(), results.Auto()); err != nil {
		return fmt.Errorf("block raw decode: %w", err)
	}
	return nil
}
