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
// capture automatically. Compressed payloads are decompressed through a
// raw-capturing reader and decoded to the authoritative Native block boundary;
// uncompressed blocks fall back to DecodeRawBlock on c.r.
//
// Uses c.Compression() to pick between the compressed and uncompressed
// (BlockInfo + empty-block fast path + DecodeRawBlock) branches.
func (c *Codec) walkDataBlock() error {
	// block_name is always a short uncompressed string in the plaintext stream.
	if _, err := c.r.Str(); err != nil {
		return fmt.Errorf("data block name: %w", err)
	}

	if c.Compression() == proto.CompressionEnabled {
		return c.walkCompressedDataBlock()
	}
	return c.walkUncompressedBlock()
}

// walkCompressedDataBlock consumes one logical Native block from the
// compressed stream. A block may span multiple compression frames, including
// frames that have not reached the socket yet. The decompressed Native block
// structure, rather than currently-buffered raw bytes, is the packet boundary.
//
// ClickHouse flushes the compression stream at the end of each Native block.
// Therefore proto.Reader may buffer the remainder of the block's final frame,
// but cannot prefetch bytes from the following packet.
func (c *Codec) walkCompressedDataBlock() error {
	r := c.newCompressedCaptureReader()
	if _, _, err := decodeBlockInfoCompat(r); err != nil {
		return fmt.Errorf("compressed BlockInfo decode: %w", err)
	}

	var block proto.Block
	if err := block.DecodeRawBlock(r, c.Revision(), discardBlockResult{}); err != nil {
		return fmt.Errorf("compressed block raw decode: %w", err)
	}
	return nil
}

// walkCompressedTableColumns consumes the two strings in a compressed
// TableColumns packet. It uses the decoded protocol body as the boundary for
// the same reason walkCompressedDataBlock does.
func (c *Codec) walkCompressedTableColumns() error {
	r := c.newCompressedCaptureReader()
	var columns proto.TableColumns
	if err := columns.DecodeAware(r, c.Revision()); err != nil {
		return fmt.Errorf("compressed table_columns decode: %w", err)
	}
	return nil
}

func (c *Codec) newCompressedCaptureReader() *proto.Reader {
	raw := &compressedCaptureReader{codec: c}
	return proto.NewReader(compress.NewReader(raw))
}

// compressedCaptureReader lets the compression decoder read raw frames in
// normal-sized chunks while preserving the exact on-wire bytes in Packet.Raw.
// compress.Reader uses io.ReadFull for frame headers and payloads, so this
// reader never consumes beyond the current compressed frame.
type compressedCaptureReader struct {
	codec *Codec
}

func (r *compressedCaptureReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.codec.cap.wouldExceed(len(p)) {
		return 0, ErrPacketTooLarge
	}
	n, err := r.codec.br.Read(p)
	if n > 0 {
		r.codec.cap.appendRaw(p[:n])
	}
	return n, err
}

// discardBlockResult decodes one column at a time and immediately releases
// it. Native column encodings are type-dependent, so decoding is required to
// locate the end of a block; retaining every decoded column would needlessly
// make proxy memory proportional to the entire result block.
type discardBlockResult struct{}

func (discardBlockResult) DecodeResult(r *proto.Reader, version int, block proto.Block) error {
	for i := 0; i < block.Columns; i++ {
		name, err := r.Str()
		if err != nil {
			return fmt.Errorf("column [%d] name: %w", i, err)
		}
		typeName, err := r.Str()
		if err != nil {
			return fmt.Errorf("column [%d] type: %w", i, err)
		}
		if proto.FeatureCustomSerialization.In(version) {
			custom, err := r.Bool()
			if err != nil {
				return fmt.Errorf("column [%d] custom serialization: %w", i, err)
			}
			if custom {
				return fmt.Errorf("column [%d] has unsupported custom serialization", i)
			}
		}

		column := &proto.ColAuto{}
		if err := column.Infer(proto.ColumnType(typeName)); err != nil {
			return fmt.Errorf("column [%d] %q type inference: %w", i, name, err)
		}
		if block.Rows == 0 {
			continue
		}
		if stateful, ok := column.Data.(proto.StateDecoder); ok {
			if err := stateful.DecodeState(r); err != nil {
				return fmt.Errorf("column [%d] %q state: %w", i, name, err)
			}
		}
		if err := column.Data.DecodeColumn(r, block.Rows); err != nil {
			return fmt.Errorf("column [%d] %q data: %w", i, name, err)
		}
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
