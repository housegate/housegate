package chproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ClickHouse/ch-go/proto"
)

// NormalizeServerDataBlockInfo adapts one already captured, bounded,
// uncompressed ServerData packet for ch-go's Native decoder. ClickHouse 26.x
// adds BlockInfo field 3, which the pinned ch-go cannot decode. Preserve the
// ordinary fields and the exact column body while removing that metadata.
// This is a decoding copy only: callers must still forward original Packet.Raw
// when relaying packets. It neither frames a stream nor validates column types.
func NormalizeServerDataBlockInfo(raw []byte, revision int) ([]byte, error) {
	if revision <= 0 {
		return nil, fmt.Errorf("server Data revision is required")
	}
	r := bytes.NewReader(raw)
	code, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("server Data code: %w", err)
	}
	if code != uint64(proto.ServerCodeData) {
		return nil, fmt.Errorf("packet type %d is not ServerData", code)
	}
	nameLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("server Data name length: %w", err)
	}
	if nameLen > uint64(r.Len()) {
		return nil, fmt.Errorf("server Data name exceeds packet length")
	}
	if _, err := r.Seek(int64(nameLen), io.SeekCurrent); err != nil {
		return nil, err
	}
	start := len(raw) - r.Len()
	if !proto.FeatureBlockInfo.In(revision) {
		return append([]byte(nil), raw...), nil
	}
	info, consumed, err := decodeBlockInfoCompat(proto.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("server Data BlockInfo: %w", err)
	}
	var b proto.Buffer
	b.Buf = append(b.Buf, raw[:start]...)
	proto.BlockInfo{Overflows: info.Overflows, BucketNum: info.BucketNum}.Encode(&b)
	b.Buf = append(b.Buf, raw[start+consumed:]...)
	return b.Buf, nil
}
