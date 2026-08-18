package chproto

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/ClickHouse/ch-go/proto"
)

// chunkedFramePool caches ChunkedWriter frame buffers to reduce GC pressure from high-frequency writes.
var chunkedFramePool = sync.Pool{
	New: func() interface{} {
		// Allocate 64KB + 8-byte frame header/trailer buffer by default.
		buf := make([]byte, 0, defaultChunkPayloadSize+chunkedHeaderSize*2)
		return buf
	},
}

// ClickHouse Chunked protocol frame format (from ClickHouse C++ source ReadBufferFromPocoSocketChunked.h):
//
//	Chunk begins   | [4 bytes LE: chunk_size]   ← uint32 little-endian, frame data length
//	               |   packet_type + packet_data
//	               |   ... may contain multiple packets ...
//	Chunk ends     | [4 bytes: 0x00000000]      ← 4-byte zero end marker
//
// Three modes:
//
//	Basic:      single frame contains one packet
//	Datastream: single frame contains multiple consecutive packets
//	Multipart:  packet data spans multiple frames (chunk parts)
const (
	// chunkedHeaderSize is the chunk frame header size (4-byte uint32 LE).
	chunkedHeaderSize = 4
	// chunkedEndMarker is the chunk end marker value.
	chunkedEndMarker = uint32(0)
	// maxChunkSize is the reasonable maximum chunk size (defensive limit, 256MB).
	maxChunkSize = 256 * 1024 * 1024
	// defaultChunkPayloadSize is the ChunkedWriter's default chunk payload size.
	defaultChunkPayloadSize = 64 * 1024
)

// ChunkedReader strips chunked frame headers and end markers from the underlying io.Reader,
// exposing a "raw" protocol data stream to upper layers, requiring no changes to existing packet parsing logic.
//
// Implements io.Reader interface. When enabled=false, directly passes through to underlying Reader.
// Note: ChunkedReader is not concurrency-safe. Callers must ensure single goroutine access.
type ChunkedReader struct {
	r         io.Reader
	enabled   bool
	remaining int // Remaining unread bytes in the current frame.
	header    [chunkedHeaderSize]byte
	headerN   int // Bytes retained from a fragmented frame header.
}

// NewChunkedReader creates a ChunkedReader. enabled controls whether chunked deframing is active.
func NewChunkedReader(r io.Reader, enabled bool) *ChunkedReader {
	return &ChunkedReader{
		r:       r,
		enabled: enabled,
	}
}

// Enabled returns whether chunked deframing is currently active.
func (cr *ChunkedReader) Enabled() bool {
	return cr.enabled
}

// Enable switches subsequent reads to ClickHouse's chunked transport.
// Addendum negotiation is itself unchunked, so codecs enable this only after
// the final addendum field has been consumed.
func (cr *ChunkedReader) Enable() {
	cr.enabled = true
}

// ReadChunk returns one complete logical ClickHouse chunk with transport
// headers and the trailing zero marker removed. A logical chunk may contain
// multiple non-zero parts when a packet exceeds the sender's socket buffer.
//
// Server TCPHandler calls finishChunk after every emitted packet. Reading the
// logical chunk directly therefore gives the proxy an authoritative packet
// boundary without decoding Native column values (whose layouts are
// type-dependent and, for AggregateFunction states, not self-delimiting).
func (cr *ChunkedReader) ReadChunk() ([]byte, error) {
	if !cr.enabled {
		return nil, fmt.Errorf("chunked: ReadChunk called while disabled")
	}
	if cr.remaining != 0 {
		return nil, fmt.Errorf("chunked: ReadChunk called during streaming frame")
	}

	var chunk []byte
	for {
		size, err := cr.readChunkHeader()
		if err != nil {
			return nil, err
		}
		if size == chunkedEndMarker {
			if len(chunk) == 0 {
				return nil, fmt.Errorf("chunked: empty logical chunk")
			}
			return chunk, nil
		}
		// maxChunkSize limits each transport frame, not the whole logical
		// packet: ClickHouse may split one large Native block across many
		// frames before writing the zero terminator. Reject only an impossible
		// aggregate length that cannot fit in a Go slice.
		if uint64(len(chunk))+uint64(size) > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("chunked: logical chunk length overflows int")
		}
		start := len(chunk)
		chunk = append(chunk, make([]byte, int(size))...)
		if _, err := io.ReadFull(cr.r, chunk[start:]); err != nil {
			return nil, fmt.Errorf("chunked: read frame payload: %w", err)
		}
	}
}

// Read implements io.Reader.
// When enabled=true, automatically strips chunk frame headers and end markers.
// When enabled=false, directly passes through to underlying Reader.
func (cr *ChunkedReader) Read(p []byte) (int, error) {
	if !cr.enabled {
		return cr.r.Read(p)
	}

	for {
		if cr.remaining > 0 {
			// Current frame still has data to read.
			toRead := len(p)
			if toRead > cr.remaining {
				toRead = cr.remaining
			}
			n, err := cr.r.Read(p[:toRead])
			cr.remaining -= n
			if n > 0 {
				return n, err
			}
			if err != nil {
				return 0, err
			}
			continue
		}

		// Need to read the next chunk header.
		size, err := cr.readChunkHeader()
		if err != nil {
			return 0, err // io.EOF will be correctly propagated
		}

		if size == 0 {
			// Chunk end marker (0x00000000); continue reading the next chunk.
			continue
		}

		cr.remaining = int(size)
		// Return to top of loop, read data from new frame.
	}
}

// readChunkHeader reads the 4-byte chunk frame header and returns the chunk size.
// If the underlying Reader has reached EOF, returns (0, io.EOF).
func (cr *ChunkedReader) readChunkHeader() (uint32, error) {
	for cr.headerN < len(cr.header) {
		n, err := cr.r.Read(cr.header[cr.headerN:])
		cr.headerN += n
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return 0, io.EOF
			}
			// In particular, retain a partial header across a temporary read
			// deadline. WaitForPacketStart clears that deadline, and the normal
			// decoder resumes from exactly these bytes.
			return 0, fmt.Errorf("chunked: read frame header: %w", err)
		}
		if n == 0 {
			return 0, fmt.Errorf("chunked: read frame header: %w", io.ErrNoProgress)
		}
	}
	size := binary.LittleEndian.Uint32(cr.header[:])
	cr.headerN = 0
	if size > uint32(maxChunkSize) {
		return 0, fmt.Errorf("chunked: frame size %d exceeds max %d", size, maxChunkSize)
	}
	return size, nil
}

// ChunkedWriter wraps written data in chunked frame format before sending to the underlying Writer.
//
// Each Write call produces one chunk: [4 bytes LE: size][data][4 bytes: 0x00000000]
// When enabled=false, directly passes through to underlying Writer.
// Note: ChunkedWriter is not concurrency-safe. Callers must ensure single goroutine access.
type ChunkedWriter struct {
	w       io.Writer
	enabled bool
}

// NewChunkedWriter creates a ChunkedWriter. enabled controls whether chunked framing is active.
func NewChunkedWriter(w io.Writer, enabled bool) *ChunkedWriter {
	return &ChunkedWriter{
		w:       w,
		enabled: enabled,
	}
}

// Enabled returns whether chunked framing is currently active.
func (cw *ChunkedWriter) Enabled() bool {
	return cw.enabled
}

// Enable switches subsequent writes to ClickHouse's chunked transport.
// Addendum negotiation is itself unchunked, so codecs call this only after
// sending the final addendum field.
func (cw *ChunkedWriter) Enable() {
	cw.enabled = true
}

// Write implements io.Writer.
// When enabled=true, wraps data in a chunk frame: [size: 4 bytes LE][data][end: 4 bytes 0x00]
// When enabled=false, directly passes through to underlying Writer.
// When data exceeds defaultChunkPayloadSize, split it into multipart frames
// followed by one zero end marker. Intermediate parts MUST NOT carry a zero
// marker: that would turn the remainder of one packet into a new logical
// chunk whose first payload byte is misread as a packet type.
// Uses sync.Pool to cache frame buffers, reducing GC pressure for high-frequency small writes.
func (cw *ChunkedWriter) Write(p []byte) (int, error) {
	if !cw.enabled {
		return cw.w.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}

	totalWritten := 0
	for len(p) > 0 {
		// Fragmentation — each part has max size of defaultChunkPayloadSize.
		chunkSize := len(p)
		if chunkSize > defaultChunkPayloadSize {
			chunkSize = defaultChunkPayloadSize
		}
		chunk := p[:chunkSize]
		p = p[chunkSize:]

		// Only the final part carries the logical chunk's zero end marker.
		endSize := 0
		if len(p) == 0 {
			endSize = chunkedHeaderSize
		}
		frameSize := chunkedHeaderSize + chunkSize + endSize
		frame := chunkedFramePool.Get().([]byte)
		if cap(frame) < frameSize {
			frame = make([]byte, frameSize)
		} else {
			frame = frame[:frameSize]
		}
		binary.LittleEndian.PutUint32(frame[:chunkedHeaderSize], uint32(chunkSize))
		copy(frame[chunkedHeaderSize:], chunk)
		if endSize != 0 {
			binary.LittleEndian.PutUint32(frame[chunkedHeaderSize+chunkSize:], chunkedEndMarker)
		}

		n, err := cw.w.Write(frame)
		if err == nil && n != len(frame) {
			err = io.ErrShortWrite
		}

		// Return to pool. Oversized frames are not returned to avoid accumulating large buffers.
		// Return even on write failure — buffer is re-sliced on next use, so old data is overwritten.
		const maxPoolFrameSize = 256 * 1024 // 256KB
		if cap(frame) <= maxPoolFrameSize {
			chunkedFramePool.Put(frame)
		}

		if err != nil {
			return totalWritten, fmt.Errorf("chunked: write frame: %w", err)
		}

		totalWritten += chunkSize
	}

	return totalWritten, nil
}

// resolveChunkedMode resolves the negotiated chunked transport mode from two peers' proposals.
//
// Rules (matching ClickHouse C++ negotiation semantics):
//   - If either side proposes "" (empty / not applicable), result is "".
//   - If either side proposes "notchunked", result is "notchunked".
//   - If both sides propose any combination of "chunked" or "chunked_optional", result is "chunked".
func resolveChunkedMode(ours, theirs string) string {
	if ours == "" || theirs == "" {
		return ""
	}
	if ours == "notchunked" || theirs == "notchunked" {
		return "notchunked"
	}
	// Both are "chunked" or "chunked_optional" — upgrade to "chunked".
	return "chunked"
}

// ResolveUpstreamAddendum resolves Housegate's independent client role
// against the capabilities captured from the upstream ServerHello. base
// contributes the quota key and parallel-replicas version received from the
// downstream client; its chunked fields are replaced because the two legs may
// negotiate different transport modes.
func (c *Codec) ResolveUpstreamAddendum(base AddendumResult, opts AddendumOpts) AddendumResult {
	base.NegotiatedRecv = resolveChunkedMode(opts.ProposedRecv, c.serverSendChunked)
	base.NegotiatedSend = resolveChunkedMode(opts.ProposedSend, c.serverRecvChunked)
	return base
}

// AddendumOpts is the set of chunked-mode preferences the proxy proposes
// during addendum negotiation with a peer.
type AddendumOpts struct {
	// ProposedRecv is what we propose for our receive direction:
	// "notchunked" | "chunked" | "chunked_optional"
	ProposedRecv string
	// ProposedSend is what we propose for our send direction.
	ProposedSend string
}

// AddendumResult is the outcome of addendum negotiation.
type AddendumResult struct {
	// QuotaKey is the quota key read from the peer's addendum.
	QuotaKey string
	// NegotiatedRecv is the resolved chunked mode for our receive direction.
	NegotiatedRecv string
	// NegotiatedSend is the resolved chunked mode for our send direction.
	NegotiatedSend string
	// ParallelReplicasVersion is the parallel replicas protocol version read
	// from the peer's addendum (revision >= RevisionMinVersionedParallelReplicas = 54471).
	// Zero if the peer revision is below the threshold.
	ParallelReplicasVersion uint64
}

// NegotiateAddendum reads the client's addendum fields and resolves the
// chunked mode by pairing the client's proposals with the proxy's via
// resolveChunkedMode. The ClickHouse native protocol addendum is one-way
// (client → server); the server MUST NOT write an addendum back — after
// `encodeAddendum` the client moves straight to sending Query packets (see
// ch-go handshake.go: client calls `encodeAddendum` + `flush` and returns).
// Echoing anything to the client here produces spurious leading bytes that
// the client interprets as the next packet's type — most commonly seen as
// "Unknown packet 0 from server" because an empty quota_key starts with
// 0x00, which the client decodes as ServerHello mid-stream.
//
// Wire layout of the client addendum (client → proxy):
//
//	quota_key                 String    (revision >= FeatureAddendum = 54458)
//	proto_send_chunked        String    (revision >= FeatureChunkedPackets = 54470)
//	proto_recv_chunked        String    (revision >= FeatureChunkedPackets = 54470)
//	parallel_replicas_version UVarInt   (revision >= FeatureVersionedParallelReplicas = 54471)
//
// The client's proto_send_chunked describes what the client will send to us
// (our recv). The client's proto_recv_chunked describes what the client
// expects to receive (our send). NegotiateAddendum resolves:
//
//	NegotiatedRecv = resolveChunkedMode(opts.ProposedRecv, clientSendChunked)
//	NegotiatedSend = resolveChunkedMode(opts.ProposedSend, clientRecvChunked)
//
// Callers that need to forward the resolved addendum to an upstream codec
// (the proxy-to-upstream case) should invoke SendAddendum on the upstream
// codec separately — never on the client codec.
//
// The caller must have set the codec revision via SetRevision before calling
// NegotiateAddendum; revision guards which fields are expected on the wire.
// Only call NegotiateAddendum when SupportsAddendum(revision) is true.
func (c *Codec) NegotiateAddendum(opts AddendumOpts) (AddendumResult, error) {
	rev := c.Revision()

	// 1. quota_key — present for all addendum-capable revisions.
	quotaKey, err := c.r.Str()
	if err != nil {
		return AddendumResult{}, fmt.Errorf("addendum: read quota_key: %w", err)
	}

	// 2+3. proto_send_chunked / proto_recv_chunked
	var clientSendChunked, clientRecvChunked string
	if SupportsChunkedPackets(rev) {
		clientSendChunked, err = c.r.Str()
		if err != nil {
			return AddendumResult{}, fmt.Errorf("addendum: read proto_send_chunked: %w", err)
		}
		clientRecvChunked, err = c.r.Str()
		if err != nil {
			return AddendumResult{}, fmt.Errorf("addendum: read proto_recv_chunked: %w", err)
		}
	}

	// 4. parallel_replicas_version — read and store in result.
	var parallelReplicasVersion uint64
	if proto.FeatureVersionedParallelReplicas.In(rev) {
		parallelReplicasVersion, err = c.r.UVarInt()
		if err != nil {
			return AddendumResult{}, fmt.Errorf("addendum: read parallel_replicas_version: %w", err)
		}
	}

	// Resolve chunked modes.
	// Client's send (clientSendChunked) pairs with our recv (ProposedRecv).
	// Client's recv (clientRecvChunked) pairs with our send (ProposedSend).
	res := AddendumResult{
		QuotaKey:                quotaKey,
		NegotiatedRecv:          resolveChunkedMode(opts.ProposedRecv, clientSendChunked),
		NegotiatedSend:          resolveChunkedMode(opts.ProposedSend, clientRecvChunked),
		ParallelReplicasVersion: parallelReplicasVersion,
	}
	c.EnableChunked(res.NegotiatedRecv == "chunked", res.NegotiatedSend == "chunked")
	return res, nil
}

// SendAddendum writes the resolved addendum negotiation result to the peer.
//
// Wire layout (server → client direction), revision-gated to match the
// ClickHouse native protocol (reference: pkg/proxy/proxy.go:1492-1528):
//
//	quota_key                 String    (revision >= RevisionMinAddendum = 54458)
//	proto_send_chunked        String    (revision >= RevisionMinChunkedPackets = 54470)
//	proto_recv_chunked        String    (revision >= RevisionMinChunkedPackets = 54470)
//	parallel_replicas_version UVarInt   (revision >= RevisionMinVersionedParallelReplicas = 54471)
//
// Fields are only emitted when the codec's current Revision() meets the
// threshold — callers must call SetRevision with the negotiated revision
// before calling SendAddendum.
//
// In pass-through scenarios where NegotiateAddendum is not used, SendAddendum
// can be called directly with a pre-computed AddendumResult.
func (c *Codec) SendAddendum(res AddendumResult) error {
	rev := c.Revision()
	var buf proto.Buffer

	// 1. quota_key — present for all addendum-capable revisions.
	if SupportsAddendum(rev) {
		buf.PutString(res.QuotaKey)
	}

	// 2+3. proto_send_chunked / proto_recv_chunked
	if SupportsChunkedPackets(rev) {
		buf.PutString(res.NegotiatedSend)
		buf.PutString(res.NegotiatedRecv)
	}

	// 4. parallel_replicas_version
	if SupportsVersionedParallelReplicas(rev) {
		buf.PutUVarInt(res.ParallelReplicasVersion)
	}

	_, err := c.w.Write(buf.Buf)
	if err != nil {
		return err
	}
	c.EnableChunked(res.NegotiatedRecv == "chunked", res.NegotiatedSend == "chunked")
	return nil
}
