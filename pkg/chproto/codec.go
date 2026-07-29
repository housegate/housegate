package chproto

import (
	"bufio"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// Codec wraps one side of a ClickHouse TCP connection.
//
// One Codec per net.Conn. The underlying io.Reader is a bufio.Reader sized at
// defaultBufSize (128KB, matching legacy proxy.go). On top of that sits a
// captureByteReader that returns exactly one byte per Read call and records
// every byte returned. The long-lived proto.Reader (c.r) wraps the capture
// reader.
//
// The 1-byte-at-a-time guarantee is what keeps the codec correct across
// multiple interleaved read phases (ReadPacket → NegotiateAddendum →
// ReadPacket → …). A stock proto.Reader wraps its input in a 4KB bufio that
// eagerly prefetches from the underlying stream; if we ever replaced that
// proto.Reader mid-connection (as an earlier design did by creating a fresh
// proto.Reader per ReadPacket call), any bytes already prefetched into the
// old inner bufio would be stranded and the next logical read would block
// forever waiting on a socket whose contents it already consumed. By forcing
// the source to dribble bytes out one at a time, the inner bufio never has
// more than one byte at rest, no prefetch ever crosses a logical boundary,
// and the codec can be safely used across any mixture of read callers — at
// the cost of a few extra function calls per byte during handshake.
// Performance-sensitive paths (compressed Data block payloads) bypass the
// capture reader via readFullRaw.
//
// Writes go directly to the underlying io.Writer; the proxy relay does not
// need write-side buffering beyond what ch-go's *proto.Buffer provides.
type Codec struct {
	conn        io.ReadWriter
	br          *bufio.Reader
	cap         *captureByteReader
	r           *proto.Reader // permanent; reads via cap
	w           io.Writer
	rev         atomic.Int64 // negotiated protocol revision; 0 until SetRevision
	helloRev    atomic.Int64 // ClientHello revision used by server to shape ServerHello
	serverMajor atomic.Int64 // upstream server version, captured from ServerHello
	serverMinor atomic.Int64
	serverPatch atomic.Int64
	compression atomic.Int32 // proto.Compression value; 0=Disabled, 1=Enabled
	dir         Direction
}

const defaultBufSize = 131072 // 128KB, matches legacy proxy.go

// NewCodec constructs a Codec wrapping conn. direction controls the code-set
// ReadPacket expects.
func NewCodec(conn io.ReadWriter, direction Direction) *Codec {
	br := bufio.NewReaderSize(conn, defaultBufSize)
	cap := newCaptureByteReader(br)
	return &Codec{
		conn: conn,
		br:   br,
		cap:  cap,
		r:    proto.NewReader(cap),
		w:    conn,
		dir:  direction,
	}
}

// Conn returns the underlying io.ReadWriter (for Splice targets and
// Close from the session).
func (c *Codec) Conn() io.ReadWriter { return c.conn }

// SetRevision records the negotiated protocol revision. Called by the caller
// after handshake completes; affects decoding of packets whose layout varies
// with revision (Query primarily).
func (c *Codec) SetRevision(rev int) { c.rev.Store(int64(rev)) }

// Revision returns the last revision set by SetRevision, or 0 pre-handshake.
func (c *Codec) Revision() int { return int(c.rev.Load()) }

// SetServerHelloRevisionHint records the ClientHello ProtocolVersion sent to
// the upstream. ClickHouse gates optional ServerHello fields on the client's
// advertised revision, not only on the server revision written inside the
// packet. This is separate from SetRevision, which stores the negotiated
// min(client, server) revision after the handshake completes.
func (c *Codec) SetServerHelloRevisionHint(rev int) { c.helloRev.Store(int64(rev)) }

func (c *Codec) serverHelloRevisionHint() int { return int(c.helloRev.Load()) }

// SetCompression records the compression mode negotiated for the current query.
// Called by the caller (Relay) after decoding a Query packet; affects how
// walkDataBlock parses subsequent Data packet bodies.
func (c *Codec) SetCompression(comp proto.Compression) { c.compression.Store(int32(comp)) }

// Compression returns the current compression mode, or CompressionDisabled if
// SetCompression has not been called.
func (c *Codec) Compression() proto.Compression { return proto.Compression(c.compression.Load()) }

// ReadPacket reads the next packet from the wire.
//
// If the packet's type appears in decodeTypes, it is decoded into a typed
// struct and returned in Packet.Decoded. Otherwise, the entire packet
// (including its leading type VarUInt) is accumulated into Packet.Raw for
// zero-copy Splice.
//
// ServerHello is special: when an upstream-direction codec decodes a
// ServerHello successfully, Raw is ALSO populated with the full on-wire
// bytes (including trailing fields ch-go's ServerHello struct does not
// know about, such as proto_send_chunked_srv, password_complexity_rules,
// nonce, etc.). Callers that need to echo the ServerHello to the client
// must write Raw directly instead of calling WriteServerHello, otherwise
// newer ClickHouse servers' trailing fields will be silently dropped and
// the client will hang waiting for them during addendum negotiation.
//
// Known sentinels:
//
//	io.EOF           — peer closed cleanly
//	ErrMalformed     — protocol error; caller must close
//	ErrUnknownPacket — type not in the known table. Packet carries the bytes
//	                   consumed before the framing gap, but the caller must
//	                   close rather than splice an incomplete packet.
//	ErrDecode        — decode of a requested type failed; per spec D8, caller
//	                   falls back to Splice(Raw) — this function populates
//	                   both Raw and the Decode error.
func (c *Codec) ReadPacket(decodeTypes ...uint64) (*Packet, error) {
	return c.readPacket(0, false, decodeTypes...)
}

// ReadPacketWithDataLimit is ReadPacket with an on-wire byte limit applied to
// ClientData packets while they are being captured. Other packet types are not
// limited by this method.
func (c *Codec) ReadPacketWithDataLimit(maxDataBytes uint64, decodeTypes ...uint64) (*Packet, error) {
	return c.readPacket(maxDataBytes, true, decodeTypes...)
}

func (c *Codec) readPacket(maxDataBytes uint64, enforceDataLimit bool, decodeTypes ...uint64) (*Packet, error) {
	// The 1-byte-at-a-time capture reader guarantees proto.Reader's inner
	// bufio is empty at a packet boundary, so it's safe to drop the capture
	// buffer from the previous packet here.
	c.cap.reset()

	typeVar, err := c.r.UVarInt()
	if err != nil {
		return nil, err // includes io.EOF
	}
	if c.dir == DirFromClient && typeVar == uint64(proto.ClientCodeData) && enforceDataLimit {
		if err := c.cap.setLimit(maxDataBytes); err != nil {
			raw := c.cap.snapshot()
			return &Packet{Type: typeVar, RawLen: len(raw), Raw: raw}, err
		}
	}

	wantDecode := false
	for _, t := range decodeTypes {
		if t == typeVar {
			wantDecode = true
			break
		}
	}

	if !wantDecode {
		if err := c.skipPacketBody(typeVar); err != nil {
			raw := c.cap.snapshot()
			return &Packet{Type: typeVar, RawLen: len(raw), Raw: raw}, err
		}
		raw := c.cap.snapshot()
		return &Packet{Type: typeVar, RawLen: len(raw), Raw: raw}, nil
	}

	// ClientCodeHello == 0 == ServerCodeHello, so we use the codec Direction to
	// decide which struct to decode into.
	var decoded any
	var decErr error
	switch typeVar {
	case uint64(proto.ClientCodeHello): // also ServerCodeHello (both == 0)
		if c.dir == DirFromClient {
			decoded, decErr = c.decodeClientHello()
		} else {
			decoded, decErr = c.decodeServerHello()
		}
	case uint64(proto.ClientCodeQuery): // == ServerCodeData (1); direction disambiguates
		if c.dir == DirFromClient {
			decoded, decErr = c.decodeQuery()
		} else {
			// ServerCodeData handling is not in Task 3's scope; fall through.
			decErr = fmt.Errorf("%w: type=%d", ErrUnknownPacket, typeVar)
		}
	case uint64(proto.ServerCodeException): // == ClientCodeData (2); direction disambiguates
		if c.dir == DirToUpstream {
			// Upstream→client direction; server sends Exception.
			decoded, decErr = c.decodeException()
		} else {
			// ClientCodeData handling deferred to Task 5 (Splice).
			decErr = fmt.Errorf("%w: type=%d", ErrUnknownPacket, typeVar)
		}
	default:
		decErr = fmt.Errorf("%w: type=%d", ErrUnknownPacket, typeVar)
	}

	if decErr != nil {
		raw := c.cap.snapshot()
		return &Packet{Type: typeVar, RawLen: len(raw), Raw: raw}, decErr
	}

	// ServerHello pass-through: ch-go's ServerHello struct only covers fields
	// up to Patch (FeatureVersionPatch). Newer ClickHouse revisions append
	// proto_send_chunked_srv, proto_recv_chunked_srv, parallel_replicas_version,
	// password_complexity_rules, nonce, server_settings, and
	// query_plan_serialization_version. DecodeAware leaves those bytes in the
	// underlying bufio; re-encoding via EncodeAware would drop them.
	//
	// Drain any remaining buffered bytes in c.br into the capture so the caller
	// can forward the raw ServerHello byte-for-byte. This is safe because the
	// client blocks after sending ClientHello until it receives the full
	// ServerHello; no subsequent upstream packets are pipelined behind it.
	//
	// Additionally, rewrite the server's chunked-protocol advertisements to
	// "notchunked" before handing Raw back. The codec does not yet wrap
	// reads/writes in ChunkedReader / ChunkedWriter, so honoring the server's
	// chunked_optional would break the packet loop (the 4-byte chunked frame
	// header gets parsed as a varint packet type). See
	// rewriteServerHelloChunkedToNotChunked for the detailed rationale and the
	// TODO to remove this once chunked support lands.
	if c.dir == DirToUpstream && typeVar == uint64(proto.ServerCodeHello) {
		c.drainBufferedIntoCapture()
		raw := c.cap.snapshot()
		rewritten, rerr := rewriteServerHelloChunkedToNotChunked(raw, c.serverHelloRevisionHint())
		if rerr != nil {
			return &Packet{Type: typeVar, RawLen: len(raw), Raw: raw, Decoded: decoded},
				fmt.Errorf("%w: server hello chunked-rewrite: %v", ErrDecode, rerr)
		}
		return &Packet{Type: typeVar, RawLen: len(rewritten), Raw: rewritten, Decoded: decoded}, nil
	}
	raw := c.cap.snapshot()
	return &Packet{Type: typeVar, RawLen: len(raw), Decoded: decoded}, nil
}

// drainBufferedIntoCapture appends any currently-buffered bytes in c.br to
// the capture. Used after ServerHello decode to capture forward-compatible
// trailing fields that ch-go's ServerHello struct does not know about.
//
// Safety: only invoke this in handshake contexts where the peer is known to
// be blocked waiting for us — never on the client side (the client may have
// already pipelined the next packet) nor on the upstream side after the
// handshake has completed (the upstream may be mid-stream on a result set).
func (c *Codec) drainBufferedIntoCapture() {
	for {
		n := c.br.Buffered()
		if n <= 0 {
			return
		}
		buf := make([]byte, n)
		read, _ := c.br.Read(buf)
		if read <= 0 {
			return
		}
		c.cap.appendRaw(buf[:read])
	}
}

// skipPacketBody consumes bytes belonging to a packet whose type is not in
// the decode set. Reads go through c.r where applicable (so bytes land in
// the capture) or via readFullRaw for batch transfers.
//
// Dispatch is direction-aware: on the wire, packet-type numbers collide
// between the client → server and server → client sides (e.g. code 2 is
// ClientCodeData from the client but ServerCodeException from the server;
// code 7 is ClientScalar vs ServerCodeTotals). c.dir disambiguates.
func (c *Codec) skipPacketBody(typeVar uint64) error {
	if c.dir == DirFromClient {
		return c.skipClientPacketBody(typeVar)
	}
	return c.skipServerPacketBody(typeVar)
}

func (c *Codec) skipClientPacketBody(typeVar uint64) error {
	switch typeVar {
	case uint64(proto.ClientCodePing),
		uint64(proto.ClientCodeCancel):
		return nil
	case uint64(proto.ClientCodeData),
		uint64(ClientScalar):
		return c.walkDataBlock()
	case uint64(proto.ClientTablesStatusRequest):
		return c.skipTablesStatusRequest()
	}
	return fmt.Errorf("%w: skip body for client type=%d not yet supported", ErrUnknownPacket, typeVar)
}

// skipTablesStatusRequest consumes the body of a
// ClientTablesStatusRequest packet (type 5). ClickHouse sends this as a
// pre-flight check before issuing a distributed query against a
// remote() target — the originating server asks the remote replica
// which of a list of tables exist and which are replicated. The proxy
// only needs to skip past the body so the surrounding capture / splice
// logic can forward the bytes to the local upstream.
//
// Wire format (writeTablesStatusRequest in ClickHouse):
//
//	varuint count
//	for each: string database, string table
func (c *Codec) skipTablesStatusRequest() error {
	n, err := c.r.UVarInt()
	if err != nil {
		return fmt.Errorf("tables status req: count: %w", err)
	}
	for i := uint64(0); i < n; i++ {
		if _, err := c.r.Str(); err != nil {
			return fmt.Errorf("tables status req[%d]: database: %w", i, err)
		}
		if _, err := c.r.Str(); err != nil {
			return fmt.Errorf("tables status req[%d]: table: %w", i, err)
		}
	}
	return nil
}

func (c *Codec) skipServerPacketBody(typeVar uint64) error {
	rev := c.Revision()
	switch typeVar {
	case uint64(proto.ServerCodePong),
		uint64(proto.ServerCodeEndOfStream),
		uint64(ServerReadTaskRequestCode):
		return nil

	// Data / Totals / Extremes always follow the query's compression mode.
	case uint64(proto.ServerCodeData),
		uint64(proto.ServerCodeTotals),
		uint64(proto.ServerCodeExtremes):
		return c.walkDataBlock()

	// Log / ProfileEvents use the same Native block body, but older revisions
	// send it uncompressed even when the query's Data packets are compressed.
	case uint64(proto.ServerCodeLog),
		uint64(ServerProfileEventsCode):
		if rev < revisionMinCompressedLogsProfileEventsColumns {
			return c.walkDataBlockWithCompression(proto.CompressionDisabled)
		}
		return c.walkDataBlock()

	case uint64(proto.ServerCodeException):
		var exc proto.Exception
		if err := exc.DecodeAware(c.r, rev); err != nil {
			return fmt.Errorf("exception decode: %w", err)
		}
		return nil

	case uint64(proto.ServerCodeProgress):
		return c.skipServerProgress(rev)

	case uint64(proto.ServerCodeProfile):
		return c.skipServerProfile(rev)

	case uint64(proto.ServerCodeTablesStatus):
		return c.skipTablesStatusResponse(rev)

	case uint64(proto.ServerCodeTableColumns):
		// Since revision 54481, TableColumns travels through the same
		// maybe-compressed stream as Data/Log/ProfileEvents.
		if rev >= revisionMinCompressedLogsProfileEventsColumns &&
			c.Compression() == proto.CompressionEnabled {
			return c.walkCompressedTableColumns()
		}
		var t proto.TableColumns
		if err := t.DecodeAware(c.r, rev); err != nil {
			return fmt.Errorf("table_columns decode: %w", err)
		}
		return nil

	case uint64(ServerPartUUIDsCode):
		return c.skipPartUUIDs()

	case uint64(ServerTimezoneUpdateCode),
		uint64(ServerSSHChallengeCode):
		if _, err := c.r.Str(); err != nil {
			return fmt.Errorf("server string packet type=%d: %w", typeVar, err)
		}
		return nil
	}
	return fmt.Errorf("%w: skip body for server type=%d not yet supported", ErrUnknownPacket, typeVar)
}

// Protocol revision gates used by current ClickHouse server packet layouts.
// ch-go v0.73 does not yet model these trailing fields, so the framing path
// consumes them directly instead of relying on its shorter decoders.
const (
	revisionMinClientWriteInfo                    = 54420
	revisionMinTotalBytesInProgress               = 54463
	revisionMinServerQueryTimeInProgress          = 54460
	revisionMinTableReadOnlyCheck                 = 54467
	revisionMinRowsBeforeAggregation              = 54469
	revisionMinCompressedLogsProfileEventsColumns = 54481
)

func (c *Codec) skipServerProgress(rev int) error {
	fields := 3 // read_rows, read_bytes, total_rows_to_read
	if rev >= revisionMinTotalBytesInProgress {
		fields++ // total_bytes_to_read
	}
	if rev >= revisionMinClientWriteInfo {
		fields += 2 // written_rows, written_bytes
	}
	if rev >= revisionMinServerQueryTimeInProgress {
		fields++ // elapsed_ns
	}
	for i := 0; i < fields; i++ {
		if _, err := c.r.UVarInt(); err != nil {
			return fmt.Errorf("progress field %d: %w", i, err)
		}
	}
	return nil
}

func (c *Codec) skipServerProfile(rev int) error {
	for i := 0; i < 3; i++ { // rows, blocks, bytes
		if _, err := c.r.UVarInt(); err != nil {
			return fmt.Errorf("profile counter %d: %w", i, err)
		}
	}
	if _, err := c.r.Bool(); err != nil { // applied_limit
		return fmt.Errorf("profile applied_limit: %w", err)
	}
	if _, err := c.r.UVarInt(); err != nil { // rows_before_limit
		return fmt.Errorf("profile rows_before_limit: %w", err)
	}
	if _, err := c.r.Bool(); err != nil { // obsolete calculated_rows_before_limit
		return fmt.Errorf("profile obsolete flag: %w", err)
	}
	if rev >= revisionMinRowsBeforeAggregation {
		if _, err := c.r.Bool(); err != nil {
			return fmt.Errorf("profile applied_aggregation: %w", err)
		}
		if _, err := c.r.UVarInt(); err != nil {
			return fmt.Errorf("profile rows_before_aggregation: %w", err)
		}
	}
	return nil
}

// skipTablesStatusResponse consumes the response body:
//
//	varuint count
//	for each:
//	  string database, string table, bool replicated
//	  if replicated: varuint absolute_delay
//	  if replicated && revision >= 54467: varuint readonly
func (c *Codec) skipTablesStatusResponse(rev int) error {
	n, err := c.r.UVarInt()
	if err != nil {
		return fmt.Errorf("tables status response count: %w", err)
	}
	for i := uint64(0); i < n; i++ {
		if _, err := c.r.Str(); err != nil {
			return fmt.Errorf("tables status response[%d] database: %w", i, err)
		}
		if _, err := c.r.Str(); err != nil {
			return fmt.Errorf("tables status response[%d] table: %w", i, err)
		}
		replicated, err := c.r.Bool()
		if err != nil {
			return fmt.Errorf("tables status response[%d] replicated: %w", i, err)
		}
		if !replicated {
			continue
		}
		if _, err := c.r.UVarInt(); err != nil {
			return fmt.Errorf("tables status response[%d] delay: %w", i, err)
		}
		if rev >= revisionMinTableReadOnlyCheck {
			if _, err := c.r.UVarInt(); err != nil {
				return fmt.Errorf("tables status response[%d] readonly: %w", i, err)
			}
		}
	}
	return nil
}

func (c *Codec) skipPartUUIDs() error {
	n, err := c.r.UVarInt()
	if err != nil {
		return fmt.Errorf("part UUID count: %w", err)
	}
	maxInt := int(^uint(0) >> 1)
	if n > uint64(maxInt/16) {
		return fmt.Errorf("part UUID count %d overflows packet size", n)
	}
	if _, err := c.readFullRaw(int(n) * 16); err != nil {
		return fmt.Errorf("part UUID payload: %w", err)
	}
	return nil
}

// Splice forwards a Packet read by ReadPacket to dst, byte-for-byte.
//
// If p.Decoded != nil, Splice refuses (API-usage violation): callers that
// decoded a packet should round-trip via a typed Write* method instead.
// (ServerHello is an exception — it populates both Decoded and Raw so the
// caller can echo the raw bytes via Conn().Write; using Splice on a
// ServerHello Packet is still not supported because the decoded/raw dual
// invariant is intentionally limited to that one case.)
//
// If p.Raw != nil, Splice writes Raw to dst.
func (c *Codec) Splice(dst io.Writer, p *Packet) error {
	if p == nil {
		return fmt.Errorf("%w: nil packet", ErrMalformed)
	}
	if p.Decoded != nil {
		return fmt.Errorf("%w: Splice called on decoded packet; use a typed Write* instead", ErrMalformed)
	}
	_, err := dst.Write(p.Raw)
	return err
}

// WriteEmptyDataBlock writes a "no input / end of external tables"
// terminator after a Query packet. ClickHouse's TCPHandler::runImpl
// calls receiveData(true) before executeQuery for every Query —
// regardless of statement kind — and that call blocks reading Data
// packets from the client until it sees one with both num_columns=0
// and num_rows=0. Inter-server clients (Connection::sendQuery +
// sendExternalTables) emit it inline.
//
// The block_name is always plaintext; the trailing payload (BlockInfo +
// num_columns + num_rows) is compressed when the codec's compression
// is enabled, mirroring CH's wire format. For compressed streams we
// use LZ4 — the inter-server default; ZSTD is configurable but not
// negotiated through the Query.Compression bit (which is just
// enabled/disabled), so picking LZ4 for the terminator is safe even
// when the actual data path uses ZSTD because the receiver dispatches
// per-frame on the method byte.
func (c *Codec) WriteEmptyDataBlock() error {
	// Plaintext header: type byte + empty block_name.
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")

	// Trailing payload is BlockInfo + num_columns + num_rows.
	var payload proto.Buffer
	bi := proto.BlockInfo{BucketNum: -1}
	bi.Encode(&payload)
	payload.PutUVarInt(0) // num_columns
	payload.PutUVarInt(0) // num_rows

	if c.Compression() == proto.CompressionEnabled {
		// Compressed path: wrap payload in an LZ4 frame.
		// compress.Writer.Data carries the full frame: [16 bytes
		// cityHash128 | 1 byte method | 4 bytes compressed_size |
		// 4 bytes decompressed_size | compressed_payload].
		w := compress.NewWriter(0, compress.LZ4)
		if err := w.Compress(payload.Buf); err != nil {
			return fmt.Errorf("compress empty data: %w", err)
		}
		buf.Buf = append(buf.Buf, w.Data...)
	} else {
		buf.Buf = append(buf.Buf, payload.Buf...)
	}

	// Single Write so net.Pipe-backed test peers (which match Write↔Read 1:1)
	// receive everything in one read.
	_, err := c.w.Write(buf.Buf)
	return err
}

// readFullRaw reads exactly n bytes directly from c.br — bypassing the
// one-byte-at-a-time capture reader — and appends them to the capture. Used
// by walkDataBlock's compressed-frame path, where proto.Reader's
// byte-by-byte discipline would force millions of function calls per MB of
// payload for no benefit.
func (c *Codec) readFullRaw(n int) ([]byte, error) {
	if c.cap.wouldExceed(n) {
		return nil, ErrPacketTooLarge
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.br, buf); err != nil {
		return nil, err
	}
	c.cap.appendRaw(buf)
	return buf, nil
}

// captureByteReader wraps a bufio.Reader with a read discipline that returns
// at most one byte per Read call, recording every byte returned into cap.
// Feeding this into proto.NewReader keeps proto.Reader's inner bufio from
// prefetching beyond the logical read boundary, which is the invariant that
// makes a long-lived proto.Reader correct across interleaved read callers.
//
// See Codec's type doc for the full rationale.
type captureByteReader struct {
	br      *bufio.Reader
	cap     []byte
	limit   int
	limited bool
}

func newCaptureByteReader(br *bufio.Reader) *captureByteReader {
	return &captureByteReader{br: br, cap: make([]byte, 0, 1024)}
}

// Read returns at most one byte, guaranteeing proto.Reader's inner bufio
// never holds unclaimed bytes at a packet boundary.
func (c *captureByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.wouldExceed(1) {
		return 0, ErrPacketTooLarge
	}
	b, err := c.br.ReadByte()
	if err != nil {
		return 0, err
	}
	p[0] = b
	c.cap = append(c.cap, b)
	return 1, nil
}

// appendRaw appends b to the capture without going through the Read path —
// used when bytes are pulled straight out of the underlying bufio (e.g. the
// ServerHello-tail drain, or readFullRaw for compressed Data payloads).
func (c *captureByteReader) appendRaw(b []byte) {
	c.cap = append(c.cap, b...)
}

// reset clears the capture buffer. Called by ReadPacket at packet boundaries;
// safe because the 1-byte-at-a-time discipline guarantees proto.Reader's
// inner bufio holds no unconsumed bytes at that point.
func (c *captureByteReader) reset() {
	c.cap = c.cap[:0]
	c.limit = 0
	c.limited = false
}

func (c *captureByteReader) setLimit(max uint64) error {
	c.limited = true
	maxInt := int(^uint(0) >> 1)
	if max > uint64(maxInt) {
		c.limit = maxInt
	} else {
		c.limit = int(max)
	}
	if len(c.cap) > c.limit {
		return ErrPacketTooLarge
	}
	return nil
}

func (c *captureByteReader) wouldExceed(additional int) bool {
	return c.limited && additional > c.limit-len(c.cap)
}

// snapshot returns an independent copy of the capture — callers stash this
// as Packet.Raw, so mutations through the capture must not affect it.
func (c *captureByteReader) snapshot() []byte {
	out := make([]byte, len(c.cap))
	copy(out, c.cap)
	return out
}

// uvarintEncLen returns the number of bytes needed to encode v as a base-128
// VarUInt (same encoding as proto.Buffer.PutUVarInt / binary.PutUvarint).
func uvarintEncLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
