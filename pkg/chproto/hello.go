package chproto

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

// MaxSupportedRevision is the newest ClickHouse TCP protocol revision the
// pinned ch-go codec fully models. Housegate terminates the protocol on both
// sides, so it must negotiate this revision (or an older client revision)
// rather than forwarding a newer client's revision transparently.
const MaxSupportedRevision = proto.Version

// ClientHelloForUpstream returns a shallow copy of h with ProtocolVersion
// capped to the codec's supported revision. The remaining credentials and
// routing envelopes are preserved exactly.
func ClientHelloForUpstream(h *ClientHello) *ClientHello {
	if h == nil {
		return nil
	}
	upstream := *h
	if upstream.ProtocolVersion > MaxSupportedRevision {
		upstream.ProtocolVersion = MaxSupportedRevision
	}
	return &upstream
}

// rewriteServerHelloChunkedToNotChunked parses a complete on-wire ServerHello
// packet (including its leading type byte) and returns a new byte slice in
// which the two chunked-protocol advertisements the server sends to the
// client — proto_send_chunked_srv and proto_recv_chunked_srv — are replaced
// by the literal string "notchunked". All other fields (including trailing
// fields ch-go's ServerHello struct doesn't know about, such as
// password_complexity_rules, nonce, server_settings, and
// query_plan_serialization_version) are passed through unchanged.
//
// This is a short-term workaround for the Codec, which does not yet
// wrap reads/writes with ChunkedReader / ChunkedWriter. Without the rewrite, a
// modern ClickHouse server (rev >= 54470) advertises chunked_optional in the
// ServerHello; a modern client then negotiates "chunked" framing for both
// directions, and the proxy — which splices bytes byte-for-byte — reads the
// client's chunked frame header as a malformed packet type and falls over.
// By forcing "notchunked", we coerce the client to stay on the pre-54470
// unframed protocol, matching what our splice path actually supports.
//
// Wire layout understood here follows ClickHouse's TCPHandler::sendHello order.
// The important detail is that ClickHouse gates these optional ServerHello
// fields using the ClientHello protocol revision it just received. The server
// revision embedded in this packet is only one side of the compatibility
// decision.
//
//	type                      UVarInt = ServerCodeHello (0)
//	name                      String
//	major, minor, revision    UVarInt
//	parallel_replicas_version UVarInt    (wire rev >= 54471)
//	timezone                  String     (wire rev >= 54058)
//	display_name              String     (wire rev >= 54372)
//	version_patch             UVarInt    (wire rev >= 54401)
//	proto_send_chunked_srv    String     (wire rev >= 54470)  ← rewritten
//	proto_recv_chunked_srv    String     (wire rev >= 54470)  ← rewritten
//	[ trailing unknown bytes passed through unchanged ]
//
// TODO: drop this once the Codec learns to (un)frame chunked packets and the
// proxy can honor the server's chunked_optional advertisement end-to-end.
func rewriteServerHelloChunkedToNotChunked(raw []byte, clientRevision int) ([]byte, error) {
	pos := 0

	readUVarInt := func(field string) (uint64, error) {
		v, n, ok := decodeUVarInt(raw[pos:])
		if !ok {
			return 0, fmt.Errorf("server hello: read %s (varint) at offset %d", field, pos)
		}
		pos += n
		return v, nil
	}
	// readStrBounds returns the raw byte range [start, end) covering the length
	// prefix + string payload — useful when we want to pass a field through
	// unchanged by appending raw[start:end].
	readStrBounds := func(field string) (start, end int, err error) {
		start = pos
		length, n, ok := decodeUVarInt(raw[pos:])
		if !ok {
			return 0, 0, fmt.Errorf("server hello: read %s (str len) at offset %d", field, pos)
		}
		pos += n
		if pos+int(length) > len(raw) {
			return 0, 0, fmt.Errorf("server hello: %s truncated (len=%d, avail=%d)", field, length, len(raw)-pos)
		}
		pos += int(length)
		return start, pos, nil
	}

	typ, err := readUVarInt("type")
	if err != nil {
		return nil, err
	}
	if typ != uint64(proto.ServerCodeHello) {
		return nil, fmt.Errorf("server hello rewrite: unexpected type %d, want %d", typ, proto.ServerCodeHello)
	}

	// name
	if _, _, err := readStrBounds("name"); err != nil {
		return nil, err
	}
	// major, minor, revision
	if _, err := readUVarInt("major"); err != nil {
		return nil, err
	}
	if _, err := readUVarInt("minor"); err != nil {
		return nil, err
	}
	revisionStart := pos
	rev, err := readUVarInt("revision")
	if err != nil {
		return nil, err
	}
	revisionEnd := pos
	irev := int(rev)
	wireRev := irev
	if clientRevision > 0 && clientRevision < wireRev {
		wireRev = clientRevision
	}

	// parallel_replicas_version (54471+) — ordered before timezone per
	// ClickHouse's TCPHandler::sendHello (verified against 25.7.6 and
	// 25.8.20: identical byte layout, field still on the wire — see
	// src/Server/TCPHandler.cpp:1919-1920). The field is still conditional on
	// the client revision, so using only the server revision shifts the cursor
	// when an older client such as clickhouse-go v2.40.3 advertises 54460 to a
	// newer ClickHouse server. That drift surfaces as "display_name truncated".
	if proto.FeatureVersionedParallelReplicas.In(wireRev) {
		if _, err := readUVarInt("parallel_replicas_version"); err != nil {
			return nil, err
		}
	}
	if proto.FeatureTimezone.In(wireRev) {
		if _, _, err := readStrBounds("timezone"); err != nil {
			return nil, err
		}
	}
	if proto.FeatureDisplayName.In(wireRev) {
		if _, _, err := readStrBounds("display_name"); err != nil {
			return nil, err
		}
	}
	if proto.FeatureVersionPatch.In(wireRev) {
		if _, err := readUVarInt("version_patch"); err != nil {
			return nil, err
		}
	}

	rewriteRevision := func(suffixStart int) []byte {
		var negotiated proto.Buffer
		negotiated.PutInt(wireRev)
		out := make([]byte, 0, len(raw)+(len(negotiated.Buf)-(revisionEnd-revisionStart)))
		out = append(out, raw[:revisionStart]...)
		out = append(out, negotiated.Buf...)
		out = append(out, raw[revisionEnd:suffixStart]...)
		return out
	}

	// If the negotiated revision is older than FeatureChunkedPackets, those
	// fields are not on the wire. Still rewrite the server's advertised
	// revision to the negotiated client/proxy revision so the downstream client
	// decodes later packets with the same feature gates as the upstream server.
	if !proto.FeatureChunkedPackets.In(wireRev) {
		out := rewriteRevision(len(raw))
		return out, nil
	}

	// This is the byte range covering the server-side chunked advertisement —
	// everything before it is preserved byte-for-byte, and everything after is
	// forwarded as-is (trailing unknown fields).
	chunkedStart := pos
	if _, _, err := readStrBounds("proto_send_chunked_srv"); err != nil {
		return nil, err
	}
	if _, _, err := readStrBounds("proto_recv_chunked_srv"); err != nil {
		return nil, err
	}
	chunkedEnd := pos

	// Rebuild: prefix + two "notchunked" strings + suffix.
	var replacement proto.Buffer
	replacement.PutString("notchunked")
	replacement.PutString("notchunked")

	out := rewriteRevision(chunkedStart)
	out = append(out, replacement.Buf...)
	out = append(out, raw[chunkedEnd:]...)
	return out, nil
}

// decodeUVarInt reads a base-128 VarUInt from the head of b, returning the
// value, the number of bytes consumed, and whether the slice contained a
// complete varint. Mirrors the ClickHouse/proto encoding.
func decodeUVarInt(b []byte) (val uint64, n int, ok bool) {
	var shift uint
	for i, by := range b {
		val |= uint64(by&0x7F) << shift
		if by&0x80 == 0 {
			return val, i + 1, true
		}
		shift += 7
		if shift > 63 {
			return 0, 0, false
		}
	}
	return 0, 0, false
}

// decodeClientHello reads a ClientHello body from the Codec's proto.Reader.
// The caller must have already consumed the type VarUInt from the wire.
func (c *Codec) decodeClientHello() (*ClientHello, error) {
	h := &proto.ClientHello{}
	if err := h.Decode(c.r); err != nil {
		return nil, fmt.Errorf("%w: client hello: %v", ErrDecode, err)
	}
	return h, nil
}

// decodeServerHello mirrors decodeClientHello for ServerHello.
// ServerHello uses DecodeAware because its layout varies with protocol revision
// (timezone, display_name, patch fields were added in later revisions).
func (c *Codec) decodeServerHello() (*ServerHello, error) {
	h := &proto.ServerHello{}
	if err := h.DecodeAware(c.r, c.Revision()); err != nil {
		return nil, fmt.Errorf("%w: server hello: %v", ErrDecode, err)
	}
	c.serverMajor.Store(int64(h.Major))
	c.serverMinor.Store(int64(h.Minor))
	c.serverPatch.Store(int64(h.Patch))
	return h, nil
}

// WriteClientHello encodes a ClientHello onto the wire. ClientHello.Encode
// already writes the leading type byte, so we call it directly.
func (c *Codec) WriteClientHello(h *ClientHello) error {
	var buf proto.Buffer
	h.Encode(&buf)
	_, err := c.w.Write(buf.Buf)
	return err
}

// WriteServerHello mirrors WriteClientHello for ServerHello. EncodeAware
// already writes the leading type byte.
func (c *Codec) WriteServerHello(h *ServerHello) error {
	var buf proto.Buffer
	h.EncodeAware(&buf, c.Revision())
	_, err := c.w.Write(buf.Buf)
	return err
}
