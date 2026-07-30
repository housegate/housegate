package chproto

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

const (
	revisionMinPasswordComplexityRules = 54461
	revisionMinInterserverSecretV2     = 54462
)

// MaxSupportedRevision is the newest ClickHouse TCP protocol revision the
// terminating Housegate codec models. The pinned ch-go release advertises
// 54460, but Housegate carries its own codecs for the 54461-54470 additions:
// forward-compatible ServerHello tails, Progress/Profile fields, timezone and
// table-status packets, sparse/custom Native flags, and chunked transport.
// Staying exactly at 54470 enables authoritative packet boundaries without
// opting into the later versioned-parallel-replicas fields.
const MaxSupportedRevision = RevisionMinChunkedPackets

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

// rewriteServerHelloForProxy parses a complete on-wire ServerHello packet,
// records the upstream server's original chunked capabilities, and replaces
// its downstream advertisements with Housegate's own "chunked_optional"
// capability. Housegate terminates the protocol, so its client-side and
// upstream-side addenda are negotiated independently. All other fields,
// including the password-complexity rules and interserver nonce, pass through
// unchanged.
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
//	password_rules_count      UVarInt    (wire rev >= 54461)
//	  pattern, message        String × 2 per rule
//	interserver_nonce         UInt64 LE  (wire rev >= 54462)
func rewriteServerHelloForProxy(raw []byte, clientRevision int) (rewritten []byte, serverSend, serverRecv string, err error) {
	pos := 0

	readUVarInt := func(field string) (uint64, error) {
		v, n, ok := decodeUVarInt(raw[pos:])
		if !ok {
			return 0, fmt.Errorf("server hello: read %s (varint) at offset %d", field, pos)
		}
		pos += n
		return v, nil
	}
	// readStr returns the raw byte range [start, end) covering the length
	// prefix + string payload plus its decoded value.
	readStr := func(field string) (start, end int, value string, err error) {
		start = pos
		length, n, ok := decodeUVarInt(raw[pos:])
		if !ok {
			return 0, 0, "", fmt.Errorf("server hello: read %s (str len) at offset %d", field, pos)
		}
		pos += n
		if pos+int(length) > len(raw) {
			return 0, 0, "", fmt.Errorf("server hello: %s truncated (len=%d, avail=%d)", field, length, len(raw)-pos)
		}
		value = string(raw[pos : pos+int(length)])
		pos += int(length)
		return start, pos, value, nil
	}

	typ, err := readUVarInt("type")
	if err != nil {
		return nil, "", "", err
	}
	if typ != uint64(proto.ServerCodeHello) {
		return nil, "", "", fmt.Errorf("server hello rewrite: unexpected type %d, want %d", typ, proto.ServerCodeHello)
	}

	// name
	if _, _, _, err := readStr("name"); err != nil {
		return nil, "", "", err
	}
	// major, minor, revision
	if _, err := readUVarInt("major"); err != nil {
		return nil, "", "", err
	}
	if _, err := readUVarInt("minor"); err != nil {
		return nil, "", "", err
	}
	revisionStart := pos
	rev, err := readUVarInt("revision")
	if err != nil {
		return nil, "", "", err
	}
	revisionEnd := pos
	irev := int(rev)
	wireRev := irev
	if clientRevision > 0 && clientRevision < wireRev {
		wireRev = clientRevision
	}
	if wireRev > MaxSupportedRevision {
		wireRev = MaxSupportedRevision
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
			return nil, "", "", err
		}
	}
	if proto.FeatureTimezone.In(wireRev) {
		if _, _, _, err := readStr("timezone"); err != nil {
			return nil, "", "", err
		}
	}
	if proto.FeatureDisplayName.In(wireRev) {
		if _, _, _, err := readStr("display_name"); err != nil {
			return nil, "", "", err
		}
	}
	if proto.FeatureVersionPatch.In(wireRev) {
		if _, err := readUVarInt("version_patch"); err != nil {
			return nil, "", "", err
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
		return out, "", "", nil
	}

	// Record the real upstream modes before substituting Housegate's own
	// downstream capability.
	chunkedStart := pos
	if _, _, serverSend, err = readStr("proto_send_chunked_srv"); err != nil {
		return nil, "", "", err
	}
	if _, _, serverRecv, err = readStr("proto_recv_chunked_srv"); err != nil {
		return nil, "", "", err
	}
	chunkedEnd := pos

	// Housegate can always receive chunked client packets. It only advertises
	// chunked responses when the upstream leg can provide authoritative
	// packet chunks too; otherwise an opaque Native result may require raw
	// streaming and cannot be safely reframed downstream.
	proxySend := "notchunked"
	if resolveChunkedMode("chunked_optional", serverSend) == "chunked" {
		proxySend = "chunked_optional"
	}

	// Rebuild: prefix + Housegate's advertisements + suffix.
	var replacement proto.Buffer
	replacement.PutString(proxySend)
	replacement.PutString("chunked_optional")

	out := rewriteRevision(chunkedStart)
	out = append(out, replacement.Buf...)
	out = append(out, raw[chunkedEnd:]...)
	return out, serverSend, serverRecv, nil
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

// decodeServerHello consumes the complete ServerHello layout Housegate
// advertises upstream (through revision 54470). It cannot use ch-go's
// DecodeAware directly: the negotiated revision is not known until the
// embedded server revision is read, and the pinned ch-go struct stops before
// password rules, nonce, and chunked capabilities.
//
// Reading every revision-gated field through c.r is important. ServerHello has
// no packet length, so draining only bytes currently buffered by bufio would
// truncate a legal TCP-fragmented hello and misread its remainder as addendum
// or query traffic.
func (c *Codec) decodeServerHello() (*ServerHello, error) {
	h := &proto.ServerHello{}
	var err error
	if h.Name, err = c.r.Str(); err != nil {
		return nil, fmt.Errorf("%w: server hello name: %v", ErrDecode, err)
	}
	if h.Major, err = c.r.Int(); err != nil {
		return nil, fmt.Errorf("%w: server hello major: %v", ErrDecode, err)
	}
	if h.Minor, err = c.r.Int(); err != nil {
		return nil, fmt.Errorf("%w: server hello minor: %v", ErrDecode, err)
	}
	if h.Revision, err = c.r.Int(); err != nil {
		return nil, fmt.Errorf("%w: server hello revision: %v", ErrDecode, err)
	}

	wireRev := h.Revision
	if hint := c.serverHelloRevisionHint(); hint > 0 && hint < wireRev {
		wireRev = hint
	}
	if wireRev > MaxSupportedRevision {
		wireRev = MaxSupportedRevision
	}

	if proto.FeatureVersionedParallelReplicas.In(wireRev) {
		if _, err := c.r.UVarInt(); err != nil {
			return nil, fmt.Errorf("%w: server hello parallel replicas version: %v", ErrDecode, err)
		}
	}
	if proto.FeatureTimezone.In(wireRev) {
		if h.Timezone, err = c.r.Str(); err != nil {
			return nil, fmt.Errorf("%w: server hello timezone: %v", ErrDecode, err)
		}
	}
	if proto.FeatureDisplayName.In(wireRev) {
		if h.DisplayName, err = c.r.Str(); err != nil {
			return nil, fmt.Errorf("%w: server hello display name: %v", ErrDecode, err)
		}
	}
	if proto.FeatureVersionPatch.In(wireRev) {
		if h.Patch, err = c.r.Int(); err != nil {
			return nil, fmt.Errorf("%w: server hello patch: %v", ErrDecode, err)
		}
	}
	if SupportsChunkedPackets(wireRev) {
		if _, err := c.r.Str(); err != nil {
			return nil, fmt.Errorf("%w: server hello send chunk capability: %v", ErrDecode, err)
		}
		if _, err := c.r.Str(); err != nil {
			return nil, fmt.Errorf("%w: server hello receive chunk capability: %v", ErrDecode, err)
		}
	}
	if wireRev >= revisionMinPasswordComplexityRules {
		count, err := c.r.UVarInt()
		if err != nil {
			return nil, fmt.Errorf("%w: server hello password rule count: %v", ErrDecode, err)
		}
		for i := uint64(0); i < count; i++ {
			if _, err := c.r.Str(); err != nil {
				return nil, fmt.Errorf("%w: server hello password rule %d pattern: %v", ErrDecode, i, err)
			}
			if _, err := c.r.Str(); err != nil {
				return nil, fmt.Errorf("%w: server hello password rule %d message: %v", ErrDecode, i, err)
			}
		}
	}
	if wireRev >= revisionMinInterserverSecretV2 {
		if _, err := c.r.UInt64(); err != nil {
			return nil, fmt.Errorf("%w: server hello interserver nonce: %v", ErrDecode, err)
		}
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
