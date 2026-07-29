package chproto

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// readerWriter bundles a read side and a write side into a single io.ReadWriter
// so a Codec can drive a synthetic byte stream in tests.
type readerWriter struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (rw *readerWriter) Read(p []byte) (int, error)  { return rw.r.Read(p) }
func (rw *readerWriter) Write(p []byte) (int, error) { return rw.w.Write(p) }

func TestReadPacket_ClientHello_Decodes(t *testing.T) {
	hello := &proto.ClientHello{
		Name:            "test-client",
		Major:           1,
		Minor:           2,
		ProtocolVersion: 54453,
		Database:        "default",
		User:            "alice",
		Password:        "secret",
	}
	// ClientHello.Encode already writes the leading type byte, so we call it
	// directly without a separate PutUVarInt to avoid double-writing the type.
	var buf proto.Buffer
	hello.Encode(&buf)

	rw := &readerWriter{r: bytes.NewBuffer(buf.Buf), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)

	pkt, err := c.ReadPacket(uint64(proto.ClientCodeHello))
	if err != nil {
		t.Fatalf("ReadPacket err: %v", err)
	}
	if pkt.Type != uint64(proto.ClientCodeHello) {
		t.Fatalf("type=%d, want Hello", pkt.Type)
	}
	got, ok := pkt.Decoded.(*ClientHello)
	if !ok {
		t.Fatalf("Decoded type=%T, want *ClientHello", pkt.Decoded)
	}
	if got.User != "alice" || got.Database != "default" || got.Password != "secret" {
		t.Fatalf("decoded=%+v, mismatch", got)
	}
}

func TestReadPacket_UnrequestedType_ReturnsRaw(t *testing.T) {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodePing))
	original := append([]byte(nil), buf.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(buf.Buf), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket err: %v", err)
	}
	if pkt.Decoded != nil {
		t.Fatalf("Decoded should be nil for unrequested type, got %T", pkt.Decoded)
	}
	if !bytes.Equal(pkt.Raw, original) {
		t.Fatalf("Raw=%x, want %x", pkt.Raw, original)
	}
	if pkt.Type != uint64(proto.ClientCodePing) {
		t.Fatalf("type=%d, want Ping", pkt.Type)
	}
}

func TestReadPacket_TablesStatusRequest_SkipsBodyAndPreservesRaw(t *testing.T) {
	// Build a TablesStatusRequest with two (database, table) entries.
	// Wire format: [VarUInt: type=5][VarUInt: count][string db][string table]*count
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientTablesStatusRequest))
	buf.PutUVarInt(2)
	buf.PutString("testnet")
	buf.PutString("d3.withdrawl")
	buf.PutString("testnet")
	buf.PutString("d3.deposit")
	original := append([]byte(nil), buf.Buf...)

	// Append a trailing packet so we can verify the codec stopped at the
	// correct boundary — if skipTablesStatusRequest over-consumes, the
	// next ReadPacket would mis-parse Ping.
	var trail proto.Buffer
	trail.PutUVarInt(uint64(proto.ClientCodePing))
	full := append(original, trail.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(full), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)

	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket TablesStatusRequest: %v", err)
	}
	if pkt.Type != uint64(proto.ClientTablesStatusRequest) {
		t.Fatalf("type=%d, want ClientTablesStatusRequest=%d", pkt.Type, proto.ClientTablesStatusRequest)
	}
	if !bytes.Equal(pkt.Raw, original) {
		t.Fatalf("Raw mismatch:\ngot=%x\nwant=%x", pkt.Raw, original)
	}

	// Next packet must parse cleanly — proves we stopped at the right boundary.
	next, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("subsequent ReadPacket: %v", err)
	}
	if next.Type != uint64(proto.ClientCodePing) {
		t.Fatalf("subsequent type=%d, want Ping", next.Type)
	}
}

func TestReadPacket_ModernServerPackets_StopAtExactBoundaries(t *testing.T) {
	const rev = 54483
	var packets [][]byte
	add := func(build func(*proto.Buffer)) {
		var b proto.Buffer
		build(&b)
		packets = append(packets, append([]byte(nil), b.Buf...))
	}

	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerProgressCode))
		for i := uint64(1); i <= 7; i++ {
			b.PutUVarInt(i)
		}
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerProfileCode))
		b.PutUVarInt(1) // rows
		b.PutUVarInt(2) // blocks
		b.PutUVarInt(3) // bytes
		b.PutBool(true)
		b.PutUVarInt(4) // rows before limit
		b.PutBool(false)
		b.PutBool(true)
		b.PutUVarInt(5) // rows before aggregation
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerTablesStatusCode))
		b.PutUVarInt(1)
		b.PutString("db")
		b.PutString("table")
		b.PutBool(true)
		b.PutUVarInt(6) // absolute delay
		b.PutUVarInt(1) // readonly
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerPartUUIDsCode))
		b.PutUVarInt(1)
		b.Buf = append(b.Buf, bytes.Repeat([]byte{0xab}, 16)...)
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerReadTaskRequestCode))
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerTimezoneUpdateCode))
		b.PutString("Asia/Shanghai")
	})
	add(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(ServerEndOfStreamCode))
	})

	var wire bytes.Buffer
	for _, packet := range packets {
		wire.Write(packet)
	}
	rw := &readerWriter{r: bytes.NewBuffer(wire.Bytes()), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(rev)

	for i, want := range packets {
		pkt, err := c.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket[%d]: %v", i, err)
		}
		if !bytes.Equal(pkt.Raw, want) {
			t.Fatalf("ReadPacket[%d] raw mismatch:\n got=%x\nwant=%x", i, pkt.Raw, want)
		}
	}
}

func TestReadPacket_Exception_DecodeAndReencode(t *testing.T) {
	exc := &proto.Exception{
		Code:    60, // UNKNOWN_TABLE
		Name:    "DB::Exception",
		Message: "Table rewritten_tbl not found",
		Stack:   "",
	}
	// Exception.EncodeAware does NOT write the leading type byte (verified from
	// ch-go source: the method only encodes Code/Name/Message/Stack plus the
	// obsolete "has nested" flag), so we must prepend the ServerCodeException
	// VarUInt explicitly.
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeException))
	exc.EncodeAware(&buf, 54453)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetRevision(54453)

	pkt, err := c.ReadPacket(uint64(proto.ServerCodeException))
	if err != nil {
		t.Fatalf("ReadPacket err: %v", err)
	}
	got, ok := pkt.Decoded.(*Exception)
	if !ok {
		t.Fatalf("Decoded type=%T, want *Exception", pkt.Decoded)
	}
	if got.Code != 60 || got.Message != "Table rewritten_tbl not found" {
		t.Fatalf("decoded=%+v, mismatch", got)
	}

	// Mutate message (simulating reverse-mapping) and round-trip.
	got.Message = "Table original_tbl not found"
	out := &readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}
	c2 := NewCodec(out, DirFromClient)
	c2.SetRevision(54453)
	if err := c2.WriteException(got); err != nil {
		t.Fatalf("WriteException: %v", err)
	}

	// Re-read to confirm the mutation survived.
	rw2 := &readerWriter{r: bytes.NewBuffer(out.w.Bytes()), w: &bytes.Buffer{}}
	c3 := NewCodec(rw2, DirToUpstream)
	c3.SetRevision(54453)
	pkt2, err := c3.ReadPacket(uint64(proto.ServerCodeException))
	if err != nil {
		t.Fatalf("reread: %v", err)
	}
	got2 := pkt2.Decoded.(*Exception)
	if got2.Message != "Table original_tbl not found" {
		t.Fatalf("mutated message lost: %q", got2.Message)
	}
}

func TestReadPacket_Query_DecodeAndReencode(t *testing.T) {
	rev := 54453
	q := &proto.Query{
		ID:   "qid-001",
		Body: "SELECT 1",
		Info: proto.ClientInfo{
			ProtocolVersion: 54453,
			Major:           24,
			Minor:           1,
			Patch:           0,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,
		},
	}
	// proto.Query.EncodeAware writes the leading type VarUInt itself, so we
	// do NOT call buf.PutUVarInt(ClientCodeQuery) before it (mirrors the
	// Hello test's setup in Task 2).
	var buf proto.Buffer
	q.EncodeAware(&buf, rev)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(rev)

	pkt, err := c.ReadPacket(uint64(proto.ClientCodeQuery))
	if err != nil {
		t.Fatalf("ReadPacket err: %v", err)
	}
	got, ok := pkt.Decoded.(*Query)
	if !ok {
		t.Fatalf("Decoded type=%T, want *Query", pkt.Decoded)
	}
	if got.Body != "SELECT 1" || got.ID != "qid-001" {
		t.Fatalf("decoded=%+v, mismatch", got)
	}

	// Round-trip: WriteQuery on a second codec should produce identical bytes.
	out := &readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}
	c2 := NewCodec(out, DirToUpstream)
	c2.SetRevision(rev)
	if err := c2.WriteQuery(got); err != nil {
		t.Fatalf("WriteQuery: %v", err)
	}
	if !bytes.Equal(out.w.Bytes(), buf.Buf) {
		t.Fatalf("re-encoded bytes differ\n got %x\nwant %x", out.w.Bytes(), buf.Buf)
	}
}

func TestSplice_NonDataPacket_PassThrough(t *testing.T) {
	// Ping packet: zero-body. Splice should produce identical bytes on the far side.
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodePing))
	src := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(src, DirFromClient)
	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}

	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), buf.Buf) {
		t.Fatalf("Splice dst=%x, want %x", dst.Bytes(), buf.Buf)
	}
}

func TestSplice_ServerPongPacket_PassThrough(t *testing.T) {
	// Server Pong — another zero-body. Uses DirToUpstream direction.
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodePong))
	src := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(src, DirToUpstream)
	pkt, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	dst := &bytes.Buffer{}
	if err := c.Splice(dst, pkt); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), buf.Buf) {
		t.Fatalf("dst=%x want %x", dst.Bytes(), buf.Buf)
	}
}

// TestReadPacket_ServerHello_RewritesChunkedToNotChunked verifies the
// codec's workaround that forces "notchunked" on both chunked-protocol
// advertisements in ServerHello. Without it, a modern client negotiates
// chunked framing based on the server's chunked_optional advertisement and
// the proxy — which does not yet implement chunked framing — breaks on the
// first Query packet.
func TestReadPacket_ServerHello_RewritesChunkedToNotChunked(t *testing.T) {
	// Hand-build a ServerHello at revision 54480 (post-chunked) and append
	// the two chunked fields advertising chunked_optional. Skip trailing
	// fields ch-go doesn't know about — the rewriter should pass those
	// through unchanged, but covering that is the job of a separate test.
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeHello))
	buf.PutString("ClickHouse")
	buf.PutInt(26)                    // major
	buf.PutInt(9)                     // minor
	buf.PutInt(54480)                 // revision
	buf.PutUVarInt(3)                 // parallel_replicas_version (rev >= 54471)
	buf.PutString("Etc/UTC")          // timezone (rev >= 54058)
	buf.PutString("ch-prod")          // display_name (rev >= 54372)
	buf.PutInt(0)                     // version_patch (rev >= 54401)
	buf.PutString("chunked_optional") // proto_send_chunked_srv (rev >= 54470)
	buf.PutString("chunked_optional") // proto_recv_chunked_srv (rev >= 54470)
	onWire := append([]byte(nil), buf.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(onWire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetServerHelloRevisionHint(54480)
	// Intentionally do NOT call SetRevision: ServerHello is decoded before the
	// negotiated revision is known. The separate ServerHello revision hint tells
	// the raw-byte rewriter which optional fields the upstream emitted without
	// making ch-go's ServerHello decoder consume fields it does not model.

	pkt, err := c.ReadPacket(uint64(proto.ServerCodeHello))
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if pkt.Decoded == nil {
		t.Fatal("Decoded is nil")
	}
	if pkt.Raw == nil {
		t.Fatal("Raw is nil; expected rewritten bytes for ServerHello")
	}

	// The rewritten Raw must no longer contain "chunked_optional".
	if bytes.Contains(pkt.Raw, []byte("chunked_optional")) {
		t.Fatalf("Raw still contains chunked_optional after rewrite:\n%x", pkt.Raw)
	}
	// And it must contain exactly two "notchunked" occurrences.
	got := bytes.Count(pkt.Raw, []byte("notchunked"))
	if got != 2 {
		t.Fatalf("Raw contains %d \"notchunked\" occurrences; want 2\n%x", got, pkt.Raw)
	}

	// Non-chunked fields must round-trip byte-for-byte: decode the rewritten
	// Raw using the same manual parsing order and verify major/minor/revision.
	rd := proto.NewReader(bytes.NewReader(pkt.Raw))
	if _, err := rd.UVarInt(); err != nil {
		t.Fatalf("type: %v", err)
	}
	if name, err := rd.Str(); err != nil || name != "ClickHouse" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	if n, err := rd.Int(); err != nil || n != 26 {
		t.Fatalf("major=%d err=%v", n, err)
	}
	if n, err := rd.Int(); err != nil || n != 9 {
		t.Fatalf("minor=%d err=%v", n, err)
	}
	if n, err := rd.Int(); err != nil || n != 54480 {
		t.Fatalf("revision=%d err=%v", n, err)
	}
	if v, err := rd.UVarInt(); err != nil || v != 3 {
		t.Fatalf("parallel_replicas_version=%d err=%v", v, err)
	}
	if s, err := rd.Str(); err != nil || s != "Etc/UTC" {
		t.Fatalf("timezone=%q err=%v", s, err)
	}
	if s, err := rd.Str(); err != nil || s != "ch-prod" {
		t.Fatalf("display_name=%q err=%v", s, err)
	}
	if n, err := rd.Int(); err != nil || n != 0 {
		t.Fatalf("patch=%d err=%v", n, err)
	}
	if s, err := rd.Str(); err != nil || s != "notchunked" {
		t.Fatalf("proto_send_chunked_srv=%q err=%v", s, err)
	}
	if s, err := rd.Str(); err != nil || s != "notchunked" {
		t.Fatalf("proto_recv_chunked_srv=%q err=%v", s, err)
	}
}

func TestReadPacket_ServerHello_UsesClientRevisionForFieldLayout(t *testing.T) {
	// clickhouse-go v2.40.3 advertises client protocol 54460. A modern server
	// still reports a newer server revision in ServerHello, but it does not emit
	// the 54470/54471 optional fields to that client.
	const clientRev = 54460

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeHello))
	buf.PutString("ClickHouse")
	buf.PutInt(26)
	buf.PutInt(9)
	buf.PutInt(54480) // server revision, not the ServerHello wire-layout gate
	buf.PutString("Etc/UTC")
	buf.PutString("ch-go-client")
	buf.PutInt(0)
	onWire := append([]byte(nil), buf.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(onWire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetServerHelloRevisionHint(clientRev)

	pkt, err := c.ReadPacket(uint64(proto.ServerCodeHello))
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(pkt.Raw, onWire) {
		t.Fatalf("ServerHello should be forwarded unchanged for clientRev=%d\ngot  %x\nwant %x", clientRev, pkt.Raw, onWire)
	}
	if bytes.Contains(pkt.Raw, []byte("notchunked")) {
		t.Fatalf("unexpected chunked rewrite for clientRev=%d: %x", clientRev, pkt.Raw)
	}
}

// TestReadPacket_ServerHello_PreservesUnknownTrailingAfterRewrite confirms
// that bytes past proto_recv_chunked_srv (e.g. the future fields
// password_complexity_rules / nonce / server_settings) are passed through
// unchanged even after the chunked rewrite.
func TestReadPacket_ServerHello_PreservesUnknownTrailingAfterRewrite(t *testing.T) {
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeHello))
	buf.PutString("ClickHouse")
	buf.PutInt(26)
	buf.PutInt(9)
	buf.PutInt(54480)
	buf.PutUVarInt(3)
	buf.PutString("Etc/UTC")
	buf.PutString("ch-prod")
	buf.PutInt(0)
	buf.PutString("chunked_optional")
	buf.PutString("chunked_optional")

	// Synthetic trailing bytes — ch-go doesn't know these fields exist. The
	// rewriter must preserve them verbatim.
	trailingMarker := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
	buf.Buf = append(buf.Buf, trailingMarker...)
	onWire := append([]byte(nil), buf.Buf...)

	rw := &readerWriter{r: bytes.NewBuffer(onWire), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirToUpstream)
	c.SetServerHelloRevisionHint(54480)
	// Intentionally do NOT call SetRevision: see the previous ServerHello test
	// for why the raw-byte rewriter uses a separate revision hint.

	pkt, err := c.ReadPacket(uint64(proto.ServerCodeHello))
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.HasSuffix(pkt.Raw, trailingMarker) {
		t.Fatalf("trailing bytes not preserved; tail=%x", pkt.Raw[max(0, len(pkt.Raw)-len(trailingMarker)*2):])
	}
}

// TestReadPacket_ConsecutivePacketsDoNotOrphanBytes is a regression for the
// following bug: the old codec created a fresh tee + proto.Reader on every
// ReadPacket call. proto.Reader's inner bufio would prefetch up to 4KB from
// the underlying stream on the first read, and any bytes beyond the current
// packet were stranded inside that inner bufio when the reader was discarded
// at the end of ReadPacket. A subsequent ReadPacket would create a new
// reader, try to read from a drained bufio, and block forever — manifesting
// as a handshake that "completes" cleanly and then hangs before the first
// query comes through.
//
// The fix was to keep one proto.Reader for the codec's lifetime, fed through
// a capture reader that returns exactly one byte per Read call. With that
// discipline, consecutive packets delivered in a single TCP payload are read
// correctly even when they share a socket buffer.
func TestReadPacket_ConsecutivePacketsDoNotOrphanBytes(t *testing.T) {
	// Two back-to-back Ping packets (single-byte varints, 0x04 each).
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodePing))
	buf.PutUVarInt(uint64(proto.ClientCodePing))

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)

	first, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("first ReadPacket: %v", err)
	}
	if first.Type != uint64(proto.ClientCodePing) {
		t.Fatalf("first packet type=%d, want Ping", first.Type)
	}

	second, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("second ReadPacket (regression; the first call would have orphaned this): %v", err)
	}
	if second.Type != uint64(proto.ClientCodePing) {
		t.Fatalf("second packet type=%d, want Ping", second.Type)
	}
}

// TestReadPacket_AcrossDecodeAndRawBoundaries verifies the same no-orphan
// property when mixing a decoded packet (ClientHello) followed by a raw
// packet (Ping) delivered together — the exact shape a client produces when
// it pipelines ClientHello with subsequent traffic.
func TestReadPacket_AcrossDecodeAndRawBoundaries(t *testing.T) {
	hello := &proto.ClientHello{
		Name:            "pipelined-client",
		Major:           1,
		Minor:           0,
		ProtocolVersion: 54453,
		Database:        "default",
		User:            "alice",
		Password:        "",
	}
	var buf proto.Buffer
	hello.Encode(&buf)
	buf.PutUVarInt(uint64(proto.ClientCodePing))

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)

	pkt1, err := c.ReadPacket(uint64(proto.ClientCodeHello))
	if err != nil {
		t.Fatalf("ReadPacket(Hello): %v", err)
	}
	if _, ok := pkt1.Decoded.(*ClientHello); !ok {
		t.Fatalf("pkt1.Decoded type=%T, want *ClientHello", pkt1.Decoded)
	}

	pkt2, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket(Ping) after pipelined Hello: %v", err)
	}
	if pkt2.Type != uint64(proto.ClientCodePing) {
		t.Fatalf("pkt2 type=%d, want Ping", pkt2.Type)
	}
}

func TestSplice_DecodedPacket_Errors(t *testing.T) {
	// Per spec: callers must NOT Splice a Packet that was decoded; they should
	// call WriteClientHello/WriteQuery/etc. instead. Splice should error out.
	pkt := &Packet{
		Type:    uint64(proto.ClientCodeHello),
		Decoded: &ClientHello{User: "alice"},
	}
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}, DirFromClient)
	dst := &bytes.Buffer{}
	err := c.Splice(dst, pkt)
	if err == nil {
		t.Fatal("Splice on decoded packet: expected error, got nil")
	}
}
