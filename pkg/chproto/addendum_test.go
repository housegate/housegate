package chproto

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestNegotiateAddendum_BothChunkedOptional_ResolvesToChunked(t *testing.T) {
	// Client advertises chunked_optional on both sides, with a non-empty quota_key.
	// Wire layout: quota_key, proto_send_chunked, proto_recv_chunked.
	// We omit parallel_replicas_version by setting a revision below FeatureVersionedParallelReplicas (54471).
	var buf proto.Buffer
	buf.PutString("myquota")          // quota_key (non-empty to verify round-trip)
	buf.PutString("chunked_optional") // client proto_send_chunked (what client will send to us → our recv)
	buf.PutString("chunked_optional") // client proto_recv_chunked (what client expects to receive → our send)

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	// Revision 54470 = FeatureChunkedPackets; below 54471 = FeatureVersionedParallelReplicas.
	c.SetRevision(RevisionMinChunkedPackets)

	res, err := c.NegotiateAddendum(AddendumOpts{
		ProposedRecv: "chunked_optional",
		ProposedSend: "chunked_optional",
	})
	if err != nil {
		t.Fatalf("NegotiateAddendum: %v", err)
	}

	// resolveChunkedMode("chunked_optional", "chunked_optional") → "chunked"
	// because neither side is "notchunked" and neither is "", so both-optional upgrades to chunked.
	if res.NegotiatedRecv != "chunked" {
		t.Errorf("NegotiatedRecv = %q, want %q", res.NegotiatedRecv, "chunked")
	}
	if res.NegotiatedSend != "chunked" {
		t.Errorf("NegotiatedSend = %q, want %q", res.NegotiatedSend, "chunked")
	}
	// QuotaKey must round-trip correctly.
	if res.QuotaKey != "myquota" {
		t.Errorf("QuotaKey = %q, want %q", res.QuotaKey, "myquota")
	}
	// Below 54471, ParallelReplicasVersion must be zero.
	if res.ParallelReplicasVersion != 0 {
		t.Errorf("ParallelReplicasVersion = %d, want 0", res.ParallelReplicasVersion)
	}

	// Regression: NegotiateAddendum MUST NOT write anything back to the client.
	// The ClickHouse addendum is one-way (client → server); echoing bytes back
	// manifests on the client as "Unknown packet 0 from server" (empty
	// quota_key starts with 0x00, which the client decodes as ServerHello).
	if n := rw.w.Len(); n != 0 {
		t.Fatalf("NegotiateAddendum wrote %d bytes to client; expected 0 — "+
			"forwarding to upstream must be done via SendAddendum on the "+
			"upstream codec, not implicit in NegotiateAddendum", n)
	}
}

func TestResolveChunkedMode(t *testing.T) {
	cases := []struct {
		ours, theirs, want string
	}{
		{"chunked_optional", "chunked_optional", "chunked"},
		{"chunked", "chunked", "chunked"},
		{"chunked", "chunked_optional", "chunked"},
		{"chunked_optional", "chunked", "chunked"},
		{"notchunked", "chunked", "notchunked"},
		{"chunked", "notchunked", "notchunked"},
		{"notchunked", "notchunked", "notchunked"},
		{"", "chunked", ""},
		{"chunked", "", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		got := resolveChunkedMode(tc.ours, tc.theirs)
		if got != tc.want {
			t.Errorf("resolveChunkedMode(%q, %q) = %q, want %q", tc.ours, tc.theirs, got, tc.want)
		}
	}
}

func TestNegotiateAddendum_NoChunked_BelowRevision(t *testing.T) {
	// At revision < RevisionMinChunkedPackets, only quota_key is read.
	var buf proto.Buffer
	buf.PutString("mykey") // quota_key only; no chunked fields expected at this revision.

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	// Revision 54458 = RevisionMinAddendum, below RevisionMinChunkedPackets (54470).
	c.SetRevision(RevisionMinAddendum)

	res, err := c.NegotiateAddendum(AddendumOpts{
		ProposedRecv: "chunked_optional",
		ProposedSend: "chunked_optional",
	})
	if err != nil {
		t.Fatalf("NegotiateAddendum: %v", err)
	}

	// No chunked fields → resolveChunkedMode("chunked_optional", "") = ""
	if res.NegotiatedRecv != "" {
		t.Errorf("NegotiatedRecv = %q, want %q", res.NegotiatedRecv, "")
	}
	if res.NegotiatedSend != "" {
		t.Errorf("NegotiatedSend = %q, want %q", res.NegotiatedSend, "")
	}
	if res.QuotaKey != "mykey" {
		t.Errorf("QuotaKey = %q, want %q", res.QuotaKey, "mykey")
	}
	if res.ParallelReplicasVersion != 0 {
		t.Errorf("ParallelReplicasVersion = %d, want 0", res.ParallelReplicasVersion)
	}
}

// TestSendAddendum_WritesAllFields_AboveParallelReplicas drives SendAddendum at revision
// 54471 (RevisionMinVersionedParallelReplicas) and verifies that all four addendum
// fields are emitted in the correct wire order:
//
//	quota_key                 String
//	proto_send_chunked        String   (NegotiatedSend)
//	proto_recv_chunked        String   (NegotiatedRecv)
//	parallel_replicas_version UVarInt
func TestSendAddendum_WritesAllFields_AboveParallelReplicas(t *testing.T) {
	var out bytes.Buffer
	rw := &readerWriter{r: &bytes.Buffer{}, w: &out}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(RevisionMinVersionedParallelReplicas) // 54471

	res := AddendumResult{
		QuotaKey:                "tenant-42",
		NegotiatedSend:          "chunked",
		NegotiatedRecv:          "notchunked",
		ParallelReplicasVersion: 7,
	}
	if err := c.SendAddendum(res); err != nil {
		t.Fatalf("SendAddendum: %v", err)
	}

	// Hand-compute expected bytes using the same proto.Buffer approach.
	var expected proto.Buffer
	expected.PutString("tenant-42")  // quota_key
	expected.PutString("chunked")    // proto_send_chunked  (NegotiatedSend)
	expected.PutString("notchunked") // proto_recv_chunked  (NegotiatedRecv)
	expected.PutUVarInt(7)           // parallel_replicas_version

	got := out.Bytes()
	want := expected.Buf
	if !bytes.Equal(got, want) {
		t.Errorf("SendAddendum output mismatch:\n  got  %v\n  want %v", got, want)
	}

	// Also decode field-by-field for human-readable failure messages.
	rd := proto.NewReader(bytes.NewReader(got))
	gotQuota, err := rd.Str()
	if err != nil || gotQuota != "tenant-42" {
		t.Errorf("field[0] quota_key = %q (err=%v), want %q", gotQuota, err, "tenant-42")
	}
	gotSend, err := rd.Str()
	if err != nil || gotSend != "chunked" {
		t.Errorf("field[1] proto_send_chunked = %q (err=%v), want %q", gotSend, err, "chunked")
	}
	gotRecv, err := rd.Str()
	if err != nil || gotRecv != "notchunked" {
		t.Errorf("field[2] proto_recv_chunked = %q (err=%v), want %q", gotRecv, err, "notchunked")
	}
	// parallel_replicas_version is UVarInt (base-128); verify the raw byte value for 7.
	// For value 7 (fits in one VarUInt byte with no continuation bit), the encoded byte is 0x07.
	lastByte := got[len(got)-1]
	if lastByte != 0x07 {
		t.Errorf("parallel_replicas_version encoded byte = 0x%02x, want 0x07", lastByte)
	}
}

// TestNegotiateAddendum_StoresParallelReplicasVersion verifies that NegotiateAddendum
// reads and preserves the parallel_replicas_version field when revision >= 54471.
func TestNegotiateAddendum_StoresParallelReplicasVersion(t *testing.T) {
	var buf proto.Buffer
	buf.PutString("qk")      // quota_key
	buf.PutString("chunked") // client proto_send_chunked
	buf.PutString("chunked") // client proto_recv_chunked
	buf.PutUVarInt(42)       // parallel_replicas_version

	rw := &readerWriter{r: bytes.NewBuffer(append([]byte(nil), buf.Buf...)), w: &bytes.Buffer{}}
	c := NewCodec(rw, DirFromClient)
	c.SetRevision(RevisionMinVersionedParallelReplicas) // 54471

	res, err := c.NegotiateAddendum(AddendumOpts{
		ProposedRecv: "chunked",
		ProposedSend: "chunked",
	})
	if err != nil {
		t.Fatalf("NegotiateAddendum: %v", err)
	}

	if res.QuotaKey != "qk" {
		t.Errorf("QuotaKey = %q, want %q", res.QuotaKey, "qk")
	}
	if res.NegotiatedRecv != "chunked" {
		t.Errorf("NegotiatedRecv = %q, want %q", res.NegotiatedRecv, "chunked")
	}
	if res.NegotiatedSend != "chunked" {
		t.Errorf("NegotiatedSend = %q, want %q", res.NegotiatedSend, "chunked")
	}
	if res.ParallelReplicasVersion != 42 {
		t.Errorf("ParallelReplicasVersion = %d, want 42", res.ParallelReplicasVersion)
	}

	// Regression: NegotiateAddendum is read-only on the client-facing side.
	// See TestNegotiateAddendum_BothChunkedOptional_ResolvesToChunked for the
	// rationale.
	if n := rw.w.Len(); n != 0 {
		t.Fatalf("NegotiateAddendum wrote %d bytes to client; expected 0", n)
	}
}
