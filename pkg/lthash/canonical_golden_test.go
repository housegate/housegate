package lthash

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// This file is the byte-level non-regression evidence Spec Q §4 item 2 requires.
// It pins the canonical *value* encoding — the framed value field encodeValue
// produces — rather than which types the storage-integrity profile admits, so it
// stays meaningful when the profile is widened or narrowed above it. Every row
// below was measured against the pre-Spec-Q encoder; a diff here means an
// admitted type's row hash moved, which is a consensus break rather than a
// widening.

// encodedValueField extracts the single column's framed value field from an
// EncodeRow output. The layout is
//
//	domain ‖ table ‖ uint32(columnCount) ‖ [name ‖ type ‖ value]
//
// with every field but the count length-framed by a little-endian uint32, so a
// one-column row is walked field by field rather than guessed at from the end.
func encodedValueField(t *testing.T, encoded []byte) []byte {
	t.Helper()
	rest := encoded
	readFramed := func(what string) []byte {
		t.Helper()
		if len(rest) < 4 {
			t.Fatalf("truncated row element before %s: %d bytes left", what, len(rest))
		}
		n := binary.LittleEndian.Uint32(rest[:4])
		rest = rest[4:]
		if uint32(len(rest)) < n {
			t.Fatalf("truncated %s field: want %d bytes, have %d", what, n, len(rest))
		}
		field := rest[:n]
		rest = rest[n:]
		return field
	}
	if got := string(readFramed("domain")); got != canonicalDomain {
		t.Fatalf("row element domain = %q, want %q", got, canonicalDomain)
	}
	readFramed("table")
	if len(rest) < 4 {
		t.Fatal("truncated row element before the column count")
	}
	if n := binary.LittleEndian.Uint32(rest[:4]); n != 1 {
		t.Fatalf("row element declares %d columns, this helper handles exactly 1", n)
	}
	rest = rest[4:]
	readFramed("name")
	readFramed("type")
	value := readFramed("value")
	if len(rest) != 0 {
		t.Fatalf("%d trailing bytes after the value field", len(rest))
	}
	return value
}

// TestCanonicalValueEncodingsAreFrozen is the golden. Each row is one admitted
// declared type, one value, and the exact value-field bytes the canonical
// encoder produces for it.
func TestCanonicalValueEncodingsAreFrozen(t *testing.T) {
	day := time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC)
	second := time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)
	milli := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)

	fixed := make([]byte, 32)
	copy(fixed, "fixed")

	for _, tc := range []struct {
		name    string
		typ     string
		value   any
		wantHex string
	}{
		{name: "String", typ: "String", value: "abc", wantHex: "04616263"},
		{
			name: "FixedString(32)", typ: "FixedString(32)", value: fixed,
			wantHex: "04" + "6669786564" + strings.Repeat("00", 27),
		},
		{name: "Bool/true", typ: "Bool", value: true, wantHex: "0501"},
		{name: "Bool/false", typ: "Bool", value: false, wantHex: "0500"},
		{name: "Float32", typ: "Float32", value: float32(1.5), wantHex: "030000c03f"},
		{name: "Float64", typ: "Float64", value: float64(2.5), wantHex: "030000000000000440"},
		{name: "UInt8", typ: "UInt8", value: uint8(8), wantHex: "0208"},
		{name: "UInt16", typ: "UInt16", value: uint16(16), wantHex: "021000"},
		{name: "UInt32", typ: "UInt32", value: uint32(32), wantHex: "0220000000"},
		{name: "UInt64", typ: "UInt64", value: uint64(64), wantHex: "024000000000000000"},
		{name: "Int8", typ: "Int8", value: int8(-8), wantHex: "01f8"},
		{name: "Int16", typ: "Int16", value: int16(-16), wantHex: "01f0ff"},
		{name: "Int32", typ: "Int32", value: int32(-32), wantHex: "01e0ffffff"},
		{name: "Int64", typ: "Int64", value: int64(-64), wantHex: "01c0ffffffffffffff"},
		{name: "Date", typ: "Date", value: day, wantHex: "06aa50000000000000"},

		// DateTime and DateTime('UTC') produce identical value bytes and are
		// separated only by the framed type string. That is the measured
		// evidence for Spec Q Q-D2's timezone decision: the value encoding is
		// already timezone-independent because encodeTime emits an absolute
		// instant, so the canonical spelling — not the value — distinguishes the
		// two declarations. Stripping the timezone during canonicalization would
		// silently merge two schema hashes that ClickHouse keeps distinct.
		{name: "DateTime", typ: "DateTime", value: second, wantHex: "06f0cf586a00000000"},
		{name: "DateTime('UTC')", typ: "DateTime('UTC')", value: second, wantHex: "06f0cf586a00000000"},

		{name: "DateTime64(3, 'UTC')", typ: "DateTime64(3, 'UTC')", value: milli, wantHex: "06c034c88143c5c218"},

		// Date32 at 1900-01-01 encodes as int64(-25567) little-endian under
		// kindTime. encodeTime's strings.HasPrefix(col.Type, "Date") branch
		// computes t.Unix()/86400, which truncates toward zero — correct here
		// only because ColDate32 values are always UTC midnight and therefore
		// exact multiples of 86400 even when negative. A future non-midnight
		// temporal type reaching that branch would be silently wrong.
		{name: "Date32/pre-epoch", typ: "Date32", value: time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC), wantHex: "06219cffffffffffff"},

		// Date and Date32 on the same calendar day produce identical value
		// bytes. That is intended: the framed type string separates them.
		{name: "Date32/same-day-as-Date", typ: "Date32", value: day, wantHex: "06aa50000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeRow("tenant.golden", []Column{{Name: "c", Type: tc.typ}}, []any{tc.value})
			if err != nil {
				t.Fatalf("EncodeRow(%q): %v", tc.typ, err)
			}
			got := hex.EncodeToString(encodedValueField(t, encoded))
			if got != tc.wantHex {
				t.Fatalf("canonical value bytes for %s changed:\n got  %s\n want %s", tc.typ, got, tc.wantHex)
			}
		})
	}
}

// TestCanonicalKindTagsAreFrozen guards Spec Q Q-D3's append-only rule. New tags
// go after these six; renumbering any of them silently rewrites every historical
// row hash.
func TestCanonicalKindTagsAreFrozen(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  byte
		want byte
	}{
		{"kindInt", KindInt, 1},
		{"kindUint", KindUint, 2},
		{"kindFloat", KindFloat, 3},
		{"kindString", KindString, 4},
		{"kindBool", KindBool, 5},
		{"kindTime", KindTime, 6},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d — kind tags are append-only", tc.name, tc.got, tc.want)
		}
	}
}

// TestCanonicalDomainIsUnchanged guards the other half of Q-D3: widening the
// admitted type set, and later appending kind tags, must not bump the MVP row
// profile domain, because neither changes any existing row's bytes.
func TestCanonicalDomainIsUnchanged(t *testing.T) {
	if canonicalDomain != "housegate-row-mvp-v0" {
		t.Fatalf("canonicalDomain = %q; Spec Q Q-D3 requires it stay housegate-row-mvp-v0", canonicalDomain)
	}
}
