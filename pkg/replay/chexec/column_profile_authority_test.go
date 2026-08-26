package chexec

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// This file is Spec Q Q-D1's proof that the four components agree.
//
// It lives in package chexec, and in an internal test file, because that is the
// only package that already sees all four without an import cycle: payloadexec
// owns the authority and therefore cannot import nativepayload or chexec, both
// of which import it, and supportedColumnType / newScanDest / derefScan are
// unexported. A test in payloadexec — the obvious first attempt — does not
// compile.

const authorityTestRevision = 54453

// authorityTestInstant is the one instant every temporal sample carries. It has
// a non-zero millisecond component so a DateTime64 sample is distinguishable
// from a DateTime one.
var authorityTestInstant = time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)

// sampleColumns supplies one non-empty ch-go column per admitted declaration,
// together with the value the Native lane must decode out of it. The
// completeness assertion below is what makes Q-D1 real: an authority entry with
// no sample fails the build, so a type cannot be admitted without being proved
// decodable, scannable and hashable.
var sampleColumns = map[string]func() (proto.ColInput, any){
	"String": func() (proto.ColInput, any) {
		c := new(proto.ColStr)
		c.Append("abc")
		return c, "abc"
	},
	"FixedString(32)": func() (proto.ColInput, any) {
		var row [32]byte
		copy(row[:], "fixed")
		c := new(proto.ColFixedStr32)
		c.Append(row)
		return c, append([]byte(nil), row[:]...)
	},
	"Bool":    func() (proto.ColInput, any) { return &proto.ColBool{true}, true },
	"Float32": func() (proto.ColInput, any) { return &proto.ColFloat32{1.5}, float32(1.5) },
	"Float64": func() (proto.ColInput, any) { return &proto.ColFloat64{2.5}, float64(2.5) },
	"UInt8":   func() (proto.ColInput, any) { return &proto.ColUInt8{8}, uint8(8) },
	"UInt16":  func() (proto.ColInput, any) { return &proto.ColUInt16{16}, uint16(16) },
	"UInt32":  func() (proto.ColInput, any) { return &proto.ColUInt32{32}, uint32(32) },
	"UInt64":  func() (proto.ColInput, any) { return &proto.ColUInt64{64}, uint64(64) },
	"Int8":    func() (proto.ColInput, any) { return &proto.ColInt8{-8}, int8(-8) },
	"Int16":   func() (proto.ColInput, any) { return &proto.ColInt16{-16}, int16(-16) },
	"Int32":   func() (proto.ColInput, any) { return &proto.ColInt32{-32}, int32(-32) },
	"Int64":   func() (proto.ColInput, any) { return &proto.ColInt64{-64}, int64(-64) },
	"Date": func() (proto.ColInput, any) {
		c := new(proto.ColDate)
		c.Append(authorityTestInstant)
		return c, time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC)
	},
	// ColDateTime with a nil Location reports the bare "DateTime" type and
	// defaults its own Row() to time.Local. nativeColumnValue's .UTC() is what
	// makes the decoded value deterministic, so the expectation below is an
	// instant, compared with time.Time.Equal rather than by Location.
	"DateTime": func() (proto.ColInput, any) {
		c := &proto.ColDateTime{}
		c.Append(authorityTestInstant)
		return c, authorityTestInstant.Truncate(time.Second)
	},
	"DateTime('UTC')": func() (proto.ColInput, any) {
		c := &proto.ColDateTime{Location: time.UTC}
		c.Append(authorityTestInstant)
		return c, authorityTestInstant.Truncate(time.Second)
	},
	"DateTime64(3)": func() (proto.ColInput, any) {
		c := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		c.Append(authorityTestInstant)
		return c, authorityTestInstant.Truncate(time.Millisecond)
	},
	"DateTime64(3, 'UTC')": func() (proto.ColInput, any) {
		c := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
		c.Append(authorityTestInstant)
		return c, authorityTestInstant.Truncate(time.Millisecond)
	},
}

func TestColumnProfileHasASampleForEveryAdmittedType(t *testing.T) {
	for _, declared := range payloadexec.AdmittedColumnTypeVectors() {
		if _, ok := sampleColumns[declared]; !ok {
			t.Errorf("admitted type %q has no sample column: add one, or remove it from the profile", declared)
		}
	}
	for name := range sampleColumns {
		if !payloadexec.SupportedColumnType(name) {
			t.Errorf("sample column %q is not in the admitted profile", name)
		}
	}
}

// TestColumnProfileAgreesAcrossAllFourComponents is Spec Q Q-D1's proof. For
// every admitted declaration it asserts, in one table-driven pass:
//
//  1. the Native lane decodes it and yields the authority's declared GoType;
//  2. chexec admits it for DDL and scans it into the same GoType;
//  3. chexec's derefScan returns that GoType too;
//  4. lthash accepts the value and tags it with the authority's KindTag.
//
// A type in the authority that any of the four cannot handle fails here, which
// is what makes the drift Spec Q §1a and §1e describe unrepeatable.
func TestColumnProfileAgreesAcrossAllFourComponents(t *testing.T) {
	for _, declared := range payloadexec.AdmittedColumnTypeVectors() {
		t.Run(declared, func(t *testing.T) {
			profile, err := payloadexec.ResolveColumnProfile(declared)
			if err != nil {
				t.Fatalf("ResolveColumnProfile(%q): %v", declared, err)
			}
			sample, ok := sampleColumns[declared]
			if !ok {
				t.Fatalf("no sample column for %q", declared)
			}
			column, wantValue := sample()

			// 1. Native decode.
			schema := payloadexec.TableSchema{
				TableID: "tenant.authority",
				Columns: []lthash.Column{{Name: "c", Type: declared}},
			}
			rows, err := nativepayload.Decode(schema, authorityTestRevision, encodeOneColumnNativePacket(t, "c", column))
			if err != nil {
				t.Fatalf("nativepayload.Decode(%q): %v", declared, err)
			}
			if len(rows) != 1 {
				t.Fatalf("nativepayload.Decode(%q) returned %d rows, want 1", declared, len(rows))
			}
			decoded := rows[0].Values[0]
			if got := reflect.TypeOf(decoded); got != profile.GoType {
				t.Fatalf("Native decode of %q yields %s, profile GoType %s", declared, got, profile.GoType)
			}
			assertSameValue(t, "Native decode", declared, decoded, wantValue)

			// 2. chexec DDL admission and read-back destination.
			if !supportedColumnType(declared) {
				t.Fatalf("supportedColumnType(%q) = false, authority admits it", declared)
			}
			dest, err := newScanDest(declared)
			if err != nil {
				t.Fatalf("newScanDest(%q): %v", declared, err)
			}
			if got := reflect.TypeOf(dest).Elem(); got != profile.GoType {
				t.Fatalf("newScanDest(%q) -> *%s, profile GoType %s", declared, got, profile.GoType)
			}

			// 3. chexec read-back normalization. Feeding it the Native lane's own
			// value is the point: the two lanes must be interchangeable.
			reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(decoded))
			scanned, err := derefScan(declared, dest)
			if err != nil {
				t.Fatalf("derefScan(%q): %v", declared, err)
			}
			if got := reflect.TypeOf(scanned); got != profile.GoType {
				t.Fatalf("derefScan(%q) -> %s, profile GoType %s", declared, got, profile.GoType)
			}
			assertSameValue(t, "chexec read-back", declared, scanned, wantValue)

			// 4. lthash acceptance and kind tag.
			encoded, err := lthash.EncodeRow(schema.TableID, schema.Columns, []any{decoded})
			if err != nil {
				t.Fatalf("lthash.EncodeRow(%q): %v", declared, err)
			}
			value := singleColumnValueField(t, encoded)
			if len(value) == 0 {
				t.Fatalf("lthash encoded an empty value field for %q", declared)
			}
			if value[0] != profile.KindTag {
				t.Fatalf("lthash tagged %q with kind %d, profile KindTag %d", declared, value[0], profile.KindTag)
			}
		})
	}
}

// assertSameValue compares by instant for temporal values and by deep equality
// otherwise. A decoded ColDateTime with no declared timezone carries time.Local,
// so comparing Location rather than instant would assert something the profile
// deliberately does not promise.
func assertSameValue(t *testing.T, lane, declared string, got, want any) {
	t.Helper()
	if gotTime, ok := got.(time.Time); ok {
		wantTime, ok := want.(time.Time)
		if !ok {
			t.Fatalf("%s of %q: got time.Time, want %T", lane, declared, want)
		}
		if !gotTime.Equal(wantTime) {
			t.Fatalf("%s of %q = %s, want %s", lane, declared, gotTime, wantTime)
		}
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s of %q = %#v, want %#v", lane, declared, got, want)
	}
}

// encodeOneColumnNativePacket builds a single-row Native ClientData packet.
// nativepayload has an equivalent unexported helper; duplicating twelve lines
// here is cheaper than widening a production package's surface for a test.
func encodeOneColumnNativePacket(t *testing.T, name string, column proto.ColInput) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Rows: column.Rows(), Columns: 1}
	if err := block.EncodeBlock(&buf, authorityTestRevision, proto.Input{{Name: name, Data: column}}); err != nil {
		t.Fatalf("encode native block: %v", err)
	}
	return buf.Buf
}

// singleColumnValueField walks an EncodeRow output — domain ‖ table ‖
// uint32(count) ‖ [name ‖ type ‖ value], every field but the count framed by a
// little-endian uint32 — and returns the one column's value field.
func singleColumnValueField(t *testing.T, encoded []byte) []byte {
	t.Helper()
	rest := encoded
	readFramed := func(what string) []byte {
		t.Helper()
		if len(rest) < 4 {
			t.Fatalf("truncated row element before %s", what)
		}
		n := binary.LittleEndian.Uint32(rest[:4])
		rest = rest[4:]
		if uint32(len(rest)) < n {
			t.Fatalf("truncated %s field", what)
		}
		field := rest[:n]
		rest = rest[n:]
		return field
	}
	readFramed("domain")
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
	return readFramed("value")
}
