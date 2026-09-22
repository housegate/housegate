package nativepayload

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// encodeTestRevision is housegate's upstream cap (chproto.MaxSupportedRevision),
// which carries FeatureBlockInfo (51903) and FeatureCustomSerialization (54454).
const encodeTestRevision = 54470

func strCol(v ...string) *proto.ColStr {
	c := &proto.ColStr{}
	for _, s := range v {
		c.Append(s)
	}
	return c
}

func timeCol[T interface {
	Append(time.Time)
	proto.ColInput
}](c T, v ...time.Time) T {
	for _, t := range v {
		c.Append(t)
	}
	return c
}

// assertValueEqual compares floats bitwise so NaN and negative zero round trip
// exactly; every other admitted type compares by value.
func assertValueEqual(t *testing.T, i int, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case float32:
		if g, ok := got.(float32); !ok || math.Float32bits(g) != math.Float32bits(w) {
			t.Fatalf("row %d: got %v (%T), want %v bitwise", i, got, got, w)
		}
	case float64:
		if g, ok := got.(float64); !ok || math.Float64bits(g) != math.Float64bits(w) {
			t.Fatalf("row %d: got %v (%T), want %v bitwise", i, got, got, w)
		}
	case time.Time:
		if g, ok := got.(time.Time); !ok || !g.Equal(w) {
			t.Fatalf("row %d: got %v (%T), want %v", i, got, got, w)
		}
	case []byte:
		if g, ok := got.([]byte); !ok || !bytes.Equal(g, w) {
			t.Fatalf("row %d: got %v (%T), want %x", i, got, got, w)
		}
	default:
		if got != want {
			t.Fatalf("row %d: got %#v (%T), want %#v", i, got, got, want)
		}
	}
}

func TestEncodeClientDataPacket_RoundTripsEveryAdmittedType(t *testing.T) {
	var fixed [32]byte
	copy(fixed[:], strings.Repeat("a", 32))
	fixedCol := &proto.ColFixedStr32{}
	fixedCol.Append(fixed)
	epochDate := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	farDate := time.Date(2149, time.June, 6, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	farTime := time.Date(2106, time.February, 7, 6, 28, 15, 0, time.UTC)
	milli := time.Date(2026, time.September, 23, 12, 34, 56, 789000000, time.UTC)
	nano := time.Date(2200, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	nz32, nz64 := float32(math.Copysign(0, -1)), math.Copysign(0, -1)

	for _, tc := range []struct {
		declared string
		col      proto.ColInput
		want     []any
	}{
		{"String", strCol("", "héllo 世界"), []any{"", "héllo 世界"}},
		{"FixedString(32)", fixedCol, []any{fixed[:]}},
		{"Bool", &proto.ColBool{true, false}, []any{true, false}},
		{"Float32", &proto.ColFloat32{float32(math.NaN()), nz32}, []any{float32(math.NaN()), nz32}},
		{"Float64", &proto.ColFloat64{math.NaN(), nz64}, []any{math.NaN(), nz64}},
		{"UInt8", &proto.ColUInt8{0, math.MaxUint8}, []any{uint8(0), uint8(math.MaxUint8)}},
		{"UInt16", &proto.ColUInt16{0, math.MaxUint16}, []any{uint16(0), uint16(math.MaxUint16)}},
		{"UInt32", &proto.ColUInt32{0, math.MaxUint32}, []any{uint32(0), uint32(math.MaxUint32)}},
		{"UInt64", &proto.ColUInt64{0, math.MaxUint64}, []any{uint64(0), uint64(math.MaxUint64)}},
		{"Int8", &proto.ColInt8{math.MinInt8, math.MaxInt8}, []any{int8(math.MinInt8), int8(math.MaxInt8)}},
		{"Int16", &proto.ColInt16{math.MinInt16, math.MaxInt16}, []any{int16(math.MinInt16), int16(math.MaxInt16)}},
		{"Int32", &proto.ColInt32{math.MinInt32, math.MaxInt32}, []any{int32(math.MinInt32), int32(math.MaxInt32)}},
		{"Int64", &proto.ColInt64{math.MinInt64, math.MaxInt64}, []any{int64(math.MinInt64), int64(math.MaxInt64)}},
		{"Date", timeCol(&proto.ColDate{}, epochDate, farDate), []any{epochDate, farDate}},
		{"DateTime", timeCol(&proto.ColDateTime{}, epoch, farTime), []any{epoch, farTime}},
		{"DateTime('UTC')", timeCol(&proto.ColDateTime{Location: time.UTC}, epoch, farTime), []any{epoch, farTime}},
		{"DateTime64(3)", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionMilli), epoch, milli), []any{epoch, milli}},
		{"DateTime64(3, 'UTC')", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC), milli), []any{milli}},
		{"DateTime64(9)", timeCol((&proto.ColDateTime64{}).WithPrecision(proto.PrecisionNano), epoch, nano), []any{epoch, nano}},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			if got := string(tc.col.Type()); got != tc.declared {
				t.Fatalf("ch-go column reports %q, case declares %q", got, tc.declared)
			}
			raw, err := EncodeClientDataPacket(encodeTestRevision, []proto.InputColumn{{Name: "c", Data: tc.col}})
			if err != nil {
				t.Fatalf("EncodeClientDataPacket: %v", err)
			}
			schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "c", Type: tc.declared}}}
			rows, err := Decode(schema, encodeTestRevision, raw)
			if err != nil || len(rows) != len(tc.want) {
				t.Fatalf("Decode returned %d rows (err %v), want %d", len(rows), err, len(tc.want))
			}
			for i, want := range tc.want {
				assertValueEqual(t, i, rows[i].Values[0], want)
			}
		})
	}
}

func TestEncodeClientDataPacket_RefusesUnencodableInput(t *testing.T) {
	uuidCol := &proto.ColUUID{}
	uuidCol.Append(uuid.UUID{})
	for _, tc := range []struct {
		name string
		rev  int
		cols []proto.InputColumn
		want string
	}{
		{"no revision", 0, []proto.InputColumn{{Name: "c", Data: strCol("x")}}, "client protocol revision is required"},
		{"negative revision", -1, []proto.InputColumn{{Name: "c", Data: strCol("x")}}, "client protocol revision is required"},
		{"nil data", encodeTestRevision, []proto.InputColumn{{Name: "c"}}, "has no data"},
		{"typed nil data", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: (*proto.ColStr)(nil)}}, "has no data"},
		{"second nil data", encodeTestRevision, []proto.InputColumn{{Name: "a", Data: strCol("x")}, {Name: "b"}}, "has no data"},
		{"unnamed column", encodeTestRevision, []proto.InputColumn{{Data: strCol("x")}}, "unnamed column"},
		{"noncanonical wire type", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: &noncanonicalFixedColumn{ColFixedStr32: proto.ColFixedStr32{{}}}}}, "not the admitted wire type"},
		{"no columns", encodeTestRevision, nil, "requires at least one column"},
		{"zero rows", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: strCol()}}, "contains no rows"},
		{"reserved column", encodeTestRevision, []proto.InputColumn{{Name: "_hg_row_id", Data: strCol("x")}}, "reserved _hg_row_id"},
		{"duplicate column", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: strCol("x")}, {Name: "c", Data: strCol("y")}}, "duplicate column"},
		{"ragged rows", encodeTestRevision, []proto.InputColumn{{Name: "a", Data: strCol("x", "y")}, {Name: "b", Data: strCol("z")}}, "has 1 rows, first column has 2"},
		{"unsupported type", encodeTestRevision, []proto.InputColumn{{Name: "c", Data: uuidCol}}, "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeClientDataPacket(tc.rev, tc.cols)
			if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
}

// Override only the declaration to exercise the exact wire-type guard.
type noncanonicalFixedColumn struct{ proto.ColFixedStr32 }

func (*noncanonicalFixedColumn) Type() proto.ColumnType { return "FixedString(032)" }

// This fixture pins the derived Native layout; it is not an independently
// captured CLI packet. Independent CLI byte parity remains an integration
// acceptance requirement.
func TestEncodeClientDataPacket_MatchesDerivedOneStringRowPacket(t *testing.T) {
	raw, err := EncodeClientDataPacket(encodeTestRevision, []proto.InputColumn{{Name: "s", Data: strCol("hello")}})
	if err != nil {
		t.Fatal(err)
	}
	goldenHex, err := os.ReadFile("testdata/insert_one_string_row_54470.hex")
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString(strings.TrimSpace(string(goldenHex)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("packet bytes\n got %x\nwant %x", raw, want)
	}
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "s", Type: "String"}}}
	rows, err := Decode(schema, encodeTestRevision, want)
	if err != nil || len(rows) != 1 || rows[0].Values[0] != "hello" {
		t.Fatalf("golden rows=%v err=%v", rows, err)
	}
}

func TestEncodeClientDataPacket_HeaderBytesPerRevisionTier(t *testing.T) {
	// Full-byte pins exercise both sides of BlockInfo (51903) and custom
	// serialization (54454). BucketNum is -1; Overflows is false.
	for _, tc := range []struct {
		rev  int
		want string
	}{
		{51802, "0200010101760655496e7436340100000000000000"},
		{51902, "0200010101760655496e7436340100000000000000"},
		{51903, "0200010002ffffffff00010101760655496e7436340100000000000000"},
		{54058, "0200010002ffffffff00010101760655496e7436340100000000000000"},
		{54453, "0200010002ffffffff00010101760655496e7436340100000000000000"},
		{54454, "0200010002ffffffff00010101760655496e743634000100000000000000"},
		{54470, "0200010002ffffffff00010101760655496e743634000100000000000000"},
	} {
		t.Run(fmt.Sprint(tc.rev), func(t *testing.T) {
			raw, err := EncodeClientDataPacket(tc.rev, []proto.InputColumn{{Name: "v", Data: &proto.ColUInt64{1}}})
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(raw); got != tc.want {
				t.Fatalf("packet bytes\n got %s\nwant %s", got, tc.want)
			}
			schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
			rows, err := Decode(schema, tc.rev, raw)
			if err != nil || len(rows) != 1 || rows[0].Values[0] != uint64(1) {
				t.Fatalf("round trip rows=%v err=%v", rows, err)
			}
		})
	}
}

func TestEncodeClientDataPacket_PreservesColumnsAndPacketOwnership(t *testing.T) {
	stringsCol := strCol("first", "second")
	cols := []proto.InputColumn{{Name: "s", Data: stringsCol}, {Name: "n", Data: &proto.ColInt64{-1, 2}}}
	raw, err := EncodeClientDataPacket(encodeTestRevision, cols)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), raw...)
	stringsCol.Reset()
	stringsCol.Append("changed")
	if !bytes.Equal(raw, original) {
		t.Fatal("returned packet aliases input storage")
	}
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "n", Type: "Int64"}, {Name: "s", Type: "String"}}}
	rows, err := Decode(schema, encodeTestRevision, raw)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	for i, want := range []struct {
		n int64
		s string
	}{{-1, "first"}, {2, "second"}} {
		if rows[i].Values[0] != want.n || rows[i].Values[1] != want.s {
			t.Fatalf("row %d=%v", i, rows[i].Values)
		}
	}
}
