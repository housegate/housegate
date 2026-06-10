package lthash

import (
	"math"
	"testing"
	"time"
)

func mustEncode(t *testing.T, table string, cols []Column, values []any) []byte {
	t.Helper()
	b, err := EncodeRow(table, cols, values)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	return b
}

func TestEncodeRowColumnOrderIndependence(t *testing.T) {
	a := mustEncode(t, "db.balances",
		[]Column{{"user_id", "String"}, {"balance", "UInt64"}},
		[]any{"0x123", uint64(10)})
	b := mustEncode(t, "db.balances",
		[]Column{{"balance", "UInt64"}, {"user_id", "String"}},
		[]any{uint64(10), "0x123"})
	if string(a) != string(b) {
		t.Fatal("column order on the wire must not affect the canonical encoding")
	}
}

func TestEncodeRowTableSeparation(t *testing.T) {
	cols := []Column{{"v", "UInt64"}}
	a := mustEncode(t, "db.t1", cols, []any{uint64(1)})
	b := mustEncode(t, "db.t2", cols, []any{uint64(1)})
	if string(a) == string(b) {
		t.Fatal("same row in different tables must encode differently")
	}
}

func TestEncodeRowIntegerWidthDistinct(t *testing.T) {
	a := mustEncode(t, "t", []Column{{"v", "UInt8"}}, []any{uint8(1)})
	b := mustEncode(t, "t", []Column{{"v", "UInt64"}}, []any{uint64(1)})
	if string(a) == string(b) {
		t.Fatal("the same numeric value at different declared widths must encode differently")
	}
}

func TestEncodeRowStringFramingUnambiguous(t *testing.T) {
	cols := []Column{{"a", "String"}, {"b", "String"}}
	x := mustEncode(t, "t", cols, []any{"ab", "c"})
	y := mustEncode(t, "t", cols, []any{"a", "bc"})
	if string(x) == string(y) {
		t.Fatal("adjacent string values must be framed; concatenation must not collide")
	}
}

func TestEncodeRowFloatCanonicalization(t *testing.T) {
	cols := []Column{{"v", "Float64"}}

	nan1 := mustEncode(t, "t", cols, []any{math.NaN()})
	nan2 := mustEncode(t, "t", cols, []any{math.Float64frombits(0x7ff8000000000001)})
	if string(nan1) != string(nan2) {
		t.Fatal("all NaN bit patterns must canonicalize to one encoding")
	}

	negZero := mustEncode(t, "t", cols, []any{math.Copysign(0, -1)})
	posZero := mustEncode(t, "t", cols, []any{float64(0)})
	if string(negZero) != string(posZero) {
		t.Fatal("-0.0 must canonicalize to +0.0")
	}
}

func TestEncodeRowDateTime(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	cols := []Column{{"ts", "DateTime"}}

	utc := mustEncode(t, "t", cols, []any{ts})
	shanghai := mustEncode(t, "t", cols, []any{ts.In(time.FixedZone("CST", 8*3600))})
	if string(utc) != string(shanghai) {
		t.Fatal("DateTime must canonicalize to the absolute instant, independent of zone representation")
	}
}

func TestEncodeRowValueCountMismatch(t *testing.T) {
	if _, err := EncodeRow("t", []Column{{"a", "UInt8"}}, []any{uint8(1), uint8(2)}); err == nil {
		t.Fatal("mismatched column/value counts must error")
	}
}

func TestEncodeRowUnsupportedType(t *testing.T) {
	if _, err := EncodeRow("t", []Column{{"a", "Array(UInt8)"}}, []any{struct{}{}}); err == nil {
		t.Fatal("unsupported value kinds must error, not encode ambiguously")
	}
}

func TestRowHashEndToEnd(t *testing.T) {
	cols := []Column{{"user_id", "String"}, {"balance", "UInt64"}}

	h1, err := RowHash("db.balances", cols, []any{"0x123", uint64(10)})
	if err != nil {
		t.Fatalf("RowHash: %v", err)
	}
	h2, err := RowHash("db.balances", cols, []any{"0x123", uint64(0)})
	if err != nil {
		t.Fatalf("RowHash: %v", err)
	}
	if h1.Equal(h2) {
		t.Fatal("balance=10 and balance=0 must hash differently (the motivating attack)")
	}

	acc := New()
	acc.AddHash(h1)
	acc.SubHash(h1)
	acc.AddHash(h2)
	want, _ := RowHash("db.balances", cols, []any{"0x123", uint64(0)})
	if !acc.Equal(want) {
		t.Fatal("UPDATE semantics (remove old row, add new row) must transition the accumulator exactly")
	}
}
