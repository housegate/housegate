package payloadexec

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
)

func keySchema(partitionBy, column, columnType string) TableSchema {
	return TableSchema{TableID: "db.t", PartitionBy: partitionBy, Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: column, Type: columnType}}}
}

func TestParsePartitionKeyAdmits(t *testing.T) {
	for _, tc := range []struct {
		key, column, columnType, function, result string
		divisor                                   uint64
	}{
		{"region", "region", "String", PartitionFuncBare, "String", 0},
		{"select", "select", "String", PartitionFuncBare, "String", 0}, // a bare String key keeps v1 semantics
		{"n", "n", "UInt8", PartitionFuncBare, "UInt8", 0},
		{"n", "n", "Int64", PartitionFuncBare, "Int64", 0},
		{"d", "d", "Date32", PartitionFuncBare, "Date32", 0},
		{"ts", "ts", "DateTime('UTC')", PartitionFuncBare, "DateTime('UTC')", 0},
		{"ts", "ts", "DateTime64(9, 'UTC')", PartitionFuncBare, "DateTime64(9, 'UTC')", 0},
		{"toYYYYMM(ts)", "ts", "DateTime64(3, 'UTC')", PartitionFuncToYYYYMM, "UInt32", 0},
		{"toYYYYMMDD(d)", "d", "Date32", PartitionFuncToYYYYMMDD, "UInt32", 0},
		{"toDate(ts)", "ts", "DateTime('UTC')", PartitionFuncToDate, "Date", 0},
		{"toStartOfMonth(d)", "d", "Date", PartitionFuncToStartOfMonth, "Date", 0},
		{"toMonday(ts)", "ts", "DateTime('UTC')", PartitionFuncToMonday, "Date", 0},
		{"toStartOfDay(ts)", "ts", "DateTime('UTC')", PartitionFuncToStartOfDay, "DateTime('UTC')", 0},
		{"intDiv(n, 2)", "n", "UInt32", PartitionFuncIntDiv, "UInt32", 2},
		{"intDiv(n, 4294967295)", "n", "UInt32", PartitionFuncIntDiv, "UInt32", math.MaxUint32},
		{"intDiv(n, 18446744073709551615)", "n", "UInt64", PartitionFuncIntDiv, "UInt64", math.MaxUint64},
		{"intDiv(n, 2147483647)", "n", "Int32", PartitionFuncIntDiv, "Int32", math.MaxInt32},
		{"intDiv(block_number, 1000)", "block_number", "Int64", PartitionFuncIntDiv, "Int64", 1000},
	} {
		key, err := ParsePartitionKey(keySchema(tc.key, tc.column, tc.columnType))
		if err != nil {
			t.Errorf("%s over %s: %v", tc.key, tc.columnType, err)
			continue
		}
		if !key.IsPartitioned() || key.String() != tc.key || key.Column() != tc.column || key.ColumnIndex() != 1 ||
			key.Function() != tc.function || key.ResultType() != tc.result || key.Divisor() != tc.divisor ||
			key.IsBare() != (tc.function == PartitionFuncBare) {
			t.Errorf("%s over %s: got %+v", tc.key, tc.columnType, key)
		}
	}
	key, err := ParsePartitionKey(keySchema("", "x", "String"))
	if err != nil || key.IsPartitioned() || key.ColumnIndex() != -1 || key.ResultType() != "" {
		t.Fatalf("unpartitioned: %+v %v", key, err)
	}
	if id, err := key.PartitionID([]any{uint64(1), "x"}); err != nil || id != "all" {
		t.Fatalf("unpartitioned id = %q, %v", id, err)
	}
}

func TestParsePartitionKeyRefuses(t *testing.T) {
	for _, tc := range []struct{ key, column, columnType string }{
		// Bare types outside D2.
		{"b", "b", "Bool"}, {"f", "f", "Float64"}, {"u", "u", "UUID"}, {"s", "s", "FixedString(32)"},
		{"w", "w", "Int128"}, {"s", "s", "LowCardinality(String)"}, {"s", "s", "Nullable(String)"},
		{"n", "n", "Nullable(UInt64)"},
		// Naive and non-UTC temporal keys (D4).
		{"ts", "ts", "DateTime"}, {"ts", "ts", "DateTime64(3)"}, {"ts", "ts", "DateTime('Asia/Tokyo')"},
		{"toDate(ts)", "ts", "DateTime"}, {"toYYYYMM(ts)", "ts", "DateTime64(3)"}, {"toStartOfDay(ts)", "ts", "DateTime"},
		{"toStartOfDay(d)", "d", "Date"},
		// Date-returning functions over Date32 and DateTime64 (D3: 25.8 wraps, 26.8 saturates).
		{"toDate(d)", "d", "Date32"}, {"toStartOfMonth(d)", "d", "Date32"}, {"toMonday(ts)", "ts", "DateTime64(3, 'UTC')"},
		{"toStartOfDay(ts)", "ts", "DateTime64(0, 'UTC')"},
		// Wrong argument types.
		{"toYYYYMM(n)", "n", "UInt32"}, {"toYYYYMM(s)", "s", "String"}, {"intDiv(n, 1000)", "n", "UInt16"},
		{"intDiv(n, 1000)", "n", "Int8"}, {"intDiv(ts, 1000)", "ts", "DateTime('UTC')"},
		// intDiv divisors.
		{"intDiv(n, 0)", "n", "UInt64"}, {"intDiv(n, 1)", "n", "UInt64"}, {"intDiv(n, -1000)", "n", "Int64"},
		{"intDiv(n, 1000.)", "n", "UInt64"}, {"intDiv(n, 01000)", "n", "UInt64"}, {"intDiv(n, +1000)", "n", "UInt64"},
		{"intDiv(n, 4294967296)", "n", "UInt32"}, {"intDiv(n, 2147483648)", "n", "Int32"},
		{"intDiv(n, 9223372036854775808)", "n", "Int64"}, {"intDiv(n, 18446744073709551616)", "n", "UInt64"},
		{"intDiv(n, toUInt64(1000))", "n", "UInt64"}, {"intDiv(n,1000)", "n", "UInt64"}, {"intDiv(n,  1000)", "n", "UInt64"},
		{"intDiv(n)", "n", "UInt64"}, {"intDiv(n, 1000, 1)", "n", "UInt64"},
		// Spellings ClickHouse versions print differently, or never print (D5).
		{"(toYYYYMM(ts))", "ts", "DateTime('UTC')"}, {"(ts)", "ts", "DateTime('UTC')"}, {"toyyyymm(ts)", "ts", "DateTime('UTC')"},
		{"toYYYYMM( ts )", "ts", "DateTime('UTC')"}, {"toYYYYMM(`ts`)", "ts", "DateTime('UTC')"},
		{"toDate(ts, 'UTC')", "ts", "DateTime('UTC')"}, {"toStartOfWeek(ts, 1)", "ts", "DateTime('UTC')"},
		{"toStartOfDay(toDate(ts))", "ts", "DateTime('UTC')"}, {"n % 16", "n", "UInt64"}, {"modulo(n, 16)", "n", "UInt64"},
		{"tuple(s)", "s", "String"}, {"(s, s)", "s", "String"}, {"toYYYYMM(ts) ", "ts", "DateTime('UTC')"},
		// Names: quoted by some version, needing quotes, or absent.
		{"toYYYYMM(select)", "select", "DateTime('UTC')"}, {"toYYYYMM(Values)", "Values", "DateTime('UTC')"},
		{"intDiv(NULL, 1000)", "NULL", "UInt64"}, {"from", "from", "UInt64"}, {"`select`", "select", "UInt64"},
		{"toYYYYMM(1ts)", "1ts", "DateTime('UTC')"}, {"toYYYYMM(missing)", "ts", "DateTime('UTC')"},
		{"missing", "ts", "UInt64"}, {"my col", "my col", "UInt64"},
		// A non-canonical column type spelling.
		{"ts", "ts", "DateTime( 'UTC' )"},
	} {
		if key, err := ParsePartitionKey(keySchema(tc.key, tc.column, tc.columnType)); !errors.Is(err, ErrPartitionKeyNotAdmitted) {
			t.Errorf("%q over %s %s: got %+v, %v; want ErrPartitionKeyNotAdmitted", tc.key, tc.column, tc.columnType, key, err)
		}
	}
}

func utc(y int, m time.Month, d, hh, mm, ss, ns int) time.Time {
	return time.Date(y, m, d, hh, mm, ss, ns, time.UTC)
}

func TestPartitionKeyEvaluation(t *testing.T) {
	for _, tc := range []struct {
		key, columnType string
		value           any
		want            string
	}{
		{"toYYYYMM(c)", "Date32", utc(1900, 1, 1, 0, 0, 0, 0), "p_190001"},
		{"toYYYYMM(c)", "DateTime64(9, 'UTC')", utc(2262, 4, 11, 23, 47, 16, 854775807), "p_226204"},
		{"toYYYYMMDD(c)", "DateTime('UTC')", utc(2024, 1, 15, 23, 59, 59, 0), "p_20240115"},
		{"toDate(c)", "DateTime('UTC')", utc(2106, 2, 7, 6, 28, 15, 0), "p_2106-02-07"},
		{"toStartOfMonth(c)", "Date", utc(2149, 6, 6, 0, 0, 0, 0), "p_2149-06-01"},
		{"toMonday(c)", "Date", utc(2024, 2, 29, 0, 0, 0, 0), "p_2024-02-26"},
		{"toMonday(c)", "Date", utc(2024, 1, 15, 0, 0, 0, 0), "p_2024-01-15"},
		{"toMonday(c)", "Date", utc(2149, 6, 6, 0, 0, 0, 0), "p_2149-06-02"},
		// The first, partial week clamps to 1970-01-01 (measured on 25.8 and 26.8).
		{"toMonday(c)", "Date", utc(1970, 1, 1, 0, 0, 0, 0), "p_1970-01-01"},
		{"toMonday(c)", "Date", utc(1970, 1, 4, 0, 0, 0, 0), "p_1970-01-01"},
		{"toMonday(c)", "DateTime('UTC')", utc(1970, 1, 4, 23, 59, 59, 0), "p_1970-01-01"},
		{"toMonday(c)", "Date", utc(1970, 1, 5, 0, 0, 0, 0), "p_1970-01-05"},
		{"toStartOfDay(c)", "DateTime('UTC')", utc(2024, 1, 15, 23, 59, 59, 0), "p_2024-01-15 00:00:00"},
		{"intDiv(c, 1000)", "Int64", int64(-1500), "p_-1"},
		{"intDiv(c, 1000)", "Int64", int64(-999), "p_0"},
		{"intDiv(c, 1000)", "Int64", int64(-1000), "p_-1"},
		{"intDiv(c, 1000)", "Int64", int64(math.MinInt64), "p_-9223372036854775"},
		{"intDiv(c, 1000)", "Int32", int32(math.MinInt32), "p_-2147483"},
		{"intDiv(c, 2)", "UInt32", uint32(math.MaxUint32), "p_2147483647"},
		{"intDiv(c, 18446744073709551615)", "UInt64", uint64(math.MaxUint64), "p_1"},
		{"intDiv(c, 9223372036854775807)", "Int64", int64(math.MinInt64), "p_-1"},
		{"c", "Int8", int8(-128), "p_-128"},
		{"c", "DateTime64(3, 'UTC')", utc(1969, 12, 31, 23, 59, 59, 999000000), "p_1969-12-31 23:59:59.999"},
	} {
		key, err := ParsePartitionKey(keySchema(tc.key, "c", tc.columnType))
		if err != nil {
			t.Errorf("%s over %s: %v", tc.key, tc.columnType, err)
			continue
		}
		got, err := key.PartitionID([]any{uint64(0), tc.value})
		if err != nil || got != tc.want {
			t.Errorf("%s over %s of %v = %q, %v; want %q", tc.key, tc.columnType, tc.value, got, err, tc.want)
		}
		if err := key.ValidatePartitionID(tc.want); err != nil {
			t.Errorf("%s: ValidatePartitionID(%q): %v", tc.key, tc.want, err)
		}
	}
}

func TestPartitionKeyEvaluationFailsClosed(t *testing.T) {
	key, err := ParsePartitionKey(keySchema("toYYYYMM(c)", "c", "DateTime('UTC')"))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{nil, "2024-01-15", int64(1)} {
		if _, err := key.PartitionID([]any{uint64(0), v}); err == nil {
			t.Errorf("value %#v: want an error", v)
		}
	}
	if _, err := key.PartitionID([]any{uint64(0)}); err == nil {
		t.Error("short row: want an error")
	}
	div, err := ParsePartitionKey(keySchema("intDiv(c, 1000)", "c", "UInt64"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := div.PartitionID([]any{uint64(0), uint32(5)}); err == nil {
		t.Error("intDiv over a mistyped value: want an error")
	}
}

func TestPartitionKeyParsePartitionText(t *testing.T) {
	for _, tc := range []struct {
		key, columnType string
		ok, bad         []string
	}{
		{"toYYYYMM(c)", "Date", []string{"202401", "0"}, []string{"", "-1", "+5", "0202401", "4294967296", "2024-01"}},
		{"toDate(c)", "DateTime('UTC')", []string{"2024-01-15"}, []string{"2024-1-15", "2024-01-15 00:00:00", "2024-01-15T00:00:00Z"}},
		{"toStartOfDay(c)", "DateTime('UTC')", []string{"2024-01-15 00:00:00"}, []string{"2024-01-15", "2024-01-15T00:00:00Z"}},
		{"c", "DateTime64(3, 'UTC')", []string{"2024-01-15 23:59:59.999"}, []string{"2024-01-15 23:59:59.9", "2024-01-15 23:59:59"}},
		{"intDiv(c, 1000)", "Int64", []string{"-1", "0", "9223372036854775"}, []string{"-0", "01", "1.0", "9223372036854775808"}},
		{"c", "String", []string{"", "a", "it's", "tuple()"}, nil},
	} {
		key, err := ParsePartitionKey(keySchema(tc.key, "c", tc.columnType))
		if err != nil {
			t.Fatalf("%s: %v", tc.key, err)
		}
		for _, text := range tc.ok {
			if _, err := key.ParsePartitionText(text); err != nil {
				t.Errorf("%s: ParsePartitionText(%q): %v", tc.key, text, err)
			}
			if err := key.ValidatePartitionID("p_" + text); err != nil {
				t.Errorf("%s: ValidatePartitionID(p_%s): %v", tc.key, text, err)
			}
		}
		for _, text := range tc.bad {
			if _, err := key.ParsePartitionText(text); err == nil {
				t.Errorf("%s: ParsePartitionText(%q) accepted", tc.key, text)
			}
		}
		if err := key.ValidatePartitionID("all"); err == nil {
			t.Errorf("%s: ValidatePartitionID(all) accepted for a partitioned key", tc.key)
		}
	}
	unpartitioned, _ := ParsePartitionKey(keySchema("", "c", "String"))
	if err := unpartitioned.ValidatePartitionID("all"); err != nil {
		t.Errorf("unpartitioned all: %v", err)
	}
	if err := unpartitioned.ValidatePartitionID("p_x"); err == nil {
		t.Error("unpartitioned p_x accepted")
	}
}
