package payloadexec

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
)

// supportedTypeMatrix is the MVP whitelist from the storage-integrity design
// (§5.3) and Spec L D1. It is duplicated here on purpose: the test is the
// frozen statement of the set, so a change to the implementation switch alone
// cannot silently widen or narrow it.
var supportedTypeMatrix = []string{
	"String", "FixedString(32)",
	"FixedString( 32 )", "FixedString(+32)", "FixedString(032)",
	"Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64",
	"Int8", "Int16", "Int32", "Int64",
	// Spec Q Q-D2: the Native decoder, the canonical row encoder and the
	// ClickHouse-backed executor already replay all of these; only this
	// validator rejected them, which is what Spec L D1 narrowed by accident.
	"Date", "DateTime", "DateTime('UTC')", "DateTime64(3)", "DateTime64(3, 'UTC')",
}

var rejectedTypeMatrix = []string{
	"", " String", "string", "Nullable(String)", "Array(UInt64)",
	"LowCardinality(String)", "Decimal(9, 2)", "UUID", "IPv4",
	"Enum8('a' = 1)",
	// Date32 infers in ch-go but nativeColumnValue has no case for it
	// (measurement M2), so it stays rejected until Spec Q Phase 2 teaches the
	// Native lane to decode it.
	"Date32",
	// Temporal parameters the decoder itself would reject at Infer time: an
	// empty or out-of-range DateTime64 precision, and a timezone that does not
	// load. The validator must never be the looser of the two.
	"DateTime64()", "DateTime64(10)", "DateTime64(3, 'Not/AZone')", "DateTime('Not/AZone')",
	"Int128", "UInt256", "FixedString(0)", "FixedString(-1)", "FixedString(x)",
	// Spec Q Q-D7: the validator may never be wider than the Native decoder.
	// nativeColumnValue handles ColFixedStr32 and nothing else, so every other
	// width — including the six ch-go can infer — stays rejected until Phase 2
	// teaches the decoder and bumps the profile together.
	"FixedString(1)", "FixedString(8)", "FixedString(16)", "FixedString(31)",
	"FixedString(33)", "FixedString(64)", "FixedString(255)", "FixedString(512)",
	"FixedString(16777215)",
	"FixedString(4) trailing", "FixedString(4)) ENGINE = MergeTree",
	"FixedString(4) ENGINE = MergeTree)",
	"Map(String, String)", "Tuple(UInt64, String)", "AggregateFunction(sum, UInt64)",
	"String, extra UInt64", "String) ENGINE = MergeTree", "String'",
}

var oversizedFixedStringTypes = []string{
	"FixedString(16777216)",
	"FixedString(2147483647)",
	"FixedString(" + strconv.Itoa(math.MaxInt) + ")",
	"FixedString(9223372036854775808)",
}

// TestValidateColumnType_FixedStringWidthMatchesTheNativeDecoder pins Spec Q
// Q-D7. The admitted width bound is no longer ClickHouse's MAX_FIXEDSTRING_SIZE
// but ch-go's inferGenerated set (proto/col_auto_gen.go: widths 8, 16, 32, 64,
// 128, 256, 512 — measurement M1) intersected with nativeColumnValue's cases
// (native.go: ColFixedStr32 only). That intersection is a single width, and a
// validator wider than the decoder trades a loud startup refusal for a late
// replay failure.
func TestValidateColumnType_FixedStringWidthMatchesTheNativeDecoder(t *testing.T) {
	if err := ValidateColumnType("FixedString(32)"); err != nil {
		t.Fatalf("the one decodable FixedString width was rejected: %v", err)
	}
	// ClickHouse's own maximum width is no longer the bound.
	if err := ValidateColumnType("FixedString(16777215)"); !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("ValidateColumnType(\"FixedString(16777215)\") = %v, want ErrUnsupportedColumnType", err)
	}
	// The six widths ch-go can infer but nativeColumnValue cannot decode.
	for _, width := range []int{8, 16, 64, 128, 256, 512} {
		name := "FixedString(" + strconv.Itoa(width) + ")"
		if SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = true: it infers in ch-go but the Native lane cannot decode it", name)
		}
	}

	for _, name := range oversizedFixedStringTypes {
		if SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = true, want false", name)
		}
		if err := ValidateColumnType(name); !errors.Is(err, ErrUnsupportedColumnType) {
			t.Errorf("ValidateColumnType(%q) = %v, want ErrUnsupportedColumnType", name, err)
		}
	}
}

func TestParseValue_RejectsOversizedFixedStringBeforeAllocation(t *testing.T) {
	for _, name := range oversizedFixedStringTypes {
		if SupportedColumnType(name) {
			t.Fatalf("unsafe test precondition: %q is still classified as supported", name)
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("parseValue(%q) panicked before rejecting the width: %v", name, recovered)
				}
			}()
			if _, err := parseValue(name, "x"); !errors.Is(err, ErrUnsupportedColumnType) {
				t.Fatalf("parseValue(%q) = %v, want ErrUnsupportedColumnType", name, err)
			}
		}()
	}
}

func TestValidateColumnType_AcceptsExactlyTheMVPWhitelist(t *testing.T) {
	for _, name := range supportedTypeMatrix {
		if !SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = false, want true", name)
		}
		if err := ValidateColumnType(name); err != nil {
			t.Errorf("ValidateColumnType(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range rejectedTypeMatrix {
		if SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = true, want false", name)
		}
		err := ValidateColumnType(name)
		if !errors.Is(err, ErrUnsupportedColumnType) {
			t.Errorf("ValidateColumnType(%q) = %v, want ErrUnsupportedColumnType", name, err)
			continue
		}
		if !strings.Contains(err.Error(), name) && name != "" {
			t.Errorf("ValidateColumnType(%q) error %q does not name the offending type", name, err)
		}
	}
}

// parseValue is the pinned executor's row materializer. The validator must
// accept exactly what it can parse: a wider validator would admit a column the
// replay executor cannot materialize (silent divergence), a narrower one would
// reject a table that already works.
func TestValidateColumnType_AgreesWithParseValue(t *testing.T) {
	for _, name := range append(append([]string{}, supportedTypeMatrix...), rejectedTypeMatrix...) {
		_, parseErr := parseValue(name, "1")
		parseRejectsType := errors.Is(parseErr, ErrUnsupportedColumnType)
		if got := ValidateColumnType(name) != nil; got != parseRejectsType {
			t.Errorf("type %q: ValidateColumnType rejects=%v, parseValue rejects-type=%v (parseErr=%v)", name, got, parseRejectsType, parseErr)
		}
	}
}

func TestValidateTableSchemaColumns_NamesTableAndColumn(t *testing.T) {
	schema := TableSchema{
		TableID: "db.t",
		Columns: []lthash.Column{
			{Name: "ok", Type: "UInt64"},
			{Name: "bad_nullable", Type: "Nullable(String)"},
			{Name: "bad_temporal", Type: "Date32"},
		},
	}
	err := ValidateTableSchemaColumns(schema)
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("ValidateTableSchemaColumns = %v, want ErrUnsupportedColumnType", err)
	}
	for _, want := range []string{"db.t", "bad_nullable", "Nullable(String)", "bad_temporal", "Date32"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if err := ValidateTableSchemaColumns(TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "ok", Type: "UInt64"}}}); err != nil {
		t.Fatalf("clean schema rejected: %v", err)
	}
}

func TestCanonicalColumnType_PreservesWhitelistScalars(t *testing.T) {
	for _, typeName := range []string{
		"String", "Bool", "Float32", "Float64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Int8", "Int16", "Int32", "Int64",
	} {
		t.Run(typeName, func(t *testing.T) {
			got, err := CanonicalColumnType(typeName)
			if err != nil {
				t.Fatalf("CanonicalColumnType(%q) error = %v", typeName, err)
			}
			if got != typeName {
				t.Fatalf("CanonicalColumnType(%q) = %q, want unchanged", typeName, got)
			}
		})
	}
}

func TestCanonicalColumnType_NormalizesEveryAcceptedFixedStringSpelling(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "FixedString(32)", want: "FixedString(32)"},
		{input: "FixedString( 32 )", want: "FixedString(32)"},
		{input: "FixedString(+32)", want: "FixedString(32)"},
		{input: "FixedString(0032)", want: "FixedString(32)"},
		{input: "FixedString(\t +0032 \n)", want: "FixedString(32)"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if !SupportedColumnType(tc.input) {
				t.Fatalf("test precondition: %q is not accepted by the authoritative classifier", tc.input)
			}
			got, err := CanonicalColumnType(tc.input)
			if err != nil {
				t.Fatalf("CanonicalColumnType(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("CanonicalColumnType(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestCanonicalColumnType_RejectsUnsupportedAndInjectionSpellings(t *testing.T) {
	for _, typeName := range []string{
		"Nullable(String)",
		"FixedString(16777216)",
		"FixedString(4) ENGINE = MergeTree)",
		"String, injected UInt64",
	} {
		t.Run(typeName, func(t *testing.T) {
			got, err := CanonicalColumnType(typeName)
			if got != "" {
				t.Errorf("CanonicalColumnType(%q) = %q on rejection, want empty", typeName, got)
			}
			if !errors.Is(err, ErrUnsupportedColumnType) {
				t.Fatalf("CanonicalColumnType(%q) error = %v, want ErrUnsupportedColumnType", typeName, err)
			}
			if !strings.Contains(err.Error(), typeName) {
				t.Errorf("CanonicalColumnType(%q) error %q does not name the offending type", typeName, err)
			}
		})
	}
}

func TestCanonicalizeTableSchemaColumnTypes_ReturnsCanonicalDeepCopy(t *testing.T) {
	schema := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "value", Type: "FixedString( +0032 )"},
		},
	}
	original := TableSchema{
		TableID:     schema.TableID,
		PartitionBy: schema.PartitionBy,
		Columns:     append([]lthash.Column(nil), schema.Columns...),
	}

	got, err := CanonicalizeTableSchemaColumnTypes(schema)
	if err != nil {
		t.Fatalf("CanonicalizeTableSchemaColumnTypes error = %v", err)
	}
	want := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "value", Type: "FixedString(32)"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical schema = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(schema, original) {
		t.Fatalf("input schema mutated: got %#v, want %#v", schema, original)
	}

	got.Columns[0].Type = "Int8"
	if !reflect.DeepEqual(schema, original) {
		t.Fatalf("returned schema aliases input columns: input = %#v, want %#v", schema, original)
	}
}

func TestCanonicalizeTableSchemaColumnTypes_JoinsContextualErrors(t *testing.T) {
	schema := TableSchema{
		TableID: "db.t",
		Columns: []lthash.Column{
			{Name: "canonicalized", Type: "FixedString( 032 )"},
			{Name: "bad_nullable", Type: "Nullable(String)"},
			{Name: "bad_injection", Type: "String, injected UInt64"},
		},
	}

	got, err := CanonicalizeTableSchemaColumnTypes(schema)
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("CanonicalizeTableSchemaColumnTypes error = %v, want ErrUnsupportedColumnType", err)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 {
		t.Fatalf("CanonicalizeTableSchemaColumnTypes error = %#v, want two joined failures", err)
	}
	for _, want := range []string{
		"table db.t column \"bad_nullable\"",
		"Nullable(String)",
		"table db.t column \"bad_injection\"",
		"String, injected UInt64",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if got.Columns[0].Type != "FixedString(32)" {
		t.Errorf("supported column was not canonicalized while collecting errors: got %q", got.Columns[0].Type)
	}
	if got.Columns[1].Type != schema.Columns[1].Type || got.Columns[2].Type != schema.Columns[2].Type {
		t.Errorf("unsupported column spellings changed: got %#v, input %#v", got.Columns, schema.Columns)
	}
	if !reflect.DeepEqual(schema.Columns, []lthash.Column{
		{Name: "canonicalized", Type: "FixedString( 032 )"},
		{Name: "bad_nullable", Type: "Nullable(String)"},
		{Name: "bad_injection", Type: "String, injected UInt64"},
	}) {
		t.Fatalf("input schema mutated: %#v", schema)
	}
}

// TestCanonicalColumnType_TemporalSpellings pins Spec Q Q-D5 for the temporal
// families. The canonical spelling is hashed twice — lthash.EncodeRow frames it
// into every row element and tableSchemaHash digests it again — so the exact
// rendering is consensus-critical rather than cosmetic.
//
// The comma-space separator is not a style choice. proto.ColumnType.With joins
// parameters with ", ", so a decoded ch-go column reconstructs
// DateTime64(3, 'UTC') regardless of what crossed the wire; canonicalizing to
// the no-space form would leave NativeWireType permanently unequal to Canonical
// for every DateTime64 carrying a timezone.
func TestCanonicalColumnType_TemporalSpellings(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "Date", want: "Date"},
		{input: "DateTime", want: "DateTime"},
		{input: "DateTime('UTC')", want: "DateTime('UTC')"},
		{input: "DateTime( 'UTC' )", want: "DateTime('UTC')"},
		{input: "DateTime64(3)", want: "DateTime64(3)"},
		{input: "DateTime64( 3 )", want: "DateTime64(3)"},
		{input: "DateTime64(+3)", want: "DateTime64(3)"},
		{input: "DateTime64(03)", want: "DateTime64(3)"},
		{input: "DateTime64(0)", want: "DateTime64(0)"},
		{input: "DateTime64(9)", want: "DateTime64(9)"},
		{input: "DateTime64(3,'UTC')", want: "DateTime64(3, 'UTC')"},
		{input: "DateTime64(3, 'UTC')", want: "DateTime64(3, 'UTC')"},
		{input: "DateTime64( 03 , 'UTC' )", want: "DateTime64(3, 'UTC')"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := CanonicalColumnType(tc.input)
			if err != nil {
				t.Fatalf("CanonicalColumnType(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("CanonicalColumnType(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	for _, typeName := range []string{
		"DateTime64()", "DateTime64( )", "DateTime64(10)", "DateTime64(-1)", "DateTime64(x)",
		"DateTime64(3, UTC)", "DateTime64(3, '')", "DateTime64(3, 'Not/AZone')",
		"DateTime()", "DateTime(UTC)", "DateTime('')", "DateTime('Not/AZone')",
		"Date32", "Date(3)", "DateTime32",
	} {
		t.Run("rejects/"+typeName, func(t *testing.T) {
			got, err := CanonicalColumnType(typeName)
			if got != "" {
				t.Errorf("CanonicalColumnType(%q) = %q on rejection, want empty", typeName, got)
			}
			if !errors.Is(err, ErrUnsupportedColumnType) {
				t.Fatalf("CanonicalColumnType(%q) error = %v, want ErrUnsupportedColumnType", typeName, err)
			}
		})
	}
}

// TestParseValue_TemporalLanesAgreeOnUTC keeps the legacy CSV lane producing the
// same time.Time the Native lane produces for the same row: both resolve to UTC
// and to the column's declared precision, or the two lanes hash differently.
func TestParseValue_TemporalLanesAgreeOnUTC(t *testing.T) {
	for _, tc := range []struct {
		typeName string
		raw      string
		want     time.Time
	}{
		{"Date", "2026-07-16", time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC)},
		{"DateTime", "2026-07-16 12:34:56", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)},
		{"DateTime", "2026-07-16T12:34:56Z", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)},
		{"DateTime('UTC')", "2026-07-16 12:34:56", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)},
		// An offset-bearing RFC3339 form resolves to the same absolute instant.
		{"DateTime", "2026-07-16T20:34:56+08:00", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)},
		{"DateTime64(3)", "2026-07-16 12:34:56.123", time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)},
		{"DateTime64(3, 'UTC')", "2026-07-16T12:34:56.123Z", time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)},
		// Digits past the declared precision are truncated, matching what the
		// Native lane decodes for the same column.
		{"DateTime64(3)", "2026-07-16 12:34:56.123999", time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)},
		{"DateTime64(0)", "2026-07-16 12:34:56.9", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC)},
		{"DateTime64(9)", "2026-07-16 12:34:56.123456789", time.Date(2026, time.July, 16, 12, 34, 56, 123456789, time.UTC)},
	} {
		t.Run(tc.typeName+"/"+tc.raw, func(t *testing.T) {
			v, err := parseValue(tc.typeName, tc.raw)
			if err != nil {
				t.Fatalf("parseValue(%q, %q) = %v", tc.typeName, tc.raw, err)
			}
			got, ok := v.(time.Time)
			if !ok {
				t.Fatalf("parseValue(%q) = %T, want time.Time", tc.typeName, v)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("parseValue(%q, %q) = %s, want %s", tc.typeName, tc.raw, got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("parseValue(%q, %q) location = %s, want UTC", tc.typeName, tc.raw, got.Location())
			}
		})
	}

	for _, tc := range []struct{ typeName, raw string }{
		{"Date", "16/07/2026"},
		{"Date", "2026-07-16 12:34:56"},
		{"DateTime", "not-a-time"},
		{"DateTime64(3)", "not-a-time"},
	} {
		if _, err := parseValue(tc.typeName, tc.raw); err == nil {
			t.Errorf("parseValue(%q, %q) = nil error, want a parse failure", tc.typeName, tc.raw)
		}
	}
}
