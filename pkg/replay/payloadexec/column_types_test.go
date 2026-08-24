package payloadexec

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
)

// supportedTypeMatrix is the MVP whitelist from the storage-integrity design
// (§5.3) and Spec L D1. It is duplicated here on purpose: the test is the
// frozen statement of the set, so a change to the implementation switch alone
// cannot silently widen or narrow it.
var supportedTypeMatrix = []string{
	"String", "FixedString(1)", "FixedString(32)", "FixedString(255)",
	"FixedString( 4 )", "FixedString(+4)", "FixedString(04)",
	"Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64",
	"Int8", "Int16", "Int32", "Int64",
}

var rejectedTypeMatrix = []string{
	"", " String", "string", "Nullable(String)", "Array(UInt64)",
	"LowCardinality(String)", "Decimal(9, 2)", "UUID", "IPv4",
	"Date", "DateTime", "DateTime64(3)", "Enum8('a' = 1)",
	"Int128", "UInt256", "FixedString(0)", "FixedString(-1)", "FixedString(x)",
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

func TestValidateColumnType_FixedStringWidthMatchesClickHouse25_8(t *testing.T) {
	if err := ValidateColumnType("FixedString(16777215)"); err != nil {
		t.Fatalf("ClickHouse maximum FixedString width rejected: %v", err)
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
			{Name: "bad_temporal", Type: "DateTime"},
		},
	}
	err := ValidateTableSchemaColumns(schema)
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("ValidateTableSchemaColumns = %v, want ErrUnsupportedColumnType", err)
	}
	for _, want := range []string{"db.t", "bad_nullable", "Nullable(String)", "bad_temporal", "DateTime"} {
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
		{input: "FixedString(1)", want: "FixedString(1)"},
		{input: "FixedString( 1 )", want: "FixedString(1)"},
		{input: "FixedString(+1)", want: "FixedString(1)"},
		{input: "FixedString(0001)", want: "FixedString(1)"},
		{input: "FixedString(\t +0001 \n)", want: "FixedString(1)"},
		{input: "FixedString( +016777215 )", want: "FixedString(16777215)"},
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
			{Name: "value", Type: "FixedString( +0008 )"},
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
			{Name: "value", Type: "FixedString(8)"},
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
			{Name: "canonicalized", Type: "FixedString( 004 )"},
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
	if got.Columns[0].Type != "FixedString(4)" {
		t.Errorf("supported column was not canonicalized while collecting errors: got %q", got.Columns[0].Type)
	}
	if got.Columns[1].Type != schema.Columns[1].Type || got.Columns[2].Type != schema.Columns[2].Type {
		t.Errorf("unsupported column spellings changed: got %#v, input %#v", got.Columns, schema.Columns)
	}
	if !reflect.DeepEqual(schema.Columns, []lthash.Column{
		{Name: "canonicalized", Type: "FixedString( 004 )"},
		{Name: "bad_nullable", Type: "Nullable(String)"},
		{Name: "bad_injection", Type: "String, injected UInt64"},
	}) {
		t.Fatalf("input schema mutated: %#v", schema)
	}
}
