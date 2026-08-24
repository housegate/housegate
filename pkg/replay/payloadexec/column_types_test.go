package payloadexec

import (
	"errors"
	"math"
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
