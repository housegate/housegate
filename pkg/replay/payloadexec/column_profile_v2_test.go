package payloadexec

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/housegate/housegate/pkg/lthash"
)

// TestNullableProfileDescribesItsInnerType pins the Nullable seam's contract:
// the profile keeps T's family, Go type and kind tag, rebuilds the canonical
// and wire spellings around T's, and accepts nil only because it is Nullable.
func TestNullableProfileDescribesItsInnerType(t *testing.T) {
	for _, tc := range []struct {
		declared, canonical string
		family              ColumnFamily
		goType              reflect.Type
		kind                byte
	}{
		{"Nullable(UInt64)", "Nullable(UInt64)", FamilyUInt, reflect.TypeOf(uint64(0)), lthash.KindUint},
		{"Nullable( String )", "Nullable(String)", FamilyString, reflect.TypeOf(""), lthash.KindString},
		{"Nullable(FixedString(032))", "Nullable(FixedString(32))", FamilyFixedString, reflect.TypeOf([]byte(nil)), lthash.KindString},
		{"Nullable(DateTime64(3,'UTC'))", "Nullable(DateTime64(3, 'UTC'))", FamilyDateTime64, timeType, lthash.KindTime},
		{"Nullable(UUID)", "Nullable(UUID)", FamilyUUID, reflect.TypeOf(uuid.UUID{}), lthash.KindUUID},
		{"Nullable(Int256)", "Nullable(Int256)", FamilyWideInt, bigIntType, lthash.KindInt},
	} {
		t.Run(tc.declared, func(t *testing.T) {
			p, err := ResolveColumnProfile(tc.declared)
			if err != nil {
				t.Fatal(err)
			}
			if p.Canonical != tc.canonical || p.NativeWireType != tc.canonical || !p.Nullable || p.LowCardinality {
				t.Fatalf("profile = %+v", p)
			}
			if p.Family != tc.family || p.GoType != tc.goType || p.KindTag != tc.kind {
				t.Fatalf("inner description = %s %s %d", p.Family, p.GoType, p.KindTag)
			}
			if !p.AcceptsValue(nil) {
				t.Fatal("a Nullable column must accept NULL")
			}
			inner, _ := ResolveColumnProfile(tc.canonical[len("Nullable(") : len(tc.canonical)-1])
			if inner.AcceptsValue(nil) {
				t.Fatal("a non-Nullable column must refuse NULL")
			}
		})
	}
}

// TestLowCardinalityStringIsAStringOnTheWireDictionary pins that
// LowCardinality(String) differs from String only in spelling and wire type.
func TestLowCardinalityStringIsAStringOnTheWireDictionary(t *testing.T) {
	p, err := ResolveColumnProfile("LowCardinality(String)")
	if err != nil {
		t.Fatal(err)
	}
	if p.Family != FamilyString || p.GoType != reflect.TypeOf("") || p.KindTag != lthash.KindString ||
		!p.LowCardinality || p.Nullable || p.NativeWireType != "LowCardinality(String)" {
		t.Fatalf("profile = %+v", p)
	}
}

// TestParseValueV2 covers the legacy CSV lane for the v2 families: it must
// produce exactly the profile Go type, NULL only for Nullable, ClickHouse's
// own UUID rendering only, and wide integers only in range.
func TestParseValueV2(t *testing.T) {
	maxU256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	for _, tc := range []struct {
		typ, raw string
		want     any
	}{
		{"Date32", "1900-01-01", time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0")},
		{"Int128", "-170141183460469231731687303715884105728", new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))},
		{"UInt256", maxU256.String(), maxU256},
		{"LowCardinality(String)", "eu", "eu"},
		{"Nullable(UInt64)", `\N`, nil},
		{"Nullable(UInt64)", "7", uint64(7)},
		{"Nullable(String)", `\N`, nil},
	} {
		got, err := parseValue(tc.typ, tc.raw)
		if err != nil {
			t.Fatalf("parseValue(%s, %q) = %v", tc.typ, tc.raw, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("parseValue(%s, %q) = %#v, want %#v", tc.typ, tc.raw, got, tc.want)
		}
		p, _ := ResolveColumnProfile(tc.typ)
		if !p.AcceptsValue(got) {
			t.Fatalf("parseValue(%s) produced %T, outside the profile", tc.typ, got)
		}
	}
	for _, tc := range []struct{ typ, raw string }{
		{"UUID", "61F0C404-5CB3-11E7-907B-A6006AD3DBA0"},
		{"UUID", "{61f0c404-5cb3-11e7-907b-a6006ad3dba0}"},
		{"Int128", "170141183460469231731687303715884105728"},
		{"UInt128", "-1"},
		{"UInt256", new(big.Int).Lsh(big.NewInt(1), 256).String()},
		{"Int256", "1e3"},
		{"UInt64", `\N`},
	} {
		if v, err := parseValue(tc.typ, tc.raw); err == nil {
			t.Fatalf("parseValue(%s, %q) = %#v, want an error", tc.typ, tc.raw, v)
		}
	}
}

// TestPartitionIDForRowRefusesNull keeps a NULL out of the partition id.
func TestPartitionIDForRowRefusesNull(t *testing.T) {
	schema := TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "Nullable(String)"}}}
	if _, err := PartitionIDForRow(schema, []any{nil}); err == nil {
		t.Fatal("PartitionIDForRow accepted a NULL partition value")
	}
}
