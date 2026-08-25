package payloadexec

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
)

// This file is the single column-type authority Spec Q Q-D1 requires.
//
// Before it existed, four components each carried their own list of admitted
// ClickHouse column types — this package's validator, nativepayload's Native
// decoder, chexec's DDL admission and read-back scan destinations, and lthash's
// value encoder — and they drifted: the validator rejected temporal types every
// executor could already replay, and admitted 16,777,215 FixedString widths for
// a Native decoder that handles one. The table below is now the only place the
// set is written down; every consumer derives from it, and the cross-component
// test in package chexec proves they agree.

// ColumnFamily names one admitted shape of declared type. It is the closed set
// the profile is defined over. Only families with at least one admitted vector
// are declared, so a family constant can never describe a capability the
// executors do not have.
type ColumnFamily string

const (
	FamilyString      ColumnFamily = "String"
	FamilyFixedString ColumnFamily = "FixedString"
	FamilyBool        ColumnFamily = "Bool"
	FamilyFloat       ColumnFamily = "Float"
	FamilyUInt        ColumnFamily = "UInt"
	FamilyInt         ColumnFamily = "Int"
)

// allColumnFamilies is the closed set of declared families. It exists so the
// authority's completeness test can assert that every family the package
// declares is reachable through at least one admitted vector: a family with no
// vector is an untested capability claim, which is exactly the drift Q-D1 ends.
var allColumnFamilies = []ColumnFamily{
	FamilyString,
	FamilyFixedString,
	FamilyBool,
	FamilyFloat,
	FamilyUInt,
	FamilyInt,
}

// ColumnProfile is one resolved declaration. Every consumer reads these fields
// instead of re-parsing the declared type, which is how the four lists stopped
// drifting.
type ColumnProfile struct {
	Family ColumnFamily
	// Canonical is the one spelling stored, hashed and compared. It is what
	// lthash.EncodeRow frames into every row element and what tableSchemaHash
	// digests, so it is consensus-critical.
	Canonical string
	// GoType is the Go value type every materializer must produce for this
	// column, on the Native lane and on the ClickHouse read-back lane alike.
	GoType reflect.Type
	// KindTag is the lthash value kind tag this column's values encode under.
	KindTag byte
	// NativeWireType is the type string a decoded ch-go column reports for this
	// declaration. It equals Canonical for every family admitted today; the
	// Decimal families, where proto.ColAuto downcasts the declared precision and
	// scale away, are the reason the field exists separately.
	NativeWireType string
	// FixedStringWidth is the declared width of a FixedString(N) column and is
	// zero for every other family.
	FixedStringWidth int
}

// scalarColumnProfiles is the fixed-spelling half of the authority: types whose
// declaration has no parameters, so the canonical spelling is the declaration.
var scalarColumnProfiles = buildScalarColumnProfiles(
	ColumnProfile{Family: FamilyString, Canonical: "String", GoType: reflect.TypeOf(""), KindTag: lthash.KindString},
	ColumnProfile{Family: FamilyBool, Canonical: "Bool", GoType: reflect.TypeOf(false), KindTag: lthash.KindBool},
	ColumnProfile{Family: FamilyFloat, Canonical: "Float32", GoType: reflect.TypeOf(float32(0)), KindTag: lthash.KindFloat},
	ColumnProfile{Family: FamilyFloat, Canonical: "Float64", GoType: reflect.TypeOf(float64(0)), KindTag: lthash.KindFloat},
	ColumnProfile{Family: FamilyUInt, Canonical: "UInt8", GoType: reflect.TypeOf(uint8(0)), KindTag: lthash.KindUint},
	ColumnProfile{Family: FamilyUInt, Canonical: "UInt16", GoType: reflect.TypeOf(uint16(0)), KindTag: lthash.KindUint},
	ColumnProfile{Family: FamilyUInt, Canonical: "UInt32", GoType: reflect.TypeOf(uint32(0)), KindTag: lthash.KindUint},
	ColumnProfile{Family: FamilyUInt, Canonical: "UInt64", GoType: reflect.TypeOf(uint64(0)), KindTag: lthash.KindUint},
	ColumnProfile{Family: FamilyInt, Canonical: "Int8", GoType: reflect.TypeOf(int8(0)), KindTag: lthash.KindInt},
	ColumnProfile{Family: FamilyInt, Canonical: "Int16", GoType: reflect.TypeOf(int16(0)), KindTag: lthash.KindInt},
	ColumnProfile{Family: FamilyInt, Canonical: "Int32", GoType: reflect.TypeOf(int32(0)), KindTag: lthash.KindInt},
	ColumnProfile{Family: FamilyInt, Canonical: "Int64", GoType: reflect.TypeOf(int64(0)), KindTag: lthash.KindInt},
)

// scalarColumnProfileOrder pins the declaration order of the fixed-spelling
// entries, because map iteration order would otherwise reorder the admitted
// vector list and the operator-facing rejection message on every run.
var scalarColumnProfileOrder = []string{
	"String",
	"Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64",
	"Int8", "Int16", "Int32", "Int64",
}

func buildScalarColumnProfiles(entries ...ColumnProfile) map[string]ColumnProfile {
	out := make(map[string]ColumnProfile, len(entries))
	for _, entry := range entries {
		// NativeWireType defaults to the canonical spelling: for every family
		// admitted today, a decoded ch-go column reports its declaration back
		// verbatim. Only a family ColAuto downcasts needs to set it explicitly.
		if entry.NativeWireType == "" {
			entry.NativeWireType = entry.Canonical
		}
		out[entry.Canonical] = entry
	}
	return out
}

// fixedStringProfileVector is the one FixedString declaration in the admitted
// vector list. Widths are admitted by grammar, not by enumeration, at this
// point; the vector exists so the cross-component test has a concrete case.
const fixedStringProfileVector = "FixedString(32)"

// ResolveColumnProfile classifies one declared ClickHouse type against the
// pinned storage-integrity profile. It is the only parser: SupportedColumnType,
// ValidateColumnType, CanonicalColumnType and parseValue are all readers of its
// result, so validation cannot admit a type the executor has no branch for.
// Rejections unwrap to ErrUnsupportedColumnType.
func ResolveColumnProfile(typeName string) (ColumnProfile, error) {
	if profile, ok := scalarColumnProfiles[typeName]; ok {
		return profile, nil
	}
	if profile, ok := resolveFixedStringProfile(typeName); ok {
		return profile, nil
	}
	return ColumnProfile{}, unsupportedColumnTypeError(typeName)
}

// resolveFixedStringProfile parses FixedString(N). The spelling grammar is the
// legacy parseFixedString one and is deliberately preserved: surrounding
// whitespace, a leading plus and leading zeroes are accepted and canonicalized
// away rather than rejected.
func resolveFixedStringProfile(typeName string) (ColumnProfile, bool) {
	const prefix = "FixedString("
	if !strings.HasPrefix(typeName, prefix) || !strings.HasSuffix(typeName, ")") {
		return ColumnProfile{}, false
	}
	widthText := strings.TrimSpace(typeName[len(prefix) : len(typeName)-1])
	// ParseInt before any arithmetic so a width literal that overflows int is
	// rejected before it can reach a make([]byte, width) allocation.
	width, err := strconv.ParseInt(widthText, 10, 64)
	if err != nil || width <= 0 || width > maxFixedStringWidth {
		return ColumnProfile{}, false
	}
	canonical := "FixedString(" + strconv.FormatInt(width, 10) + ")"
	return ColumnProfile{
		Family:           FamilyFixedString,
		Canonical:        canonical,
		GoType:           reflect.TypeOf([]byte(nil)),
		KindTag:          lthash.KindString,
		NativeWireType:   canonical,
		FixedStringWidth: int(width),
	}, true
}

// AdmittedColumnTypeVectors returns one concrete admitted declaration per
// distinguishable shape, in canonical spelling. The cross-component authority
// test enumerates exactly this list, so adding a family without adding a vector
// is a test failure rather than an untested widening.
func AdmittedColumnTypeVectors() []string {
	out := make([]string, 0, len(scalarColumnProfileOrder)+1)
	out = append(out, "String", fixedStringProfileVector)
	for _, name := range scalarColumnProfileOrder {
		if name == "String" {
			continue
		}
		out = append(out, name)
	}
	return out
}

// admittedProfileSummary renders the admitted set for operator-facing rejection
// messages, one entry per family, derived from the table itself so widening or
// narrowing the profile updates the message without a second edit.
func admittedProfileSummary() []string {
	grouped := map[ColumnFamily][]string{}
	var order []ColumnFamily
	for _, vector := range AdmittedColumnTypeVectors() {
		profile, err := ResolveColumnProfile(vector)
		if err != nil {
			continue
		}
		if _, seen := grouped[profile.Family]; !seen {
			order = append(order, profile.Family)
		}
		grouped[profile.Family] = append(grouped[profile.Family], vector)
	}
	out := make([]string, 0, len(order))
	for _, family := range order {
		out = append(out, strings.Join(grouped[family], "|"))
	}
	return out
}
