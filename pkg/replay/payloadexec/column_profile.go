package payloadexec

import (
	"reflect"
	"strconv"
	"strings"
	"time"

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
	// Temporal families. Spec Q §1a: the Native decoder, the canonical row
	// encoder and the ClickHouse-backed executor already handle all of these;
	// only this validator rejected them, which is what Spec L D1 narrowed by
	// accident. Admitting them adds no digest byte and no kind tag.
	FamilyDate       ColumnFamily = "Date"
	FamilyDateTime   ColumnFamily = "DateTime"
	FamilyDateTime64 ColumnFamily = "DateTime64"
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
	FamilyDate,
	FamilyDateTime,
	FamilyDateTime64,
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
	// Precision is the declared sub-second precision of a DateTime64(P) column
	// and is zero for every other family. Zero is also a legal DateTime64
	// precision, so read it together with Family.
	Precision int
	// Timezone is the declared timezone of a DateTime(<tz>) or
	// DateTime64(P, <tz>) column and is empty when none was declared. Spec Q
	// Q-D2 keeps it rather than stripping it: the value encoding is already
	// timezone-independent, so the spelling is what distinguishes the two
	// declarations, and ClickHouse reports the timezone back for the physical
	// column.
	Timezone string
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
	ColumnProfile{Family: FamilyDate, Canonical: "Date", GoType: timeType, KindTag: lthash.KindTime},
	ColumnProfile{Family: FamilyDateTime, Canonical: "DateTime", GoType: timeType, KindTag: lthash.KindTime},
)

// timeType is the Go value type every temporal family resolves to. Both lanes
// must produce it in UTC: nativeColumnValue normalizes, and parseValue below
// resolves every accepted text form to UTC, or the two lanes hash the same row
// differently.
var timeType = reflect.TypeOf(time.Time{})

// scalarColumnProfileOrder pins the declaration order of the fixed-spelling
// entries, because map iteration order would otherwise reorder the admitted
// vector list and the operator-facing rejection message on every run.
var scalarColumnProfileOrder = []string{
	"String",
	"Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64",
	"Int8", "Int16", "Int32", "Int64",
	"Date", "DateTime",
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
	if profile, ok := resolveDateTimeProfile(typeName); ok {
		return profile, nil
	}
	if profile, ok := resolveDateTime64Profile(typeName); ok {
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

// maxDateTime64Precision mirrors ch-go's proto.Precision.Valid(), which admits
// 0 through PrecisionNano = 9. The bound is duplicated rather than imported
// because this package deliberately does not depend on ch-go: the authority has
// to be readable by callers that never touch the Native wire. The capability
// pin in pkg/replay/nativepayload is what keeps the two in step across a fork
// bump.
const maxDateTime64Precision = 9

// resolveDateTimeProfile parses DateTime(<tz>). The bare DateTime spelling has
// no parameters and lives in the fixed-spelling table instead.
func resolveDateTimeProfile(typeName string) (ColumnProfile, bool) {
	const prefix = "DateTime("
	if !strings.HasPrefix(typeName, prefix) || !strings.HasSuffix(typeName, ")") {
		return ColumnProfile{}, false
	}
	timezone, ok := parseQuotedTimezone(typeName[len(prefix) : len(typeName)-1])
	if !ok {
		return ColumnProfile{}, false
	}
	canonical := "DateTime('" + timezone + "')"
	return ColumnProfile{
		Family:         FamilyDateTime,
		Canonical:      canonical,
		GoType:         timeType,
		KindTag:        lthash.KindTime,
		NativeWireType: canonical,
		Timezone:       timezone,
	}, true
}

// resolveDateTime64Profile parses DateTime64(P) and DateTime64(P, <tz>).
func resolveDateTime64Profile(typeName string) (ColumnProfile, bool) {
	const prefix = "DateTime64("
	if !strings.HasPrefix(typeName, prefix) || !strings.HasSuffix(typeName, ")") {
		return ColumnProfile{}, false
	}
	inner := typeName[len(prefix) : len(typeName)-1]
	precisionText, timezoneText, hasTimezone := strings.Cut(inner, ",")
	// Leading whitespace, a leading plus and leading zeroes are canonicalized
	// away rather than rejected, matching the FixedString width grammar.
	precision, err := strconv.ParseInt(strings.TrimSpace(precisionText), 10, 64)
	if err != nil || precision < 0 || precision > maxDateTime64Precision {
		return ColumnProfile{}, false
	}
	canonical := "DateTime64(" + strconv.FormatInt(precision, 10)
	timezone := ""
	if hasTimezone {
		var ok bool
		if timezone, ok = parseQuotedTimezone(timezoneText); !ok {
			return ColumnProfile{}, false
		}
		// The comma-space separator matches proto.ColumnType.With, so a decoded
		// ch-go column reconstructs exactly this spelling.
		canonical += ", '" + timezone + "'"
	}
	canonical += ")"
	return ColumnProfile{
		Family:         FamilyDateTime64,
		Canonical:      canonical,
		GoType:         timeType,
		KindTag:        lthash.KindTime,
		NativeWireType: canonical,
		Precision:      int(precision),
		Timezone:       timezone,
	}, true
}

// parseQuotedTimezone accepts a single-quoted IANA timezone and returns its
// canonical name. The zone must load: ch-go's ColDateTime/ColDateTime64 Infer
// calls time.LoadLocation at decode time, and the validator must never be the
// looser of the two. The empty spelling is rejected explicitly because
// time.LoadLocation("") silently answers UTC, which would rewrite a nonsense
// declaration into a valid-looking one.
func parseQuotedTimezone(raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	if len(text) < 3 || text[0] != '\'' || text[len(text)-1] != '\'' {
		return "", false
	}
	name := text[1 : len(text)-1]
	if name == "" || strings.ContainsAny(name, "'\\") {
		return "", false
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return "", false
	}
	// Canonicalize through the loaded location so the stored spelling is
	// exactly what a decoded ch-go column reports for the same declaration.
	return location.String(), true
}

// AdmittedColumnTypeVectors returns one concrete admitted declaration per
// distinguishable shape, in canonical spelling. The cross-component authority
// test enumerates exactly this list, so adding a family without adding a vector
// is a test failure rather than an untested widening.
func AdmittedColumnTypeVectors() []string {
	out := make([]string, 0, len(scalarColumnProfileOrder)+3)
	out = append(out, "String", fixedStringProfileVector)
	for _, name := range scalarColumnProfileOrder {
		if name == "String" {
			continue
		}
		out = append(out, name)
	}
	// The parameterized temporal shapes: a timezone-bearing DateTime, and a
	// DateTime64 with and without one. Each is a distinguishable declaration
	// shape, so each needs its own cross-component vector.
	out = append(out, "DateTime('UTC')", "DateTime64(3)", "DateTime64(3, 'UTC')")
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
