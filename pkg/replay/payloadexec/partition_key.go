package payloadexec

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// This file is the partition-key authority of table admission rules v3: the
// one parser of TableSchema.PartitionBy and the one Go evaluation of the
// logical partition id p_<text>, where <text> is exactly what ClickHouse
// reports in system.parts.partition for the row's partition.
//
// Admitted keys (spelled exactly as ClickHouse 25.8 and 26.8 both print them
// in system.tables.partition_key):
//
//   - "" (unpartitioned; the one partition is "all");
//   - a bare declared column of type String (the P1c freeze, unchanged),
//     [U]Int8..64, Date, Date32, DateTime('UTC') or DateTime64(P, 'UTC');
//   - toYYYYMM(c) and toYYYYMMDD(c) over Date, Date32, DateTime('UTC') and
//     DateTime64(P, 'UTC');
//   - toDate(c), toStartOfMonth(c) and toMonday(c) over Date and
//     DateTime('UTC');
//   - toStartOfDay(c) over DateTime('UTC');
//   - intDiv(c, N) over [U]Int32 and [U]Int64, N a canonical decimal literal
//     with 2 <= N <= the column type's maximum.
//
// Everything else is refused, notably: naive DateTime and DateTime64 (the
// partition would follow the server timezone), Date-returning functions over
// Date32 and DateTime64 (25.8 wraps and 26.8 saturates out-of-range values),
// Bool, UUID, FixedString and Float keys, Nullable and LowCardinality columns,
// redundant parentheses, other spacing or case, explicit timezone arguments,
// operators and tuples. Every refusal unwraps to ErrPartitionKeyNotAdmitted.
//
// The measurements behind each rule are replayed by partition_key_golden_test.go
// and checked against a live ClickHouse by pkg/integration.

// ErrPartitionKeyNotAdmitted is the sentinel every partition-key refusal wraps.
var ErrPartitionKeyNotAdmitted = errors.New("payloadexec: partition key is not admitted")

// Partition-key functions. PartitionFuncBare is a bare column.
const (
	PartitionFuncBare           = ""
	PartitionFuncToYYYYMM       = "toYYYYMM"
	PartitionFuncToYYYYMMDD     = "toYYYYMMDD"
	PartitionFuncToDate         = "toDate"
	PartitionFuncToStartOfMonth = "toStartOfMonth"
	PartitionFuncToMonday       = "toMonday"
	PartitionFuncToStartOfDay   = "toStartOfDay"
	PartitionFuncIntDiv         = "intDiv"
)

// partitionKeyQuotedNames are the identifiers ClickHouse 25.8 or 26.8 prints
// backquoted in a partition key, compared case-insensitively. 25.8 quotes
// all, distinct, null and table; 26.8 also from, select and values. A column
// so named would be declared with different spellings on replicas of
// different versions, so a v3 key over it is refused.
var partitionKeyQuotedNames = map[string]bool{
	"all": true, "distinct": true, "from": true, "null": true,
	"select": true, "table": true, "values": true,
}

// PartitionKey is one parsed, admitted partition key. The zero value is the
// unpartitioned key.
type PartitionKey struct {
	partitioned bool
	spelling    string
	column      string
	columnIndex int
	source      ColumnProfile
	function    string
	divisor     uint64
	result      ColumnProfile
}

// ParsePartitionKey parses and admits sch.PartitionBy against sch.Columns.
func ParsePartitionKey(sch TableSchema) (PartitionKey, error) {
	key := sch.PartitionBy
	if key == "" {
		return PartitionKey{}, nil
	}
	if !strings.ContainsAny(key, "()") {
		return parseBarePartitionKey(sch, key)
	}
	return parsePartitionExpression(sch, key)
}

func refusePartitionKey(sch TableSchema, format string, args ...any) error {
	return fmt.Errorf("%w: table %s partition_by %q: %s", ErrPartitionKeyNotAdmitted, sch.TableID, sch.PartitionBy, fmt.Sprintf(format, args...))
}

func findPartitionColumn(sch TableSchema, name string) (int, ColumnProfile, error) {
	for i, col := range sch.Columns {
		if col.Name != name {
			continue
		}
		profile, err := ResolveColumnProfile(col.Type)
		if err != nil {
			return 0, ColumnProfile{}, fmt.Errorf("%w: table %s partition_by %q: partition column %q: %w", ErrPartitionKeyNotAdmitted, sch.TableID, sch.PartitionBy, name, err)
		}
		if profile.Canonical != col.Type {
			return 0, ColumnProfile{}, refusePartitionKey(sch, "column %q type %q is not spelled canonically (%q)", name, col.Type, profile.Canonical)
		}
		if profile.Nullable || profile.LowCardinality {
			return 0, ColumnProfile{}, refusePartitionKey(sch, "column %q has type %s", name, col.Type)
		}
		return i, profile, nil
	}
	return 0, ColumnProfile{}, refusePartitionKey(sch, "names no declared column")
}

func parseBarePartitionKey(sch TableSchema, name string) (PartitionKey, error) {
	idx, profile, err := findPartitionColumn(sch, name)
	if err != nil {
		return PartitionKey{}, err
	}
	// A bare String key is the P1c freeze and keeps its exact v1 semantics:
	// any declared name. Every other key is new in v3 and names only a column
	// both ClickHouse versions print unquoted.
	if profile.Family != FamilyString {
		if err := checkPartitionIdentifier(sch, name); err != nil {
			return PartitionKey{}, err
		}
		if !bareKeyTypeAdmitted(profile) {
			return PartitionKey{}, refusePartitionKey(sch, "column %q of type %s is not an admitted partition key type", name, profile.Canonical)
		}
	}
	return PartitionKey{partitioned: true, spelling: name, column: name, columnIndex: idx, source: profile, result: profile}, nil
}

func bareKeyTypeAdmitted(p ColumnProfile) bool {
	switch p.Family {
	case FamilyString, FamilyUInt, FamilyInt, FamilyDate, FamilyDate32:
		return true
	case FamilyDateTime, FamilyDateTime64:
		return p.Timezone == "UTC"
	}
	return false
}

// checkPartitionIdentifier admits [A-Za-z_][A-Za-z0-9_]* minus the names some
// ClickHouse version prints backquoted.
func checkPartitionIdentifier(sch TableSchema, name string) error {
	if name == "" {
		return refusePartitionKey(sch, "empty column name")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		digit := c >= '0' && c <= '9'
		if !letter && (i == 0 || !digit) {
			return refusePartitionKey(sch, "column name %q needs quoting", name)
		}
	}
	if partitionKeyQuotedNames[strings.ToLower(name)] {
		return refusePartitionKey(sch, "column name %q is printed quoted by some ClickHouse version", name)
	}
	return nil
}

func parsePartitionExpression(sch TableSchema, key string) (PartitionKey, error) {
	open := strings.IndexByte(key, '(')
	if open <= 0 || !strings.HasSuffix(key, ")") {
		return PartitionKey{}, refusePartitionKey(sch, "is not an admitted partition expression")
	}
	function := key[:open]
	inner := key[open+1 : len(key)-1]
	if strings.ContainsAny(inner, "()") {
		return PartitionKey{}, refusePartitionKey(sch, "nested expressions are not admitted")
	}
	var divisorText string
	column := inner
	if function == PartitionFuncIntDiv {
		var ok bool
		column, divisorText, ok = strings.Cut(inner, ", ")
		if !ok {
			return PartitionKey{}, refusePartitionKey(sch, "intDiv must be spelled intDiv(c, N)")
		}
	}
	if err := checkPartitionIdentifier(sch, column); err != nil {
		return PartitionKey{}, err
	}
	idx, source, err := findPartitionColumn(sch, column)
	if err != nil {
		return PartitionKey{}, err
	}
	out := PartitionKey{partitioned: true, spelling: key, column: column, columnIndex: idx, source: source, function: function}
	utcDateTime := source.Family == FamilyDateTime && source.Timezone == "UTC"
	utcDateTime64 := source.Family == FamilyDateTime64 && source.Timezone == "UTC"
	switch function {
	case PartitionFuncToYYYYMM, PartitionFuncToYYYYMMDD:
		if !(source.Family == FamilyDate || source.Family == FamilyDate32 || utcDateTime || utcDateTime64) {
			return PartitionKey{}, refusePartitionKey(sch, "%s over %s is not admitted", function, source.Canonical)
		}
		out.result = scalarColumnProfiles["UInt32"]
	case PartitionFuncToDate, PartitionFuncToStartOfMonth, PartitionFuncToMonday:
		if !(source.Family == FamilyDate || utcDateTime) {
			return PartitionKey{}, refusePartitionKey(sch, "%s over %s is not admitted", function, source.Canonical)
		}
		out.result = scalarColumnProfiles["Date"]
	case PartitionFuncToStartOfDay:
		if !utcDateTime {
			return PartitionKey{}, refusePartitionKey(sch, "%s over %s is not admitted", function, source.Canonical)
		}
		out.result = source
	case PartitionFuncIntDiv:
		maximum, ok := intDivColumnMax(source)
		if !ok {
			return PartitionKey{}, refusePartitionKey(sch, "intDiv over %s is not admitted", source.Canonical)
		}
		n, err := strconv.ParseUint(divisorText, 10, 64)
		if err != nil || strconv.FormatUint(n, 10) != divisorText {
			return PartitionKey{}, refusePartitionKey(sch, "intDiv divisor %q is not a canonical decimal integer literal", divisorText)
		}
		if n < 2 || n > maximum {
			return PartitionKey{}, refusePartitionKey(sch, "intDiv divisor %d is outside [2, %d]", n, maximum)
		}
		out.divisor = n
		out.result = source
	default:
		return PartitionKey{}, refusePartitionKey(sch, "function %q is not an admitted partition function", function)
	}
	return out, nil
}

func intDivColumnMax(p ColumnProfile) (uint64, bool) {
	switch p.Canonical {
	case "UInt32":
		return math.MaxUint32, true
	case "UInt64":
		return math.MaxUint64, true
	case "Int32":
		return math.MaxInt32, true
	case "Int64":
		return math.MaxInt64, true
	}
	return 0, false
}

// IsPartitioned reports whether the key partitions the table at all.
func (k PartitionKey) IsPartitioned() bool { return k.partitioned }

// IsBare reports whether the key is a bare column (also false when
// unpartitioned).
func (k PartitionKey) IsBare() bool { return k.partitioned && k.function == PartitionFuncBare }

// String is the key's canonical spelling: exactly the admitted PartitionBy.
func (k PartitionKey) String() string { return k.spelling }

// Column is the source column's name; empty when unpartitioned.
func (k PartitionKey) Column() string { return k.column }

// ColumnIndex is the source column's position in TableSchema.Columns; -1 when
// unpartitioned.
func (k PartitionKey) ColumnIndex() int {
	if !k.partitioned {
		return -1
	}
	return k.columnIndex
}

// Function is the partition function, PartitionFuncBare for a bare column.
func (k PartitionKey) Function() string { return k.function }

// Divisor is intDiv's N and zero for every other key.
func (k PartitionKey) Divisor() uint64 { return k.divisor }

// ResultType is the canonical ClickHouse type of the key's value, the type
// whose rendering system.parts.partition reports; empty when unpartitioned.
func (k PartitionKey) ResultType() string { return k.result.Canonical }

// PartitionID derives the logical partition id of one schema-ordered row:
// "all" when unpartitioned, else "p_" followed by the key value rendered the
// way system.parts.partition renders it.
func (k PartitionKey) PartitionID(values []any) (string, error) {
	if !k.partitioned {
		return "all", nil
	}
	if k.columnIndex >= len(values) {
		return "", fmt.Errorf("partition key %s: row has %d values", k.spelling, len(values))
	}
	value, err := k.Evaluate(values[k.columnIndex])
	if err != nil {
		return "", err
	}
	text, err := partitionValueString(k.result, value)
	if err != nil {
		return "", fmt.Errorf("partition column %q: %w", k.column, err)
	}
	return "p_" + text, nil
}

// Evaluate applies the key to its source column's value and returns a value of
// the result type's Go type.
func (k PartitionKey) Evaluate(v any) (any, error) {
	if !k.partitioned {
		return nil, fmt.Errorf("unpartitioned table has no partition key value")
	}
	if v == nil {
		return nil, fmt.Errorf("partition column %q carries NULL", k.column)
	}
	if k.function == PartitionFuncBare {
		// A bare key's value is rendered as it comes, by the same renderer
		// as before rules v3, which fails closed on a Go type it has no rule
		// for.
		return v, nil
	}
	if !k.source.AcceptsValue(v) {
		return nil, fmt.Errorf("partition column %q carries %T, want %s", k.column, v, k.source.GoType)
	}
	switch k.function {
	case PartitionFuncIntDiv:
		return k.evaluateIntDiv(v)
	}
	t := v.(time.Time).UTC()
	y, m, d := t.Date()
	switch k.function {
	case PartitionFuncToYYYYMM:
		return uint32(y*100 + int(m)), nil
	case PartitionFuncToYYYYMMDD:
		return uint32(y*10000 + int(m)*100 + d), nil
	case PartitionFuncToDate:
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), nil
	case PartitionFuncToStartOfMonth:
		return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC), nil
	case PartitionFuncToMonday:
		// ClickHouse clamps to the Date range: 1970-01-01 is a Thursday, and
		// toMonday of 1970-01-01 through 1970-01-04 is 1970-01-01 on both
		// 25.8 and 26.8, not the Monday before.
		back := (int(t.Weekday()) + 6) % 7
		monday := time.Date(y, m, d-back, 0, 0, 0, 0, time.UTC)
		if epoch := time.Unix(0, 0).UTC(); monday.Before(epoch) {
			monday = epoch
		}
		return monday, nil
	case PartitionFuncToStartOfDay:
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), nil
	}
	return nil, fmt.Errorf("partition key %s: unknown function", k.spelling)
}

// evaluateIntDiv truncates toward zero, as ClickHouse's intDiv does.
func (k PartitionKey) evaluateIntDiv(v any) (any, error) {
	n := k.divisor
	switch x := v.(type) {
	case uint32:
		return x / uint32(n), nil
	case uint64:
		return x / n, nil
	case int32:
		return x / int32(n), nil
	case int64:
		return x / int64(n), nil
	}
	return nil, fmt.Errorf("partition key %s: intDiv over %T", k.spelling, v)
}

// ParsePartitionText parses one system.parts.partition text of this key back
// into a value of the result type and requires the canonical round trip: text
// must be exactly the rendering of the value it parses to. It checks the
// result type only, not whether the key can produce that value.
func (k PartitionKey) ParsePartitionText(text string) (any, error) {
	if !k.partitioned {
		return nil, fmt.Errorf("unpartitioned table has no partition text")
	}
	value, err := parseValue(k.result.Canonical, text)
	if err != nil {
		return nil, fmt.Errorf("partition text %q is not a %s: %w", text, k.result.Canonical, err)
	}
	rendered, err := partitionValueString(k.result, value)
	if err != nil {
		return nil, err
	}
	if rendered != text {
		return nil, fmt.Errorf("partition text %q is not canonical for %s (want %q)", text, k.result.Canonical, rendered)
	}
	return value, nil
}

// ValidatePartitionID checks that id is a logical partition id this key can
// name: "all" for an unpartitioned table, else p_<text> with text passing
// ParsePartitionText.
func (k PartitionKey) ValidatePartitionID(id string) error {
	if !k.partitioned {
		if id != "all" {
			return fmt.Errorf("unpartitioned table wants partition all, got %q", id)
		}
		return nil
	}
	text, ok := strings.CutPrefix(id, "p_")
	if !ok {
		return fmt.Errorf("partition id %q does not use p_<text> form", id)
	}
	_, err := k.ParsePartitionText(text)
	return err
}
