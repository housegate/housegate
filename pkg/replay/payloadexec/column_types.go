package payloadexec

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrUnsupportedColumnType is the sentinel every column-type rejection unwraps
// to. Spec L D1: the storage-integrity profile validates declared column types
// against the set this package's pinned executor can actually materialize,
// rather than against ClickHouse's grammar, because a type ClickHouse accepts
// but the executor cannot replay is a worse failure (silent divergence) than a
// refusal to create the table.
var ErrUnsupportedColumnType = errors.New("payloadexec: unsupported column type")

// maxFixedStringWidth mirrors MAX_FIXEDSTRING_SIZE in ClickHouse
// v25.8.32.4-lts src/DataTypes/DataTypeFixedString.h. Keeping the 24-bit bound
// here makes width parsing architecture-neutral and prevents oversized
// declarations from reaching a make([]byte, width) allocation.
const maxFixedStringWidth int64 = 0xFFFFFF

type columnTypeKind uint8

const (
	columnTypeUnsupported columnTypeKind = iota
	columnTypeString
	columnTypeFixedString
	columnTypeBool
	columnTypeFloat32
	columnTypeFloat64
	columnTypeUInt8
	columnTypeUInt16
	columnTypeUInt32
	columnTypeUInt64
	columnTypeInt8
	columnTypeInt16
	columnTypeInt32
	columnTypeInt64
)

type classifiedColumnType struct {
	kind             columnTypeKind
	fixedStringWidth int
}

// classifyColumnType is the one authoritative parser for the declared SI type
// profile. parseValue dispatches on this result, so validation cannot admit a
// type for which the executor has no materializer branch. FixedString widths
// intentionally preserve the legacy parseFixedString grammar: surrounding
// whitespace, a leading plus and leading zeroes are accepted.
func classifyColumnType(typeName string) classifiedColumnType {
	switch typeName {
	case "String":
		return classifiedColumnType{kind: columnTypeString}
	case "Bool":
		return classifiedColumnType{kind: columnTypeBool}
	case "Float32":
		return classifiedColumnType{kind: columnTypeFloat32}
	case "Float64":
		return classifiedColumnType{kind: columnTypeFloat64}
	case "UInt8":
		return classifiedColumnType{kind: columnTypeUInt8}
	case "UInt16":
		return classifiedColumnType{kind: columnTypeUInt16}
	case "UInt32":
		return classifiedColumnType{kind: columnTypeUInt32}
	case "UInt64":
		return classifiedColumnType{kind: columnTypeUInt64}
	case "Int8":
		return classifiedColumnType{kind: columnTypeInt8}
	case "Int16":
		return classifiedColumnType{kind: columnTypeInt16}
	case "Int32":
		return classifiedColumnType{kind: columnTypeInt32}
	case "Int64":
		return classifiedColumnType{kind: columnTypeInt64}
	}

	const fixedStringPrefix = "FixedString("
	if !strings.HasPrefix(typeName, fixedStringPrefix) || !strings.HasSuffix(typeName, ")") {
		return classifiedColumnType{}
	}
	widthText := strings.TrimSpace(typeName[len(fixedStringPrefix) : len(typeName)-1])
	width, err := strconv.ParseInt(widthText, 10, 64)
	if err != nil || width <= 0 || width > maxFixedStringWidth {
		return classifiedColumnType{}
	}
	return classifiedColumnType{kind: columnTypeFixedString, fixedStringWidth: int(width)}
}

// SupportedColumnType reports whether a declared ClickHouse type is inside the
// MVP whitelist (§5.3): String, FixedString(N) with 0 < N <= 0xFFFFFF, Bool,
// Float32/64 and [U]Int8/16/32/64. For compatibility, FixedString widths retain
// the executor's legacy whitespace, leading-plus and leading-zero spellings.
// It is the single source of truth shared by parseValue and every caller that
// validates a declaration before executing DDL.
func SupportedColumnType(typeName string) bool {
	return classifyColumnType(typeName).kind != columnTypeUnsupported
}

// ValidateColumnType returns ErrUnsupportedColumnType naming the offending
// string when the type is outside the whitelist.
func ValidateColumnType(typeName string) error {
	if SupportedColumnType(typeName) {
		return nil
	}
	return unsupportedColumnTypeError(typeName)
}

func unsupportedColumnTypeError(typeName string) error {
	return fmt.Errorf("%w %q (whitelist: String, FixedString(N) with 0 < N <= 0xFFFFFF, Bool, Float32/64, [U]Int8/16/32/64)", ErrUnsupportedColumnType, typeName)
}

// ValidateTableSchemaColumns validates every declared column of one table and
// joins the failures, so an operator sees all offending columns at once.
func ValidateTableSchemaColumns(t TableSchema) error {
	var errs []error
	for _, column := range t.Columns {
		if err := ValidateColumnType(column.Type); err != nil {
			errs = append(errs, fmt.Errorf("table %s column %q: %w", t.TableID, column.Name, err))
		}
	}
	return errors.Join(errs...)
}
