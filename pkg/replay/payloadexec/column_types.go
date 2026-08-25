package payloadexec

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
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

// SupportedColumnType reports whether a declared ClickHouse type is inside the
// pinned storage-integrity profile (§5.3, Spec Q Q-D1). The admitted set lives
// in column_profile.go; this is a reader of it, shared by parseValue and every
// caller that validates a declaration before executing DDL.
func SupportedColumnType(typeName string) bool {
	_, err := ResolveColumnProfile(typeName)
	return err == nil
}

// ValidateColumnType returns ErrUnsupportedColumnType naming the offending
// string when the type is outside the whitelist.
func ValidateColumnType(typeName string) error {
	if SupportedColumnType(typeName) {
		return nil
	}
	return unsupportedColumnTypeError(typeName)
}

// CanonicalColumnType validates a declared ClickHouse type against the pinned
// storage-integrity profile and returns its canonical spelling. Scalar types
// have one accepted spelling and are returned unchanged. FixedString widths are
// rendered in base 10 without whitespace, a leading plus or leading zeroes.
// Every rejection unwraps to ErrUnsupportedColumnType.
func CanonicalColumnType(typeName string) (string, error) {
	profile, err := ResolveColumnProfile(typeName)
	if err != nil {
		return "", err
	}
	return profile.Canonical, nil
}

// unsupportedColumnTypeError names the offending declaration and renders the
// admitted profile from the authority table, so widening or narrowing the
// profile never leaves stale prose behind in the operator message.
func unsupportedColumnTypeError(typeName string) error {
	return fmt.Errorf("%w %q (admitted profile: %s)", ErrUnsupportedColumnType, typeName, strings.Join(admittedProfileSummary(), ", "))
}

// ValidateTableSchemaColumns validates every declared column of one table and
// joins the failures, so an operator sees all offending columns at once.
func ValidateTableSchemaColumns(t TableSchema) error {
	_, err := CanonicalizeTableSchemaColumnTypes(t)
	return err
}

// CanonicalizeTableSchemaColumnTypes validates and canonicalizes every column
// type in one table. It returns a copy with its own Columns slice and never
// mutates the input. Supported columns are canonicalized even when other
// columns fail validation; every failure is joined with table and column
// context so callers can report the complete declaration error at once.
func CanonicalizeTableSchemaColumnTypes(t TableSchema) (TableSchema, error) {
	canonical := t
	if t.Columns != nil {
		canonical.Columns = make([]lthash.Column, len(t.Columns))
		copy(canonical.Columns, t.Columns)
	}

	var errs []error
	for i, column := range canonical.Columns {
		typeName, err := CanonicalColumnType(column.Type)
		if err != nil {
			errs = append(errs, fmt.Errorf("table %s column %q: %w", t.TableID, column.Name, err))
			continue
		}
		canonical.Columns[i].Type = typeName
	}
	return canonical, errors.Join(errs...)
}
