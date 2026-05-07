// Package sqlmeta defines plain Go types describing the *semantics*
// of a SQL statement (classification, accessed tables) — emitted by
// the rewriter and consumed by downstream plugins. It deliberately
// has zero proxy-specific deps so any plugin can import it without
// dragging the rewriter or network state along.
package sqlmeta

import "strconv"

// StatementType is the rewriter-classified kind of SQL statement.
// Mirrors protos.StatementType but is a sqlmeta-owned type so
// downstream consumers (plugins, billing, audit) do not have to import
// the protobuf package.
type StatementType int

// Constants are kept numerically aligned with protos.StatementType so
// conversions are a plain cast. Adding new values is fine; renumbering
// is not.
const (
	StatementTypeUnspecified     StatementType = 0
	StatementTypeSelect          StatementType = 1
	StatementTypeUse             StatementType = 2
	StatementTypeShowTables      StatementType = 3
	StatementTypeShowCreateTable StatementType = 4
	StatementTypeExistsTable     StatementType = 5
	StatementTypeCreateTable     StatementType = 6
	StatementTypeDropTable       StatementType = 7
	StatementTypeAlterTable      StatementType = 8
	StatementTypeRenameTable     StatementType = 9
	StatementTypeInsert          StatementType = 10
	StatementTypeUpdate          StatementType = 11
	StatementTypeDelete          StatementType = 12
	StatementTypeCreateDatabase  StatementType = 13
	StatementTypeShowDatabases   StatementType = 14
	StatementTypeDropDatabase    StatementType = 15
	StatementTypeTruncateTable   StatementType = 16
	StatementTypeGrant           StatementType = 17
	StatementTypeRevoke          StatementType = 18
)

// String returns a short human label, matching the proto enum names
// minus the STATEMENT_TYPE_ prefix. Falls back to a numeric form for
// unknown values so logs stay readable when the proto enum grows.
//
// We deliberately do not call into protos.StatementType(s).String()
// here — sqlmeta is a leaf package with no proto dep. New rewriter
// values that haven't been added to this switch render as
// "STATEMENT_TYPE(<n>)" until the constant lands here.
func (s StatementType) String() string {
	switch s {
	case StatementTypeUnspecified:
		return "UNSPECIFIED"
	case StatementTypeSelect:
		return "SELECT"
	case StatementTypeUse:
		return "USE"
	case StatementTypeShowTables:
		return "SHOW_TABLES"
	case StatementTypeShowCreateTable:
		return "SHOW_CREATE_TABLE"
	case StatementTypeExistsTable:
		return "EXISTS_TABLE"
	case StatementTypeCreateTable:
		return "CREATE_TABLE"
	case StatementTypeDropTable:
		return "DROP_TABLE"
	case StatementTypeAlterTable:
		return "ALTER_TABLE"
	case StatementTypeRenameTable:
		return "RENAME_TABLE"
	case StatementTypeInsert:
		return "INSERT"
	case StatementTypeUpdate:
		return "UPDATE"
	case StatementTypeDelete:
		return "DELETE"
	case StatementTypeCreateDatabase:
		return "CREATE_DATABASE"
	case StatementTypeShowDatabases:
		return "SHOW_DATABASES"
	case StatementTypeDropDatabase:
		return "DROP_DATABASE"
	case StatementTypeTruncateTable:
		return "TRUNCATE_TABLE"
	case StatementTypeGrant:
		return "GRANT"
	case StatementTypeRevoke:
		return "REVOKE"
	default:
		return "STATEMENT_TYPE(" + strconv.Itoa(int(s)) + ")"
	}
}
