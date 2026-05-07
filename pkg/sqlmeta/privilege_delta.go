package sqlmeta

// PrivilegeAction is the kind of GRANT/REVOKE statement that produced
// a PrivilegeDelta. Mirrors protos.PrivilegeDelta_Action — kept
// numerically aligned so conversion is a plain cast.
type PrivilegeAction int

const (
	PrivilegeActionUnspecified PrivilegeAction = 0
	PrivilegeActionGrant       PrivilegeAction = 1
	PrivilegeActionRevoke      PrivilegeAction = 2
)

// String returns a short label matching the proto enum name minus
// the ACTION_ prefix.
func (a PrivilegeAction) String() string {
	switch a {
	case PrivilegeActionGrant:
		return "GRANT"
	case PrivilegeActionRevoke:
		return "REVOKE"
	default:
		return "UNSPECIFIED"
	}
}

// PrivilegeScope mirrors protos.PrivilegeDelta_Scope: the GRANT/REVOKE
// target shape (`ON db.table` vs `ON db.*`). Global scope (`ON *.*`) is
// rejected by the rewriter as UnsupportedStatement and never appears
// here.
type PrivilegeScope int

const (
	PrivilegeScopeUnspecified PrivilegeScope = 0
	PrivilegeScopeTable       PrivilegeScope = 1
	PrivilegeScopeDatabase    PrivilegeScope = 2
)

// String returns a short label matching the proto enum name minus
// the SCOPE_ prefix.
func (s PrivilegeScope) String() string {
	switch s {
	case PrivilegeScopeTable:
		return "TABLE"
	case PrivilegeScopeDatabase:
		return "DATABASE"
	default:
		return "UNSPECIFIED"
	}
}

// PrivilegeGrantee is one recipient of a GRANT (or target of a REVOKE).
// Mirrors protos.PrivilegeDelta_Grantee.
//
// The rewriter does NOT distinguish a user from a role at parse time —
// both pack into the same AST node. Callers must ensure every Name
// resolves to a real user; an unknown name passes through here and is
// rejected by the downstream auth service.
type PrivilegeGrantee struct {
	// Name is the grantee identifier exactly as it appeared in the
	// SQL. Empty when IsCurrentUser is true.
	Name string

	// IsCurrentUser is set when the SQL used the literal
	// CURRENT_USER syntax. Resolved by the downstream auth service
	// at evaluation time; the rewriter does not expand it.
	IsCurrentUser bool
}

// PrivilegeDelta is one privilege change extracted from a GRANT or
// REVOKE statement by the rewriter. Mirrors the proto message of the
// same name (rewriter.proto tag 13 child).
//
// Fan-out: one PrivilegeDelta per AccessRightsElement (the parser's
// per-privilege unit). ClickHouse's parser splits comma-separated
// privileges into separate elements but keeps comma-separated
// grantees aggregated, so Privileges is typically a single-element
// list while Grantees stays a list.
//
// Resolution rules (same as AccessedTable):
//   - LogicalDatabase: SQL qualifier OR
//     dynamic_args.upstream_logical_database_in_context. Always set
//     on a successful rewrite.
//   - PhysicalDatabase: dynamic_args.database_map[LogicalDatabase],
//     or LogicalDatabase itself when it is in
//     known_physical_databases. Always set on a successful rewrite.
//   - PhysicalTable (SCOPE_TABLE only): physical table name built
//     via the same scheme used by the table-targeting handlers
//     (`<logical><delim>[<extras><delim>]<original_table>`). Empty
//     for SCOPE_DATABASE.
type PrivilegeDelta struct {
	Action           PrivilegeAction
	Scope            PrivilegeScope
	OriginalDatabase string
	LogicalDatabase  string
	PhysicalDatabase string
	OriginalTable    string
	PhysicalTable    string

	// Privileges holds the privilege names exactly as ClickHouse's
	// parser emits them: "SELECT", "INSERT", "ALTER UPDATE",
	// "ALTER ADD COLUMN", "CREATE TABLE", "ALL", ... The rewriter
	// does NOT collapse these into a smaller taxonomy; downstream
	// owns that mapping.
	Privileges []string

	// Category is the coarse classification of Privileges via
	// CategorizePrivileges — a READ / WRITE / ADMIN bitmap aligned
	// with registry.DbAuth so downstream auth services can compare
	// directly against existing per-account permissions. Populated
	// by the rewriter package alongside the rest of the delta.
	// PrivilegeCategoryNone signals "the privilege string did not
	// match any known category" — treat as unknown, not as "no
	// access".
	Category PrivilegeCategory

	// Grantees is the GRANT TO list (or REVOKE FROM list), in
	// source order.
	Grantees []PrivilegeGrantee

	// GrantOption mirrors WITH GRANT OPTION on a GRANT statement
	// and GRANT OPTION FOR on a REVOKE statement.
	GrantOption bool
}
