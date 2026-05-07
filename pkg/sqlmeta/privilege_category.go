package sqlmeta

import "strings"

// PrivilegeCategory is a coarse classification of a ClickHouse
// privilege name into the same READ / WRITE / ADMIN tiers the rest of
// housegate uses for permission checks (mirrors registry.DbAuth).
//
// Defined as a bitmap so multi-privilege grants can be OR'd into a
// single value:
//
//	GRANT SELECT, INSERT ON db.t TO u   -> Read | Write
//	GRANT ALL ON db.t TO u              -> Read | Write | Admin
//
// PrivilegeCategoryNone (zero value) indicates the privilege string did
// not match any known category — callers should treat that as
// "unknown / pass-through" rather than "definitely no access".
type PrivilegeCategory uint32

const (
	PrivilegeCategoryNone  PrivilegeCategory = 0
	PrivilegeCategoryRead  PrivilegeCategory = 1 << 0 // SELECT, SHOW *, EXISTS, dictGet, ...
	PrivilegeCategoryWrite PrivilegeCategory = 1 << 1 // INSERT, ALTER UPDATE/DELETE, OPTIMIZE, TRUNCATE
	PrivilegeCategoryAdmin PrivilegeCategory = 1 << 2 // CREATE/DROP/RENAME, schema ALTERs

	// PrivilegeCategoryAll is the OR of every defined category. Set
	// when a privilege string maps to "everything" (e.g. the literal
	// "ALL" in `GRANT ALL ON db.t TO u`).
	PrivilegeCategoryAll = PrivilegeCategoryRead | PrivilegeCategoryWrite | PrivilegeCategoryAdmin
)

// String renders the bitmap as "READ|WRITE|ADMIN" form. Empty bitmap
// renders "NONE"; unknown bits past Admin are dropped (intentional —
// new bits should add their own label here when introduced).
func (c PrivilegeCategory) String() string {
	if c == PrivilegeCategoryNone {
		return "NONE"
	}
	if c == PrivilegeCategoryAll {
		return "ALL"
	}
	parts := make([]string, 0, 3)
	if c&PrivilegeCategoryRead != 0 {
		parts = append(parts, "READ")
	}
	if c&PrivilegeCategoryWrite != 0 {
		parts = append(parts, "WRITE")
	}
	if c&PrivilegeCategoryAdmin != 0 {
		parts = append(parts, "ADMIN")
	}
	return strings.Join(parts, "|")
}

// Has reports whether c contains every bit in want. Returns true for
// want == PrivilegeCategoryNone (everything contains nothing).
func (c PrivilegeCategory) Has(want PrivilegeCategory) bool {
	return c&want == want
}

// CategorizePrivilege maps a single ClickHouse privilege name into a
// PrivilegeCategory. The input is whatever the rewriter surfaces in
// PrivilegeDelta.Privileges — exactly the strings ClickHouse's parser
// emits, e.g. "SELECT", "ALTER UPDATE", "CREATE TABLE", "dictGet",
// "ALL". Whitespace is trimmed and ASCII case is normalized.
//
// Mapping rules (in priority order):
//
//  1. "ALL" / "ALL PRIVILEGES" -> PrivilegeCategoryAll
//  2. Exact match against the curated list below — covers the common
//     read/write privileges that don't share a stable prefix.
//  3. Prefix match for the ALTER family — "ALTER UPDATE" / "ALTER
//     DELETE" are write, every other ALTER is admin (schema mutation).
//  4. Prefix match for CREATE / DROP / RENAME — admin (schema).
//  5. Prefix match for SHOW / EXISTS — read.
//  6. Unknown -> PrivilegeCategoryNone. Callers should treat unknown
//     as "do not assume" rather than "definitely none".
//
// The categorization is intentionally coarse: column-level grants are
// rejected upstream (UnsupportedStatement) so we never see them here,
// and global-scope grants (`ON *.*`) are rejected too. The categories
// align with registry.DbAuth so a downstream auth service can compare
// the bitmap directly against existing per-account permissions.
func CategorizePrivilege(name string) PrivilegeCategory {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" {
		return PrivilegeCategoryNone
	}
	switch n {
	case "ALL", "ALL PRIVILEGES":
		return PrivilegeCategoryAll
	case "SELECT", "DICTGET":
		return PrivilegeCategoryRead
	case "INSERT", "OPTIMIZE", "TRUNCATE":
		return PrivilegeCategoryWrite
	}
	switch {
	case strings.HasPrefix(n, "ALTER UPDATE"), strings.HasPrefix(n, "ALTER DELETE"):
		return PrivilegeCategoryWrite
	case strings.HasPrefix(n, "ALTER "):
		// ALTER ADD/DROP/MODIFY/RENAME/CLEAR COLUMN, ALTER ADD/DROP
		// INDEX, ALTER MODIFY ORDER BY, ALTER MODIFY TTL, ALTER VIEW
		// MODIFY QUERY, ALTER {ATTACH,DETACH,REPLACE,DROP} PARTITION,
		// etc. — schema mutation, treat as admin.
		return PrivilegeCategoryAdmin
	case strings.HasPrefix(n, "CREATE "), strings.HasPrefix(n, "DROP "), strings.HasPrefix(n, "RENAME "):
		// CREATE/DROP TABLE/VIEW/DICTIONARY/DATABASE/TEMPORARY TABLE,
		// RENAME TABLE — schema admin.
		return PrivilegeCategoryAdmin
	case strings.HasPrefix(n, "SHOW"), strings.HasPrefix(n, "EXISTS"):
		// SHOW TABLES / DATABASES / COLUMNS / DICTIONARIES,
		// SHOW CREATE TABLE, EXISTS — pure read access.
		return PrivilegeCategoryRead
	}
	return PrivilegeCategoryNone
}

// CategorizePrivileges maps each name through CategorizePrivilege and
// returns the OR of all results. Returns PrivilegeCategoryNone for an
// empty input.
func CategorizePrivileges(names []string) PrivilegeCategory {
	var out PrivilegeCategory
	for _, n := range names {
		out |= CategorizePrivilege(n)
	}
	return out
}
